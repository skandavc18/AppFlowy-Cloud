//! Per-user file storage for the AppFlowy app's folder sync.
//!
//! The app keeps a workspace stored on the user's computer in step through a folder of files, and
//! these endpoints act as that folder when it lives in the user's own AppFlowy Cloud deployment.
//! Files are stored at `sync-folders/{user_uuid}/{folder}/{name}` and attached files
//! (`?blobs=true`) at `sync-folders/{user_uuid}/{folder}/blobs/{name}`.

use std::fmt::{Display, Formatter};

use actix_web::body::SizedStream;
use actix_web::error::PayloadError;
use actix_web::http::header::{HeaderMap, CACHE_CONTROL, CONTENT_LENGTH};
use actix_web::http::StatusCode;
use actix_web::web::{self, Data, Payload};
use actix_web::{HttpRequest, HttpResponse, ResponseError, Scope};
use app_error::{AppError, ErrorCode};
use aws_sdk_s3::primitives::{ByteStream, ByteStreamError};
use aws_sdk_s3::types::CompletedPart;
use bytes::{Bytes, BytesMut};
use database::file::s3_client_impl::{AwsS3BucketClientImpl, S3ObjectEntry};
use database::file::BucketClient;
use futures_util::{stream, Stream, StreamExt};
use serde::Deserialize;
use shared_entity::dto::sync_folder_dto::{
  SyncFolder, SyncFolderFile, SyncFolderFileList, SyncFolderList,
};
use shared_entity::response::{AppResponse, JsonAppResponse};
use tracing::{error, trace, warn};
use uuid::Uuid;

use crate::biz::authentication::jwt::UserUuid;
use crate::state::AppState;

const ROOT_PREFIX: &str = "sync-folders";
const BLOBS_DIR: &str = "blobs";
const OCTET_STREAM: &str = "application/octet-stream";
const MAX_FOLDER_NAME_LEN: usize = 100;
const MAX_FILE_NAME_LEN: usize = 200;
const MIB: u64 = 1024 * 1024;
/// Bodies up to this size are stored with a single request. Larger ones are streamed to S3 as a
/// multipart upload with parts of at least this size, so memory use per upload stays bounded.
const PART_SIZE: u64 = 8 * MIB;
/// The most parts S3 accepts in one multipart upload.
const MAX_PARTS: u64 = 10_000;

type AuthResult = Result<UserUuid, actix_web::Error>;
type QueryResult = Result<web::Query<BlobsQuery>, actix_web::Error>;

pub fn sync_folders_scope() -> Scope {
  web::scope("/api/sync-folders")
    .service(web::resource("").route(web::get().to(list_folders_handler)))
    .service(web::resource("/{folder}/files").route(web::get().to(list_files_handler)))
    .service(
      web::resource("/{folder}/files/{name}")
        .route(web::put().to(put_file_handler))
        .route(web::get().to(get_file_handler))
        .route(web::delete().to(delete_file_handler)),
    )
}

#[derive(Debug, Deserialize)]
struct BlobsQuery {
  #[serde(default)]
  blobs: bool,
}

async fn list_folders_handler(
  user: AuthResult,
  state: Data<AppState>,
) -> Result<JsonAppResponse<SyncFolderList>, SyncFolderError> {
  let user = authorized_user(user)?;
  let prefix = user_prefix(&user);
  let listing = state
    .bucket_client
    .list_dir_entries(&prefix, Some("/"))
    .await?;
  let folders = folders_from_prefixes(&prefix, &listing.common_prefixes);
  Ok(success(SyncFolderList { folders }))
}

async fn list_files_handler(
  user: AuthResult,
  state: Data<AppState>,
  folder: web::Path<String>,
  query: QueryResult,
) -> Result<JsonAppResponse<SyncFolderFileList>, SyncFolderError> {
  let user = authorized_user(user)?;
  let folder = folder.into_inner();
  validate_folder_name(&folder)?;
  let prefix = folder_prefix(&user, &folder, blobs_param(query)?);
  let listing = state
    .bucket_client
    .list_dir_entries(&prefix, Some("/"))
    .await?;
  let files = files_from_objects(&prefix, listing.objects);
  Ok(success(SyncFolderFileList { files }))
}

async fn put_file_handler(
  user: AuthResult,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  query: QueryResult,
  req: HttpRequest,
  payload: Payload,
) -> Result<JsonAppResponse<SyncFolderFile>, SyncFolderError> {
  let target = file_target(user, path.into_inner(), query)?;
  let max_file_size = state.config.sync_folder.max_file_size;
  let content_length = declared_content_length(req.headers(), max_file_size)?;
  let client = &state.bucket_client;
  if content_length <= PART_SIZE {
    put_small_file(client, &target.key, payload, content_length).await?;
  } else {
    put_large_file(client, &target.key, payload, content_length).await?;
  }
  trace!("stored {} ({content_length} bytes)", target.key);

  // Read the stored metadata back so `modified_at` matches what listings return. Another request
  // may already have deleted the file again.
  let file = match client.object_entry(&target.key).await? {
    Some(entry) => SyncFolderFile {
      name: target.name,
      size: entry.size,
      modified_at: entry.last_modified_ms,
    },
    None => SyncFolderFile {
      name: target.name,
      size: content_length as i64,
      modified_at: chrono::Utc::now().timestamp_millis(),
    },
  };
  Ok(success(file))
}

async fn get_file_handler(
  user: AuthResult,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  query: QueryResult,
) -> Result<HttpResponse, SyncFolderError> {
  let target = file_target(user, path.into_inner(), query)?;
  let object = match state.bucket_client.get_object_stream(&target.key).await {
    Ok(object) => object,
    Err(err) if err.is_record_not_found() => {
      return Err(SyncFolderError::not_found(format!(
        "No file named {}",
        target.name
      )));
    },
    Err(err) => return Err(err.into()),
  };
  let body = body_stream(object.body);
  let mut response = HttpResponse::Ok();
  response
    .content_type(OCTET_STREAM)
    .insert_header((CACHE_CONTROL, "private, no-store"));
  match object.content_length.map(u64::try_from) {
    Some(Ok(length)) => Ok(response.body(SizedStream::new(length, body))),
    _ => Ok(response.streaming(body)),
  }
}

async fn delete_file_handler(
  user: AuthResult,
  state: Data<AppState>,
  path: web::Path<(String, String)>,
  query: QueryResult,
) -> Result<JsonAppResponse<()>, SyncFolderError> {
  let target = file_target(user, path.into_inner(), query)?;
  state.bucket_client.delete_blob(&target.key).await?;
  trace!("deleted {}", target.key);
  Ok(AppResponse::Ok().with_message("").into())
}

/// Successful responses use the usual envelope with an empty message, as the API documents.
fn success<T>(data: T) -> JsonAppResponse<T> {
  AppResponse::Ok().with_data(data).with_message("").into()
}

struct FileTarget {
  name: String,
  key: String,
}

fn file_target(
  user: AuthResult,
  (folder, name): (String, String),
  query: QueryResult,
) -> Result<FileTarget, SyncFolderError> {
  let user = authorized_user(user)?;
  validate_folder_name(&folder)?;
  validate_file_name(&name)?;
  let key = file_key(&user, &folder, &name, blobs_param(query)?);
  Ok(FileTarget { name, key })
}

fn authorized_user(user: AuthResult) -> Result<Uuid, SyncFolderError> {
  match user {
    Ok(user) => Ok(*user),
    Err(err) => Err(SyncFolderError::from_auth_error(err)),
  }
}

fn blobs_param(query: QueryResult) -> Result<bool, SyncFolderError> {
  match query {
    Ok(query) => Ok(query.blobs),
    Err(err) => Err(SyncFolderError::bad_request(err.to_string())),
  }
}

fn validate_folder_name(folder: &str) -> Result<(), SyncFolderError> {
  if is_valid_folder_name(folder) {
    return Ok(());
  }
  Err(SyncFolderError::bad_request(format!(
    "Invalid folder name: {folder:?}"
  )))
}

fn validate_file_name(name: &str) -> Result<(), SyncFolderError> {
  if is_valid_file_name(name) {
    return Ok(());
  }
  Err(SyncFolderError::bad_request(format!(
    "Invalid file name: {name:?}"
  )))
}

/// 1 to 100 ASCII letters, digits, spaces, `.`, `_` or `-`, not starting or ending with a space
/// or `.`. Names arrive percent-decoded, so this also rules out `/` and path traversal.
fn is_valid_folder_name(name: &str) -> bool {
  let bytes = name.as_bytes();
  let (Some(&first), Some(&last)) = (bytes.first(), bytes.last()) else {
    return false;
  };
  bytes.len() <= MAX_FOLDER_NAME_LEN
    && name.bytes().all(is_folder_name_byte)
    && !matches!(first, b' ' | b'.')
    && !matches!(last, b' ' | b'.')
}

/// 1 to 200 characters matching `^[A-Za-z0-9][A-Za-z0-9._-]*$`.
fn is_valid_file_name(name: &str) -> bool {
  let Some(first) = name.bytes().next() else {
    return false;
  };
  name.len() <= MAX_FILE_NAME_LEN
    && first.is_ascii_alphanumeric()
    && name.bytes().all(is_file_name_byte)
}

fn is_folder_name_byte(byte: u8) -> bool {
  byte.is_ascii_alphanumeric() || matches!(byte, b' ' | b'.' | b'_' | b'-')
}

fn is_file_name_byte(byte: u8) -> bool {
  byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-')
}

fn user_prefix(user: &Uuid) -> String {
  format!("{ROOT_PREFIX}/{user}/")
}

fn folder_prefix(user: &Uuid, folder: &str, blobs: bool) -> String {
  if blobs {
    format!("{ROOT_PREFIX}/{user}/{folder}/{BLOBS_DIR}/")
  } else {
    format!("{ROOT_PREFIX}/{user}/{folder}/")
  }
}

fn file_key(user: &Uuid, folder: &str, name: &str, blobs: bool) -> String {
  format!("{}{name}", folder_prefix(user, folder, blobs))
}

/// The folders below `user_prefix`, sorted by name. Every common prefix is a folder holding at
/// least one object, as S3 has no empty directories.
fn folders_from_prefixes(user_prefix: &str, common_prefixes: &[String]) -> Vec<SyncFolder> {
  let mut names: Vec<&str> = common_prefixes
    .iter()
    .filter_map(|prefix| prefix.strip_prefix(user_prefix)?.strip_suffix('/'))
    .filter(|name| is_valid_folder_name(name))
    .collect();
  names.sort_unstable();
  names.dedup();
  names
    .into_iter()
    .map(|name| SyncFolder {
      name: name.to_string(),
    })
    .collect()
}

/// The files directly inside the listed folder. Directory markers and keys that are not valid file
/// names, such as those below the `blobs/` prefix, are skipped.
fn files_from_objects(folder_prefix: &str, objects: Vec<S3ObjectEntry>) -> Vec<SyncFolderFile> {
  objects
    .into_iter()
    .filter_map(|object| {
      let name = object.key.strip_prefix(folder_prefix)?;
      if !is_valid_file_name(name) {
        return None;
      }
      Some(SyncFolderFile {
        name: name.to_string(),
        size: object.size,
        modified_at: object.last_modified_ms,
      })
    })
    .collect()
}

/// The declared body size, which is required and must be within the configured limit.
fn declared_content_length(
  headers: &HeaderMap,
  max_file_size: u64,
) -> Result<u64, SyncFolderError> {
  let Some(value) = headers.get(CONTENT_LENGTH) else {
    return Err(SyncFolderError::length_required(
      "Content-Length is required",
    ));
  };
  let length = value
    .to_str()
    .ok()
    .and_then(|value| value.parse::<u64>().ok())
    .ok_or_else(|| SyncFolderError::bad_request("Invalid Content-Length"))?;
  if length > max_file_size {
    let message = format!("The file is {length} bytes, the limit is {max_file_size} bytes");
    return Err(SyncFolderError::payload_too_large(message));
  }
  Ok(length)
}

/// Counts the bytes of a request body. Its Content-Length was checked against the size limit up
/// front, so refusing bytes past that length also enforces the limit while the body streams in.
struct BodyCounter {
  declared: u64,
  received: u64,
}

impl BodyCounter {
  fn new(declared: u64) -> Self {
    Self {
      declared,
      received: 0,
    }
  }

  fn add_chunk(&mut self, len: usize) -> Result<(), SyncFolderError> {
    self.received += len as u64;
    if self.received > self.declared {
      return Err(SyncFolderError::payload_too_large(
        "Body exceeds its Content-Length",
      ));
    }
    Ok(())
  }

  fn finish(&self) -> Result<(), SyncFolderError> {
    if self.received < self.declared {
      return Err(SyncFolderError::bad_request(
        "Body is shorter than its Content-Length",
      ));
    }
    Ok(())
  }
}

async fn put_small_file(
  client: &AwsS3BucketClientImpl,
  key: &str,
  mut payload: Payload,
  content_length: u64,
) -> Result<(), SyncFolderError> {
  let mut body = BodyCounter::new(content_length);
  let mut buffer = BytesMut::with_capacity(content_length as usize);
  while let Some(chunk) = payload.next().await {
    let chunk = chunk.map_err(payload_error)?;
    body.add_chunk(chunk.len())?;
    buffer.extend_from_slice(&chunk);
  }
  body.finish()?;
  let content = ByteStream::from(buffer.freeze());
  client.put_blob(key, content, Some(OCTET_STREAM)).await?;
  Ok(())
}

async fn put_large_file(
  client: &AwsS3BucketClientImpl,
  key: &str,
  mut payload: Payload,
  content_length: u64,
) -> Result<(), SyncFolderError> {
  let part_size = part_size_for(content_length);
  let mut body = BodyCounter::new(content_length);
  let mut upload = MultipartUpload::start(client, key).await?;
  let mut buffer = BytesMut::with_capacity(part_size);
  while let Some(chunk) = payload.next().await {
    let chunk = chunk.map_err(payload_error)?;
    body.add_chunk(chunk.len())?;
    buffer.extend_from_slice(&chunk);
    while buffer.len() >= part_size {
      let part = buffer.split_to(part_size).freeze();
      upload.send_part(part).await?;
    }
  }
  body.finish()?;
  if !buffer.is_empty() {
    upload.send_part(buffer.freeze()).await?;
  }
  upload.complete().await?;
  Ok(())
}

/// Parts are at least [PART_SIZE], and larger for huge files so they need at most [MAX_PARTS].
fn part_size_for(content_length: u64) -> usize {
  let min_part_size = content_length.div_ceil(MAX_PARTS).next_multiple_of(MIB);
  min_part_size.max(PART_SIZE) as usize
}

/// A multipart upload that is aborted unless it completes, including when the request is dropped
/// because the client went away, so failed uploads leave no orphaned parts behind.
struct MultipartUpload<'a> {
  client: &'a AwsS3BucketClientImpl,
  key: &'a str,
  upload_id: String,
  parts: Vec<CompletedPart>,
  completed: bool,
}

impl<'a> MultipartUpload<'a> {
  async fn start(client: &'a AwsS3BucketClientImpl, key: &'a str) -> Result<Self, AppError> {
    let upload_id = client.create_multipart_upload(key, OCTET_STREAM).await?;
    Ok(Self {
      client,
      key,
      upload_id,
      parts: Vec::new(),
      completed: false,
    })
  }

  async fn send_part(&mut self, part: Bytes) -> Result<(), AppError> {
    let part_number = self.parts.len() as i32 + 1;
    let body = ByteStream::from(part);
    let completed_part = self
      .client
      .upload_multipart_part(self.key, &self.upload_id, part_number, body)
      .await?;
    self.parts.push(completed_part);
    Ok(())
  }

  async fn complete(mut self) -> Result<(), AppError> {
    let parts = std::mem::take(&mut self.parts);
    self
      .client
      .complete_multipart_upload(self.key, &self.upload_id, parts)
      .await?;
    self.completed = true;
    Ok(())
  }
}

impl Drop for MultipartUpload<'_> {
  fn drop(&mut self) {
    if self.completed {
      return;
    }
    let client = self.client.clone();
    let key = self.key.to_string();
    let upload_id = std::mem::take(&mut self.upload_id);
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
      warn!("could not abort the multipart upload of {key}: no async runtime");
      return;
    };
    runtime.spawn(async move {
      if let Err(err) = client.abort_multipart_upload(&key, &upload_id).await {
        warn!("failed to abort the multipart upload of {key}: {err}");
      }
    });
  }
}

fn payload_error(err: PayloadError) -> SyncFolderError {
  SyncFolderError::bad_request(format!("Failed to read the request body: {err}"))
}

/// The chunks of an S3 object body as they arrive.
fn body_stream(body: ByteStream) -> impl Stream<Item = Result<Bytes, ByteStreamError>> {
  stream::unfold(body, |mut body| async move {
    let chunk = body.next().await?;
    Some((chunk, body))
  })
}

/// An error answered with the usual `{"code", "message"}` envelope and a matching HTTP status,
/// unlike [AppError] responses, which always use 200.
#[derive(Debug)]
struct SyncFolderError {
  status: StatusCode,
  code: ErrorCode,
  message: String,
}

impl SyncFolderError {
  fn new(status: StatusCode, code: ErrorCode, message: impl Into<String>) -> Self {
    Self {
      status,
      code,
      message: message.into(),
    }
  }

  fn bad_request(message: impl Into<String>) -> Self {
    Self::new(StatusCode::BAD_REQUEST, ErrorCode::InvalidRequest, message)
  }

  fn not_found(message: impl Into<String>) -> Self {
    Self::new(StatusCode::NOT_FOUND, ErrorCode::RecordNotFound, message)
  }

  fn length_required(message: impl Into<String>) -> Self {
    Self::new(
      StatusCode::LENGTH_REQUIRED,
      ErrorCode::InvalidRequest,
      message,
    )
  }

  fn payload_too_large(message: impl Into<String>) -> Self {
    Self::new(
      StatusCode::PAYLOAD_TOO_LARGE,
      ErrorCode::PayloadTooLarge,
      message,
    )
  }

  fn from_auth_error(err: actix_web::Error) -> Self {
    let status = err.as_response_error().status_code();
    let code = if status == StatusCode::UNAUTHORIZED {
      ErrorCode::UserUnAuthorized
    } else {
      ErrorCode::Internal
    };
    Self::new(status, code, err.to_string())
  }
}

impl From<AppError> for SyncFolderError {
  fn from(err: AppError) -> Self {
    let status = match &err {
      AppError::RecordNotFound(_) => StatusCode::NOT_FOUND,
      AppError::InvalidRequest(_) => StatusCode::BAD_REQUEST,
      AppError::PayloadTooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
      AppError::ServiceTemporaryUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
      _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    Self::new(status, err.code(), err.to_string())
  }
}

impl Display for SyncFolderError {
  fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
    write!(f, "{}: {}", self.status, self.message)
  }
}

impl ResponseError for SyncFolderError {
  fn status_code(&self) -> StatusCode {
    self.status
  }

  fn error_response(&self) -> HttpResponse {
    if self.status.is_server_error() {
      error!("sync folders request failed: {}", self.message);
    }
    let body = AppResponse::<()>::new(self.code, self.message.clone());
    HttpResponse::build(self.status).json(body)
  }
}

#[cfg(test)]
mod tests {
  use actix_web::body::to_bytes;
  use actix_web::http::header::{HeaderValue, AUTHORIZATION};
  use actix_web::test::{call_service, init_service, read_body, read_body_json, TestRequest};
  use actix_web::App;
  use aws_sdk_s3::primitives::DateTime;
  use aws_sdk_s3::types::Object;
  use jsonwebtoken::{encode, EncodingKey, Header};
  use secrecy::Secret;
  use serde_json::{json, Value};

  use super::*;

  const JWT_SECRET: &str = "sync-folders-test-secret";

  #[test]
  fn names_follow_the_contract() {
    for name in ["AppFlowy", "My Sync Folder", "a", "v1.2_final-3"] {
      assert!(is_valid_folder_name(name), "{name:?}");
    }
    assert!(is_valid_folder_name(&"x".repeat(100)));
    for name in ["", " a", "a ", ".a", "a.", "..", "a/b", "a\\b", "café"] {
      assert!(!is_valid_folder_name(name), "{name:?}");
    }
    assert!(!is_valid_folder_name(&"x".repeat(101)));

    for name in ["afsync1_0a1b", "a", "A.b-c_d", "abc."] {
      assert!(is_valid_file_name(name), "{name:?}");
    }
    assert!(is_valid_file_name(&"f".repeat(200)));
    for name in ["", ".hidden", "_a", "-a", "a b", "a/b", "..", "ü"] {
      assert!(!is_valid_file_name(name), "{name:?}");
    }
    assert!(!is_valid_file_name(&"f".repeat(201)));
  }

  #[test]
  fn keys_are_scoped_to_the_user_and_folder() {
    let user = Uuid::parse_str("8b4f7f5e-3c2a-4d1e-9f00-1a2b3c4d5e6f").unwrap();
    assert_eq!(
      user_prefix(&user),
      "sync-folders/8b4f7f5e-3c2a-4d1e-9f00-1a2b3c4d5e6f/"
    );
    assert_eq!(
      file_key(&user, "My Sync", "afsync1_x", false),
      "sync-folders/8b4f7f5e-3c2a-4d1e-9f00-1a2b3c4d5e6f/My Sync/afsync1_x"
    );
    assert_eq!(
      file_key(&user, "My Sync", "photo.png", true),
      "sync-folders/8b4f7f5e-3c2a-4d1e-9f00-1a2b3c4d5e6f/My Sync/blobs/photo.png"
    );
  }

  #[test]
  fn folders_are_listed_from_common_prefixes() {
    let common_prefixes = [
      "sync-folders/u/Work/",
      "sync-folders/u/AppFlowy/",
      "sync-folders/u/.hidden/",
      "sync-folders/u/AppFlowy/",
    ]
    .map(String::from);
    let names: Vec<String> = folders_from_prefixes("sync-folders/u/", &common_prefixes)
      .into_iter()
      .map(|folder| folder.name)
      .collect();
    assert_eq!(names, ["AppFlowy", "Work"]);
  }

  #[test]
  fn files_are_the_direct_children_of_the_folder() {
    let objects = [
      object("sync-folders/u/F/afsync1_a", 12, 1_731_470_000_123),
      object("sync-folders/u/F/", 0, 1_731_470_000_000),
      object("sync-folders/u/F/blobs/a.png", 99, 1_731_470_000_456),
      object("sync-folders/u/F/afsync1_b", 0, 1_731_470_000_789),
    ];
    let entries = objects
      .iter()
      .filter_map(S3ObjectEntry::from_object)
      .collect();
    let expected = [
      SyncFolderFile {
        name: "afsync1_a".to_string(),
        size: 12,
        modified_at: 1_731_470_000_123,
      },
      SyncFolderFile {
        name: "afsync1_b".to_string(),
        size: 0,
        modified_at: 1_731_470_000_789,
      },
    ];
    let files = files_from_objects("sync-folders/u/F/", entries);
    assert_eq!(files, expected);
  }

  #[test]
  fn large_files_use_parts_within_the_s3_limits() {
    assert_eq!(part_size_for(PART_SIZE + 1), PART_SIZE as usize);
    assert_eq!(part_size_for(2 * 1024 * MIB), PART_SIZE as usize);
    let huge = 200 * 1024 * MIB;
    let part_size = part_size_for(huge) as u64;
    assert!(huge.div_ceil(part_size) <= MAX_PARTS);
    assert_eq!(part_size % MIB, 0);
  }

  #[test]
  fn upload_sizes_are_checked_before_and_while_streaming() {
    let mut headers = HeaderMap::new();
    let err = declared_content_length(&headers, 100).unwrap_err();
    assert_eq!(err.status, StatusCode::LENGTH_REQUIRED);

    headers.insert(CONTENT_LENGTH, HeaderValue::from_static("ten"));
    let err = declared_content_length(&headers, 100).unwrap_err();
    assert_eq!(err.status, StatusCode::BAD_REQUEST);

    headers.insert(CONTENT_LENGTH, HeaderValue::from_static("101"));
    let err = declared_content_length(&headers, 100).unwrap_err();
    assert_eq!(err.status, StatusCode::PAYLOAD_TOO_LARGE);

    headers.insert(CONTENT_LENGTH, HeaderValue::from_static("100"));
    assert_eq!(declared_content_length(&headers, 100).unwrap(), 100);

    let mut body = BodyCounter::new(100);
    body.add_chunk(60).unwrap();
    let err = body.finish().unwrap_err();
    assert_eq!(err.status, StatusCode::BAD_REQUEST);
    body.add_chunk(40).unwrap();
    body.finish().unwrap();
    let err = body.add_chunk(1).unwrap_err();
    assert_eq!(err.status, StatusCode::PAYLOAD_TOO_LARGE);
  }

  #[actix_web::test]
  async fn errors_use_the_envelope_with_a_matching_status() {
    let response = SyncFolderError::not_found("No file named a").error_response();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    let body = to_bytes(response.into_body()).await.unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body, json!({"code": -2, "message": "No file named a"}));
  }

  #[actix_web::test]
  async fn requests_are_authorized_and_validated_after_decoding() {
    let app = init_service(
      App::new()
        .app_data(Data::new(Secret::new(JWT_SECRET.to_string())))
        .route("/{folder}/files/{name}", web::get().to(resolve_key)),
    )
    .await;

    let unauthenticated = TestRequest::get().uri("/F/files/a").to_request();
    let response = call_service(&app, unauthenticated).await;
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body: Value = read_body_json(response).await;
    assert_eq!(body["code"], ErrorCode::UserUnAuthorized.value());

    let user = Uuid::new_v4();
    let token = token_for(user);
    let request = |uri: &str| {
      TestRequest::get()
        .uri(uri)
        .insert_header((AUTHORIZATION, format!("Bearer {token}")))
        .to_request()
    };

    let response = call_service(&app, request("/My%20Sync/files/a?blobs=true")).await;
    assert_eq!(response.status(), StatusCode::OK);
    let key = read_body(response).await;
    assert_eq!(key, format!("sync-folders/{user}/My Sync/blobs/a"));

    let rejected = [
      "/a%2Fb/files/a",
      "/%2E%2E/files/a",
      "/F/files/..%2Fa",
      "/F/files/a?blobs=yes",
    ];
    for uri in rejected {
      let response = call_service(&app, request(uri)).await;
      assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
    }
  }

  async fn resolve_key(
    user: AuthResult,
    path: web::Path<(String, String)>,
    query: QueryResult,
  ) -> Result<String, SyncFolderError> {
    Ok(file_target(user, path.into_inner(), query)?.key)
  }

  fn token_for(user: Uuid) -> String {
    let claims = json!({
      "sub": user.to_string(),
      "exp": chrono::Utc::now().timestamp() + 3600,
      "email": "sync@example.com",
      "phone": "",
      "app_metadata": {},
      "user_metadata": {},
      "role": "authenticated",
    });
    let key = EncodingKey::from_secret(JWT_SECRET.as_bytes());
    encode(&Header::default(), &claims, &key).unwrap()
  }

  fn object(key: &str, size: i64, modified_at: i64) -> Object {
    Object::builder()
      .key(key)
      .size(size)
      .last_modified(DateTime::from_millis(modified_at))
      .build()
  }
}
