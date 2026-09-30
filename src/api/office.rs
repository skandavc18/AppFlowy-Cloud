use std::sync::Arc;
use std::time::{Duration, Instant};

use access_control::act::Action;
use actix_web::http::header::{AUTHORIZATION, CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE};
use actix_web::web::{self, Data, Json};
use actix_web::{HttpRequest, HttpResponse, Scope};
use app_error::AppError;
use aws_sdk_s3::primitives::ByteStream;
use dashmap::DashMap;
use database::file::BlobKey;
use futures_util::TryStreamExt;
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use percent_encoding::percent_decode_str;
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use shared_entity::dto::office_dto::{CreateOfficeSessionRequest, OfficeSessionResponse};
use shared_entity::response::{AppResponse, JsonAppResponse};
use tracing::{error, instrument};
use uuid::Uuid;

use crate::biz::authentication::jwt::UserUuid;
use crate::config::config::DocumentServerSetting;
use crate::state::AppState;

const SESSION_TTL: Duration = Duration::from_secs(24 * 60 * 60);

pub fn office_scope() -> Scope {
  web::scope("/api/office")
    .service(web::resource("/{workspace_id}/session").route(web::post().to(create_session_handler)))
    .service(
      web::resource("/session/{session_id}/document").route(web::get().to(get_document_handler)),
    )
    .service(
      web::resource("/session/{session_id}/callback").route(web::post().to(callback_handler)),
    )
}

#[derive(Clone, Default)]
pub struct OfficeSessionStore {
  sessions: Arc<DashMap<Uuid, OfficeSession>>,
}

impl OfficeSessionStore {
  fn insert(&self, id: Uuid, session: OfficeSession) {
    self.sessions.retain(|_, session| !session.is_expired());
    self.sessions.insert(id, session);
  }

  fn get(&self, id: &Uuid) -> Option<OfficeSession> {
    let session = self.sessions.get(id)?.clone();
    if session.is_expired() {
      self.sessions.remove(id);
      return None;
    }
    Some(session)
  }

  fn remove(&self, id: &Uuid) {
    self.sessions.remove(id);
  }
}

#[derive(Clone)]
struct OfficeSession {
  blob: OfficeBlobPath,
  content_type: String,
  document_key: String,
  editable: bool,
  created_at: Instant,
}

impl OfficeSession {
  fn is_expired(&self) -> bool {
    self.created_at.elapsed() >= SESSION_TTL
  }
}

#[derive(Clone, Debug)]
enum OfficeBlobPath {
  V0 {
    workspace_id: Uuid,
    file_id: String,
  },
  V1 {
    workspace_id: Uuid,
    parent_dir: String,
    file_id: String,
  },
}

impl BlobKey for OfficeBlobPath {
  fn workspace_id(&self) -> &Uuid {
    match self {
      Self::V0 { workspace_id, .. } | Self::V1 { workspace_id, .. } => workspace_id,
    }
  }

  fn object_key(&self) -> String {
    match self {
      Self::V0 {
        workspace_id,
        file_id,
      } => format!("{workspace_id}/{file_id}"),
      Self::V1 {
        workspace_id,
        parent_dir,
        file_id,
      } => format!("{workspace_id}/{parent_dir}/{file_id}"),
    }
  }

  fn blob_metadata_key(&self) -> String {
    match self {
      Self::V0 { file_id, .. } => file_id.clone(),
      Self::V1 {
        parent_dir,
        file_id,
        ..
      } => format!("{parent_dir}_{file_id}"),
    }
  }

  fn e_tag(&self) -> &str {
    match self {
      Self::V0 { file_id, .. } | Self::V1 { file_id, .. } => file_id,
    }
  }
}

#[instrument(skip_all, err)]
async fn create_session_handler(
  user_uuid: UserUuid,
  workspace_id: web::Path<Uuid>,
  state: Data<AppState>,
  request: Json<CreateOfficeSessionRequest>,
) -> actix_web::Result<JsonAppResponse<OfficeSessionResponse>> {
  let document_server = require_document_server(&state.config.document_server)?;
  probe_document_server(document_server).await?;

  let workspace_id = workspace_id.into_inner();
  let uid = state.user_cache.get_user_uid(&user_uuid).await?;
  state
    .workspace_access_control
    .enforce_action(&uid, &workspace_id, Action::Read)
    .await?;

  let request = request.into_inner();
  let editable = if request.editable {
    match state
      .workspace_access_control
      .enforce_action(&uid, &workspace_id, Action::Write)
      .await
    {
      Ok(()) => true,
      Err(AppError::NotEnoughPermissions) => false,
      Err(error) => return Err(error.into()),
    }
  } else {
    false
  };

  let requested_session_id = request
    .session_id
    .as_deref()
    .map(Uuid::parse_str)
    .transpose()
    .map_err(|_| AppError::InvalidRequest("Invalid document editing session ID".to_string()))?;
  let previous_session = match requested_session_id {
    Some(session_id) => Some(
      state
        .office_sessions
        .get(&session_id)
        .ok_or_else(|| AppError::RecordNotFound("Document editing session expired".to_string()))?,
    ),
    None => None,
  };

  let blob = parse_storage_url(&request.storage_url, workspace_id)?;
  if previous_session
    .as_ref()
    .is_some_and(|session| session.blob.object_key() != blob.object_key())
  {
    return Err(
      AppError::InvalidRequest("Document editing session belongs to another file".to_string())
        .into(),
    );
  }
  let metadata = state
    .bucket_storage
    .get_blob_metadata(&workspace_id, &blob.blob_metadata_key())
    .await?;
  if metadata.file_size < 0
    || metadata.file_size as u64 > document_server.max_file_size_bytes as u64
  {
    return Err(
      AppError::PayloadTooLarge("Office document exceeds the configured limit".to_string()).into(),
    );
  }
  let (file_type, document_type) = office_document_type(&request.file_name)?;
  let session_id = requested_session_id.unwrap_or_else(Uuid::new_v4);
  let callback_base = document_server.callback_url();
  let document_url = format!("{callback_base}/api/office/session/{session_id}/document");
  let callback_url = format!("{callback_base}/api/office/session/{session_id}/callback");
  let document_key = previous_session
    .map(|session| session.document_key)
    .unwrap_or_else(|| office_document_key(&blob, session_id));

  let mut editor_config = json!({
    "document": {
      "fileType": file_type,
      "key": document_key.clone(),
      "title": request.file_name,
      "url": document_url,
      "permissions": {
        "edit": editable,
        "download": true,
        "print": true
      }
    },
    "documentType": document_type,
    "type": "desktop",
    "editorConfig": {
      "mode": if editable { "edit" } else { "view" },
      "lang": "en",
      "callbackUrl": callback_url,
      "user": {
        "id": format!("{}", *user_uuid),
        "name": request.user_name
      },
      "customization": office_editor_customization(request.is_dark)
    }
  });
  let token = encode(
    &Header::new(Algorithm::HS256),
    &editor_config,
    &EncodingKey::from_secret(document_server.jwt_secret().as_bytes()),
  )
  .map_err(|error| AppError::Internal(error.into()))?;
  editor_config["token"] = Value::String(token);

  state.office_sessions.insert(
    session_id,
    OfficeSession {
      blob,
      content_type: metadata.file_type,
      document_key,
      editable,
      created_at: Instant::now(),
    },
  );

  Ok(
    AppResponse::Ok()
      .with_data(OfficeSessionResponse {
        session_id: session_id.to_string(),
        document_server_url: document_server.public_url().to_string(),
        editor_config,
      })
      .into(),
  )
}

#[derive(Deserialize)]
struct OfficeSessionPath {
  session_id: Uuid,
}

#[instrument(skip_all)]
async fn get_document_handler(
  state: Data<AppState>,
  path: web::Path<OfficeSessionPath>,
) -> actix_web::Result<HttpResponse> {
  let Some(session) = state.office_sessions.get(&path.session_id) else {
    return Ok(HttpResponse::NotFound().finish());
  };
  let bytes = state.bucket_storage.get_blob(&session.blob).await?;
  Ok(
    HttpResponse::Ok()
      .insert_header((CONTENT_TYPE, session.content_type))
      .insert_header((CONTENT_LENGTH, bytes.len()))
      .insert_header((CACHE_CONTROL, "private, no-store"))
      .body(bytes),
  )
}

#[derive(Serialize)]
struct OnlyOfficeCallbackResponse {
  error: u8,
}

async fn callback_handler(
  request: HttpRequest,
  state: Data<AppState>,
  path: web::Path<OfficeSessionPath>,
  body: Json<Value>,
) -> Json<OnlyOfficeCallbackResponse> {
  match process_callback(&request, &state, path.session_id, body.into_inner()).await {
    Ok(()) => Json(OnlyOfficeCallbackResponse { error: 0 }),
    Err(callback_error) => {
      error!(
        session_id = %path.session_id,
        error = %callback_error,
        "ONLYOFFICE callback failed"
      );
      Json(OnlyOfficeCallbackResponse { error: 1 })
    },
  }
}

async fn process_callback(
  request: &HttpRequest,
  state: &AppState,
  session_id: Uuid,
  body: Value,
) -> Result<(), AppError> {
  let document_server = require_document_server(&state.config.document_server)?;
  let session = state
    .office_sessions
    .get(&session_id)
    .ok_or_else(|| AppError::RecordNotFound("Document editing session expired".to_string()))?;
  let payload = verified_callback_payload(request, &body, document_server.jwt_secret().as_bytes())?;
  let status = payload
    .get("status")
    .and_then(Value::as_i64)
    .ok_or_else(|| AppError::InvalidRequest("Document callback status is missing".to_string()))?;
  validate_callback_key(&payload, &session.document_key)?;

  match status {
    2 | 6 if session.editable => {
      let callback_url = payload
        .get("url")
        .and_then(Value::as_str)
        .ok_or_else(|| AppError::InvalidRequest("Document callback URL is missing".to_string()))?;
      let internal_url = rewrite_document_server_url(callback_url, document_server.internal_url())?;
      let bytes =
        download_edited_document(internal_url, document_server.max_file_size_bytes).await?;
      let file_size = bytes.len();
      state
        .bucket_storage
        .replace_blob_with_content_type(
          session.blob,
          ByteStream::from(bytes),
          session.content_type,
          file_size,
        )
        .await?;
      if status == 2 {
        state.office_sessions.remove(&session_id);
      }
    },
    2 | 6 => {
      return Err(AppError::NotEnoughPermissions);
    },
    4 => state.office_sessions.remove(&session_id),
    _ => {},
  }

  Ok(())
}

fn validate_callback_key(payload: &Value, expected_key: &str) -> Result<(), AppError> {
  let key = payload
    .get("key")
    .and_then(Value::as_str)
    .ok_or_else(|| AppError::InvalidRequest("Document callback key is missing".to_string()))?;
  if key != expected_key {
    return Err(AppError::UserUnAuthorized(
      "Document callback key does not match its session".to_string(),
    ));
  }
  Ok(())
}

fn require_document_server(
  setting: &DocumentServerSetting,
) -> Result<&DocumentServerSetting, AppError> {
  if setting.is_configured() {
    Ok(setting)
  } else {
    Err(AppError::ServiceTemporaryUnavailable(
      "Managed document editing is not configured".to_string(),
    ))
  }
}

async fn probe_document_server(setting: &DocumentServerSetting) -> Result<(), AppError> {
  let health_url = format!("{}/healthcheck", setting.internal_url());
  let response = reqwest::Client::builder()
    .timeout(Duration::from_secs(5))
    .build()
    .map_err(|error| AppError::Internal(error.into()))?
    .get(health_url)
    .send()
    .await
    .map_err(|error| AppError::ServiceTemporaryUnavailable(error.to_string()))?;
  let healthy = response.status().is_success()
    && response
      .text()
      .await
      .map_err(|error| AppError::ServiceTemporaryUnavailable(error.to_string()))?
      .trim()
      .eq_ignore_ascii_case("true");
  if healthy {
    Ok(())
  } else {
    Err(AppError::ServiceTemporaryUnavailable(
      "The document server is not ready".to_string(),
    ))
  }
}

fn parse_storage_url(
  storage_url: &str,
  expected_workspace_id: Uuid,
) -> Result<OfficeBlobPath, AppError> {
  let url = Url::parse(storage_url)
    .map_err(|_| AppError::InvalidRequest("Invalid file storage URL".to_string()))?;
  let segments = url
    .path_segments()
    .ok_or_else(|| AppError::InvalidRequest("Invalid file storage URL".to_string()))?
    .collect::<Vec<_>>();

  let path = match segments.as_slice() {
    ["api", "file_storage", workspace_id, "blob", file_id] if !file_id.is_empty() => {
      OfficeBlobPath::V0 {
        workspace_id: workspace_id.parse()?,
        file_id: (*file_id).to_string(),
      }
    },
    ["api", "file_storage", workspace_id, "v1", "blob", parent_dir, file_id]
      if !parent_dir.is_empty() && !file_id.is_empty() =>
    {
      OfficeBlobPath::V1 {
        workspace_id: workspace_id.parse()?,
        parent_dir: percent_decode_str(parent_dir)
          .decode_utf8()
          .map_err(|_| AppError::InvalidRequest("Invalid file storage URL".to_string()))?
          .into_owned(),
        file_id: (*file_id).to_string(),
      }
    },
    _ => {
      return Err(AppError::InvalidRequest(
        "The file is not stored in AppFlowy Cloud".to_string(),
      ));
    },
  };

  if path.workspace_id() != &expected_workspace_id {
    return Err(AppError::NotEnoughPermissions);
  }
  Ok(path)
}

fn office_document_type(file_name: &str) -> Result<(String, &'static str), AppError> {
  let extension = file_name
    .rsplit_once('.')
    .map(|(_, extension)| extension.to_ascii_lowercase())
    .filter(|extension| !extension.is_empty())
    .ok_or_else(|| AppError::InvalidRequest("Office file extension is missing".to_string()))?;
  let document_type = match extension.as_str() {
    "doc" | "docx" | "odt" | "rtf" | "txt" => "word",
    "xls" | "xlsx" | "ods" | "csv" => "cell",
    "ppt" | "pptx" | "odp" => "slide",
    _ => {
      return Err(AppError::InvalidRequest(
        "This file type is not supported by the document server".to_string(),
      ));
    },
  };
  Ok((extension, document_type))
}

fn office_document_key(blob: &OfficeBlobPath, session_id: Uuid) -> String {
  let digest = Sha256::digest(format!("{}:{session_id}", blob.object_key()).as_bytes());
  format!("{digest:x}")[..40].to_string()
}

fn office_ui_theme(is_dark: bool) -> &'static str {
  if is_dark {
    "theme-night"
  } else {
    "theme-white"
  }
}

/// AppFlowy already shows the file name around the embedded editor.
fn office_editor_customization(is_dark: bool) -> Value {
  json!({
    "autosave": true,
    "forcesave": true,
    "compactHeader": true,
    "hideRightMenu": true,
    "toolbarHideFileName": true,
    "suggestFeature": false,
    "features": { "featuresTips": false },
    "uiTheme": office_ui_theme(is_dark)
  })
}

fn verified_callback_payload(
  request: &HttpRequest,
  body: &Value,
  secret: &[u8],
) -> Result<Value, AppError> {
  let token = request
    .headers()
    .get(AUTHORIZATION)
    .and_then(|header| header.to_str().ok())
    .and_then(|header| header.strip_prefix("Bearer "))
    .or_else(|| body.get("token").and_then(Value::as_str))
    .ok_or_else(|| AppError::UserUnAuthorized("Document callback token is missing".to_string()))?;

  let mut validation = Validation::new(Algorithm::HS256);
  validation.required_spec_claims.clear();
  validation.validate_exp = false;
  let claims = decode::<Value>(token, &DecodingKey::from_secret(secret), &validation)
    .map_err(|_| AppError::UserUnAuthorized("Document callback token is invalid".to_string()))?
    .claims;
  let payload = claims.get("payload").cloned().unwrap_or(claims);
  if payload.get("status").is_none() {
    return Err(AppError::InvalidRequest(
      "Signed document callback payload is missing".to_string(),
    ));
  }
  Ok(payload)
}

fn rewrite_document_server_url(callback_url: &str, internal_origin: &str) -> Result<Url, AppError> {
  let callback = Url::parse(callback_url)
    .map_err(|_| AppError::InvalidRequest("Invalid document callback URL".to_string()))?;
  let mut internal = Url::parse(internal_origin)
    .map_err(|_| AppError::Internal(anyhow::anyhow!("Invalid document server URL")))?;
  internal.set_path(callback.path());
  internal.set_query(callback.query());
  internal.set_fragment(None);
  Ok(internal)
}

async fn download_edited_document(url: Url, max_file_size: usize) -> Result<Vec<u8>, AppError> {
  let response = reqwest::Client::builder()
    .timeout(Duration::from_secs(60))
    .build()
    .map_err(|error| AppError::Internal(error.into()))?
    .get(url)
    .send()
    .await
    .map_err(|error| AppError::ServiceTemporaryUnavailable(error.to_string()))?
    .error_for_status()
    .map_err(|error| AppError::ServiceTemporaryUnavailable(error.to_string()))?;
  if response
    .content_length()
    .is_some_and(|size| size > max_file_size as u64)
  {
    return Err(AppError::PayloadTooLarge(
      "Edited office document exceeds the configured limit".to_string(),
    ));
  }

  let mut bytes = Vec::new();
  let mut stream = response.bytes_stream();
  while let Some(chunk) = stream
    .try_next()
    .await
    .map_err(|error| AppError::ServiceTemporaryUnavailable(error.to_string()))?
  {
    if bytes.len().saturating_add(chunk.len()) > max_file_size {
      return Err(AppError::PayloadTooLarge(
        "Edited office document exceeds the configured limit".to_string(),
      ));
    }
    bytes.extend_from_slice(&chunk);
  }
  Ok(bytes)
}

#[cfg(test)]
mod tests {
  use actix_web::test::TestRequest;
  use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
  use serde_json::json;

  use super::*;

  #[test]
  fn parses_supported_cloud_blob_urls() {
    let workspace_id = Uuid::new_v4();
    let v0 = parse_storage_url(
      &format!("https://cloud.example/api/file_storage/{workspace_id}/blob/report.docx"),
      workspace_id,
    )
    .unwrap();
    assert_eq!(v0.object_key(), format!("{workspace_id}/report.docx"));

    let v1 = parse_storage_url(
      &format!(
        "https://cloud.example/api/file_storage/{workspace_id}/v1/blob/folder%20one/file-id"
      ),
      workspace_id,
    )
    .unwrap();
    assert_eq!(
      v1.object_key(),
      format!("{workspace_id}/folder one/file-id")
    );
  }

  #[test]
  fn rejects_a_blob_from_another_workspace() {
    let workspace_id = Uuid::new_v4();
    let other_workspace_id = Uuid::new_v4();
    let result = parse_storage_url(
      &format!("https://cloud.example/api/file_storage/{other_workspace_id}/blob/report.docx"),
      workspace_id,
    );
    assert!(matches!(result, Err(AppError::NotEnoughPermissions)));
  }

  #[test]
  fn rewrites_only_the_untrusted_callback_origin() {
    let result = rewrite_document_server_url(
      "https://public.example/cache/files/result.docx?token=one",
      "http://onlyoffice",
    )
    .unwrap();
    assert_eq!(
      result.as_str(),
      "http://onlyoffice/cache/files/result.docx?token=one"
    );
  }

  #[test]
  fn uses_modern_editor_themes() {
    assert_eq!(office_ui_theme(false), "theme-white");
    assert_eq!(office_ui_theme(true), "theme-night");
  }

  #[test]
  fn leaves_naming_the_file_to_appflowy() {
    let customization = office_editor_customization(true);
    assert_eq!(customization["toolbarHideFileName"], true);
    assert_eq!(customization["suggestFeature"], false);
    assert_eq!(customization["features"]["featuresTips"], false);
    assert_eq!(customization["compactHeader"], true);
    assert_eq!(customization["uiTheme"], "theme-night");
  }

  #[test]
  fn uses_the_signed_callback_payload() {
    let secret = b"secret";
    let claims = json!({
      "payload": {
        "status": 2,
        "key": "document-key",
        "url": "http://onlyoffice/result.docx"
      }
    });
    let token = encode(
      &Header::new(Algorithm::HS256),
      &claims,
      &EncodingKey::from_secret(secret),
    )
    .unwrap();
    let request = TestRequest::default()
      .insert_header((AUTHORIZATION, format!("Bearer {token}")))
      .to_http_request();
    let payload = verified_callback_payload(
      &request,
      &json!({"status": 6, "url": "http://attacker.invalid/file"}),
      secret,
    )
    .unwrap();
    assert_eq!(payload["status"], 2);
    assert_eq!(payload["url"], "http://onlyoffice/result.docx");
  }

  #[test]
  fn binds_callbacks_to_their_document_session() {
    let payload = json!({"status": 2, "key": "document-key"});
    assert!(validate_callback_key(&payload, "document-key").is_ok());
    assert!(matches!(
      validate_callback_key(&payload, "another-key"),
      Err(AppError::UserUnAuthorized(_))
    ));
  }
}
