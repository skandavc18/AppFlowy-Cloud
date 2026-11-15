use crate::file::{BucketClient, BucketStorage, ResponseBlob};
use anyhow::anyhow;
use app_error::AppError;
use async_trait::async_trait;
use aws_sdk_s3::operation::delete_object::DeleteObjectOutput;

use std::ops::Deref;
use std::time::{Duration, SystemTime};

use aws_sdk_s3::error::{DisplayErrorContext, SdkError};
use aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput;
use aws_sdk_s3::operation::get_object::GetObjectError;

use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
  CompletedMultipartUpload, CompletedPart, Delete, Object, ObjectIdentifier,
};
use aws_sdk_s3::Client;
use database_entity::file_dto::{
  CompleteUploadRequest, CreateUploadRequest, CreateUploadResponse, UploadPartData,
  UploadPartResponse,
};

use tracing::{debug, error, trace};

pub type S3BucketStorage = BucketStorage<AwsS3BucketClientImpl>;

impl S3BucketStorage {
  pub fn from_bucket_impl(client: AwsS3BucketClientImpl, pg_pool: sqlx::PgPool) -> Self {
    Self::new(client, pg_pool)
  }
}

#[derive(Clone)]
pub struct AwsS3BucketClientImpl {
  client: Client,
  bucket: String,
  endpoint: String,
  presigned_url_endpoint: Option<String>,
}

impl AwsS3BucketClientImpl {
  pub fn new(
    client: Client,
    bucket: String,
    endpoint: String,
    presigned_url_endpoint: Option<String>,
  ) -> Self {
    debug_assert!(!bucket.is_empty());
    AwsS3BucketClientImpl {
      client,
      bucket,
      endpoint,
      presigned_url_endpoint,
    }
  }

  pub async fn gen_presigned_url(
    &self,
    s3_key: &str,
    content_length: u64,
    expires_in_secs: u64,
  ) -> Result<String, AppError> {
    let expires_in = Duration::from_secs(expires_in_secs);
    let config = PresigningConfig::builder()
      .start_time(SystemTime::now())
      .expires_in(expires_in)
      .build()
      .map_err(|e| AppError::S3ResponseError(e.to_string()))?;

    // There is no easy way to restrict file size of the upload (default limit max 5GB using PUT or other upload methods)
    // https://github.com/aws/aws-sdk-net/issues/424
    //
    // consider using POST:
    // https://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-HTTPPOSTConstructPolicy.html
    let put_object_req = self
      .client
      .put_object()
      .bucket(&self.bucket)
      .key(s3_key)
      .content_type("application/zip")
      .content_length(content_length as i64)
      .presigned(config)
      .await
      .map_err(|err| AppError::Internal(anyhow!("Generate presigned url failed: {:?}", err)))?;
    let url = put_object_req.uri().to_string();

    let public_url = self
      .presigned_url_endpoint
      .as_ref()
      .map_or(url.clone(), |presigned| {
        url.replace(&self.endpoint, presigned)
      });
    trace!(
      "generated presigned url: {}, public presigned url:{}, endpoint:{}, presigned_url_endpoint:{:?}",
      url,
      public_url,
      self.endpoint,
      self.presigned_url_endpoint
    );
    Ok(public_url)
  }

  async fn complete_upload_and_get_metadata(
    &self,
    object_key: &str,
    upload_id: &str,
    completed_multipart_upload: CompletedMultipartUpload,
  ) -> Result<(usize, String), AppError> {
    // Complete the multipart upload
    let _ = self
      .client
      .complete_multipart_upload()
      .bucket(&self.bucket)
      .key(object_key)
      .upload_id(upload_id)
      .multipart_upload(completed_multipart_upload)
      .send()
      .await
      .map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;

    // Retrieve the object metadata using head_object
    let head_object_result = self
      .client
      .head_object()
      .bucket(&self.bucket)
      .key(object_key)
      .send()
      .await
      .map_err(|e| AppError::Internal(anyhow::anyhow!(e)))?;

    let content_len = head_object_result
      .content_length()
      .ok_or_else(|| AppError::Unhandled("Content-Length not found".to_string()))?;
    let content_type = head_object_result
      .content_type()
      .map(|s| s.to_string())
      .unwrap_or_else(|| "application/octet-stream".to_string());

    trace!(
      "completed upload to S3: {} ({} bytes)",
      object_key,
      content_len
    );

    Ok((content_len as usize, content_type))
  }
}

/// An object found by [AwsS3BucketClientImpl::list_dir_entries].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ObjectEntry {
  pub key: String,
  /// Size in bytes.
  pub size: i64,
  /// Last modification time in Unix epoch milliseconds.
  pub last_modified_ms: i64,
}

impl S3ObjectEntry {
  pub fn from_object(object: &Object) -> Option<Self> {
    let last_modified_ms = object
      .last_modified()
      .and_then(|time| time.to_millis().ok())
      .unwrap_or_default();
    Some(Self {
      key: object.key()?.to_string(),
      size: object.size().unwrap_or_default(),
      last_modified_ms,
    })
  }
}

/// Objects and rolled-up prefixes found by [AwsS3BucketClientImpl::list_dir_entries].
#[derive(Debug, Default)]
pub struct S3DirListing {
  pub objects: Vec<S3ObjectEntry>,
  /// Keys rolled up by the delimiter, such as `dir/sub/` for `dir/sub/file` when listing `dir/`.
  pub common_prefixes: Vec<String>,
}

/// An object body being downloaded, see [AwsS3BucketClientImpl::get_object_stream].
pub struct S3ObjectStream {
  pub content_length: Option<i64>,
  pub body: ByteStream,
}

impl AwsS3BucketClientImpl {
  /// Lists every object under `prefix`, following continuation tokens.
  ///
  /// With a `delimiter`, `objects` only holds the direct children and deeper keys are rolled up
  /// into `common_prefixes`.
  pub async fn list_dir_entries(
    &self,
    prefix: &str,
    delimiter: Option<&str>,
  ) -> Result<S3DirListing, AppError> {
    let mut listing = S3DirListing::default();
    let mut continuation_token = None;
    loop {
      let output = self
        .client
        .list_objects_v2()
        .bucket(&self.bucket)
        .prefix(prefix)
        .set_delimiter(delimiter.map(str::to_string))
        .set_continuation_token(continuation_token.take())
        .send()
        .await
        .map_err(|err| s3_error("Failed to list objects", err))?;

      for object in output.contents() {
        if let Some(entry) = S3ObjectEntry::from_object(object) {
          listing.objects.push(entry);
        }
      }
      for common_prefix in output.common_prefixes() {
        if let Some(value) = common_prefix.prefix() {
          listing.common_prefixes.push(value.to_string());
        }
      }

      match output.next_continuation_token() {
        Some(token) if output.is_truncated().unwrap_or_default() => {
          continuation_token = Some(token.to_string());
        },
        _ => break,
      }
    }
    Ok(listing)
  }

  /// Size and modification time of one object, or `None` when it does not exist.
  ///
  /// This reads a listing rather than HeadObject so the time has the precision of
  /// [Self::list_dir_entries], as the Last-Modified header returned by HeadObject only has whole
  /// seconds.
  pub async fn object_entry(&self, key: &str) -> Result<Option<S3ObjectEntry>, AppError> {
    let output = self
      .client
      .list_objects_v2()
      .bucket(&self.bucket)
      .prefix(key)
      .max_keys(1)
      .send()
      .await
      .map_err(|err| s3_error("Failed to read object metadata", err))?;
    for object in output.contents() {
      if object.key() == Some(key) {
        return Ok(S3ObjectEntry::from_object(object));
      }
    }
    Ok(None)
  }

  /// Starts downloading an object, or returns [AppError::RecordNotFound] when it does not exist.
  pub async fn get_object_stream(&self, key: &str) -> Result<S3ObjectStream, AppError> {
    let result = self
      .client
      .get_object()
      .bucket(&self.bucket)
      .key(key)
      .send()
      .await;
    match result {
      Ok(output) => Ok(S3ObjectStream {
        content_length: output.content_length(),
        body: output.body,
      }),
      Err(err) if is_no_such_key(&err) => Err(AppError::RecordNotFound(key.to_string())),
      Err(err) => Err(s3_error("Failed to get object", err)),
    }
  }

  /// Starts a multipart upload and returns its upload id.
  pub async fn create_multipart_upload(
    &self,
    key: &str,
    content_type: &str,
  ) -> Result<String, AppError> {
    let output = self
      .client
      .create_multipart_upload()
      .bucket(&self.bucket)
      .key(key)
      .content_type(content_type)
      .send()
      .await
      .map_err(|err| s3_error("Failed to create multipart upload", err))?;
    output
      .upload_id()
      .map(str::to_string)
      .ok_or_else(|| AppError::Internal(anyhow!("missing upload id")))
  }

  pub async fn upload_multipart_part(
    &self,
    key: &str,
    upload_id: &str,
    part_number: i32,
    body: ByteStream,
  ) -> Result<CompletedPart, AppError> {
    let output = self
      .client
      .upload_part()
      .bucket(&self.bucket)
      .key(key)
      .upload_id(upload_id)
      .part_number(part_number)
      .body(body)
      .send()
      .await
      .map_err(|err| s3_error("Failed to upload part", err))?;
    let e_tag = output
      .e_tag()
      .ok_or_else(|| AppError::Internal(anyhow!("missing part ETag")))?;
    let part = CompletedPart::builder()
      .e_tag(e_tag)
      .part_number(part_number)
      .build();
    Ok(part)
  }

  pub async fn complete_multipart_upload(
    &self,
    key: &str,
    upload_id: &str,
    parts: Vec<CompletedPart>,
  ) -> Result<(), AppError> {
    let upload = CompletedMultipartUpload::builder()
      .set_parts(Some(parts))
      .build();
    self
      .client
      .complete_multipart_upload()
      .bucket(&self.bucket)
      .key(key)
      .upload_id(upload_id)
      .multipart_upload(upload)
      .send()
      .await
      .map_err(|err| s3_error("Failed to complete multipart upload", err))?;
    Ok(())
  }

  pub async fn abort_multipart_upload(&self, key: &str, upload_id: &str) -> Result<(), AppError> {
    self
      .client
      .abort_multipart_upload()
      .bucket(&self.bucket)
      .key(key)
      .upload_id(upload_id)
      .send()
      .await
      .map_err(|err| s3_error("Failed to abort multipart upload", err))?;
    Ok(())
  }
}

fn is_no_such_key(err: &SdkError<GetObjectError>) -> bool {
  let status = err.raw_response().map(|raw| raw.status().as_u16());
  matches!(err.as_service_error(), Some(GetObjectError::NoSuchKey(_))) || status == Some(404)
}

/// Maps a failed S3 request to an [AppError]. Failing to reach S3, or S3 failing to serve the
/// request, is reported as temporary.
fn s3_error<E>(action: &str, err: SdkError<E>) -> AppError
where
  E: std::error::Error + 'static,
{
  let status = err.raw_response().map(|raw| raw.status().as_u16());
  let temporary = match &err {
    SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) => true,
    _ => status.is_some_and(|status| status >= 500),
  };
  let message = format!("{action}: {}", DisplayErrorContext(&err));
  if temporary {
    AppError::ServiceTemporaryUnavailable(message)
  } else {
    AppError::Internal(anyhow!(message))
  }
}

#[async_trait]
impl BucketClient for AwsS3BucketClientImpl {
  type ResponseData = S3ResponseData;

  async fn put_blob(
    &self,
    object_key: &str,
    content: ByteStream,
    content_type: Option<&str>,
  ) -> Result<(), AppError> {
    self
      .client
      .put_object()
      .bucket(&self.bucket)
      .key(object_key)
      .body(content)
      .content_type(content_type.unwrap_or("application/octet-stream"))
      .send()
      .await
      .map_err(|err| match err {
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ServiceError(_) => {
          AppError::ServiceTemporaryUnavailable(format!("Failed to upload object to S3: {}", err))
        },
        _ => AppError::Internal(anyhow!("Failed to upload object to S3: {}", err)),
      })?;

    trace!("put object to S3: {}", object_key);

    Ok(())
  }

  async fn put_blob_with_content_type(
    &self,
    object_key: &str,
    stream: ByteStream,
    content_type: &str,
  ) -> Result<(), AppError> {
    self
      .client
      .put_object()
      .bucket(&self.bucket)
      .key(object_key)
      .body(stream)
      .content_type(content_type)
      .send()
      .await
      .map_err(|err| match err {
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) | SdkError::ServiceError(_) => {
          AppError::ServiceTemporaryUnavailable(format!("Failed to upload object to S3: {}", err))
        },
        _ => AppError::Internal(anyhow!("Failed to upload object to S3: {}", err)),
      })?;

    trace!("put object to S3: {} ({})", object_key, content_type);

    Ok(())
  }

  async fn delete_blob(&self, object_key: &str) -> Result<Self::ResponseData, AppError> {
    let output = self
      .client
      .delete_object()
      .bucket(&self.bucket)
      .key(object_key)
      .send()
      .await
      .map_err(|err| anyhow!("Failed to delete object to S3: {}", err))?;

    trace!("deleted object from S3: {}", object_key);

    Ok(S3ResponseData::from(output))
  }

  async fn delete_blobs(&self, object_keys: Vec<String>) -> Result<(), AppError> {
    const CHUNK_SIZE: usize = 500;
    let mut deleted = 0;
    for chunk in object_keys.chunks(CHUNK_SIZE) {
      let mut delete_object_ids = Vec::with_capacity(CHUNK_SIZE);
      for obj in chunk {
        let obj_id = ObjectIdentifier::builder()
          .key(obj)
          .build()
          .map_err(|err| {
            AppError::Internal(anyhow!("Failed to create object identifier: {}", err))
          })?;
        delete_object_ids.push(obj_id);
      }
      let len = delete_object_ids.len();
      let res = self
        .client
        .delete_objects()
        .bucket(&self.bucket)
        .delete(
          Delete::builder()
            .set_objects(Some(delete_object_ids))
            .build()
            .map_err(|err| {
              AppError::Internal(anyhow!("Failed to create delete object request: {}", err))
            })?,
        )
        .send()
        .await;

      match res {
        Ok(_) => deleted += len,
        Err(err) => {
          tracing::warn!("failed to delete {} objects: {}", len, err);
          tokio::time::sleep(Duration::from_millis(100)).await;
        },
      }
    }

    trace!("deleted {} objects from S3", deleted);
    Ok(())
  }

  async fn get_blob(&self, object_key: &str) -> Result<Self::ResponseData, AppError> {
    match self
      .client
      .get_object()
      .bucket(&self.bucket)
      .key(object_key)
      .send()
      .await
    {
      Ok(output) => match output.body.collect().await {
        Ok(body) => {
          let data = body.into_bytes().to_vec();

          trace!("get object from S3: {} ({} bytes)", object_key, data.len());

          Ok(S3ResponseData::new_with_data(data, output.content_type))
        },
        Err(err) => Err(AppError::from(anyhow!("Failed to collect body: {}", err))),
      },
      Err(SdkError::ServiceError(service_err)) => match service_err.err() {
        GetObjectError::NoSuchKey(_) => Err(AppError::RecordNotFound(format!(
          "blob not found for key:{object_key}"
        ))),
        _ => Err(AppError::from(anyhow!(
          "Failed to get object from S3: {:?}",
          service_err
        ))),
      },
      Err(err) => Err(AppError::from(anyhow!(
        "Failed to get object from S3: {}",
        err
      ))),
    }
  }

  /// Create a new upload session
  /// https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
  async fn create_upload(
    &self,
    object_key: &str,
    req: CreateUploadRequest,
  ) -> Result<CreateUploadResponse, AppError> {
    trace!("creating multi-part upload to S3: {} - {}", object_key, req);

    let multipart_upload_res = self
      .client
      .create_multipart_upload()
      .bucket(&self.bucket)
      .key(object_key)
      .content_type(req.content_type)
      .send()
      .await
      .map_err(|err| anyhow!(format!("Failed to create upload: {:?}", err)))?;

    match multipart_upload_res.upload_id {
      None => Err(anyhow!("Failed to create upload: upload_id is None").into()),
      Some(upload_id) => Ok(CreateUploadResponse {
        file_id: req.file_id,
        upload_id,
      }),
    }
  }

  async fn upload_part(
    &self,
    object_key: &str,
    req: UploadPartData,
  ) -> Result<UploadPartResponse, AppError> {
    if req.body.is_empty() {
      return Err(AppError::InvalidRequest("body is empty".to_string()));
    }
    trace!("multi-part upload to s3: {} - {}", object_key, req,);
    let body = ByteStream::from(req.body);
    let upload_part_res = self
      .client
      .upload_part()
      .bucket(&self.bucket)
      .key(object_key)
      .upload_id(&req.upload_id)
      .part_number(req.part_number)
      .body(body)
      .send()
      .await
      .map_err(|err| anyhow!(format!("Failed to upload part: {:?}", err)))?;

    match upload_part_res.e_tag {
      None => Err(anyhow!("Failed to upload part: e_tag is None").into()),
      Some(e_tag) => Ok(UploadPartResponse {
        part_num: req.part_number,
        e_tag,
      }),
    }
  }

  /// Return the content length and content type of the uploaded object
  async fn complete_upload(
    &self,
    object_key: &str,
    req: CompleteUploadRequest,
  ) -> Result<(usize, String), AppError> {
    let parts = req
      .parts
      .into_iter()
      .map(|part| {
        CompletedPart::builder()
          .e_tag(part.e_tag)
          .part_number(part.part_number)
          .build()
      })
      .collect::<Vec<_>>();
    let completed_multipart_upload = CompletedMultipartUpload::builder()
      .set_parts(Some(parts))
      .build();

    self
      .complete_upload_and_get_metadata(object_key, &req.upload_id, completed_multipart_upload)
      .await
  }

  async fn remove_dir(&self, parent_dir: &str) -> Result<(), AppError> {
    let mut continuation_token = None;
    loop {
      let list_objects = self
        .client
        .list_objects_v2()
        .bucket(&self.bucket)
        .prefix(parent_dir)
        .set_continuation_token(continuation_token.clone())
        .send()
        .await
        .map_err(|err| anyhow!("Failed to list object: {}", err))?;

      let mut objects_to_delete: Vec<ObjectIdentifier> = list_objects
        .contents
        .unwrap_or_default()
        .into_iter()
        .filter_map(|object| {
          object.key.and_then(|key| {
            ObjectIdentifier::builder()
              .key(key)
              .build()
              .map_err(|e| {
                error!("Error building ObjectIdentifier: {:?}", e);
                e
              })
              .ok()
          })
        })
        .collect();

      trace!(
        "deleting {} objects at directory: {}",
        objects_to_delete.len(),
        parent_dir
      );

      // Step 2: Delete the listed objects in batches of 1000
      while !objects_to_delete.is_empty() {
        debug!(
          "Deleting batch of {} objects from S3 bucket: {}",
          objects_to_delete.len(),
          self.bucket
        );

        let batch = if objects_to_delete.len() > 1000 {
          objects_to_delete.split_off(1000)
        } else {
          Vec::new()
        };

        trace!("deleting objects: {:?}", objects_to_delete);
        let delete = Delete::builder()
          .set_objects(Some(objects_to_delete))
          .build()
          .map_err(|e| {
            println!("Error building Delete: {:?}", e);
            e
          })
          .map_err(|err| anyhow!("Failed to build delete object: {}", err))?;

        let delete_objects_output: DeleteObjectsOutput = self
          .client
          .delete_objects()
          .bucket(&self.bucket)
          .delete(delete)
          .send()
          .await
          .map_err(|err| anyhow!("Failed to delete object: {:?}", err))?;

        if let Some(errors) = delete_objects_output.errors {
          for error in errors {
            error!("Error deleting object: {:?}", error);
          }
        }

        objects_to_delete = batch;
      }

      // is_truncated is true if there are more objects to list. If it's false, it means we have listed all objects in the directory
      match list_objects.is_truncated {
        None => break,
        Some(is_truncated) => {
          if !is_truncated {
            break;
          }
        },
      }

      continuation_token = list_objects.next_continuation_token;
    }

    Ok(())
  }

  async fn list_dir(&self, dir: &str, limit: usize) -> Result<Vec<String>, AppError> {
    let list_objects = self
      .client
      .list_objects_v2()
      .bucket(&self.bucket)
      .prefix(dir)
      .max_keys(limit as i32)
      .send()
      .await
      .map_err(|err| anyhow!("Failed to list object: {}", err))?;

    Ok(
      list_objects
        .contents
        .unwrap_or_default()
        .into_iter()
        .filter_map(|o| o.key)
        .collect(),
    )
  }
}

#[derive(Debug)]
pub struct S3ResponseData {
  data: Vec<u8>,
  content_type: Option<String>,
}

impl Deref for S3ResponseData {
  type Target = Vec<u8>;

  fn deref(&self) -> &Self::Target {
    &self.data
  }
}

impl ResponseBlob for S3ResponseData {
  fn to_blob(self) -> Vec<u8> {
    self.data
  }

  fn content_type(&self) -> Option<String> {
    self.content_type.clone()
  }
}

impl From<DeleteObjectOutput> for S3ResponseData {
  fn from(_: DeleteObjectOutput) -> Self {
    S3ResponseData {
      data: Vec::new(),
      content_type: None,
    }
  }
}

impl From<DeleteObjectsOutput> for S3ResponseData {
  fn from(_: DeleteObjectsOutput) -> Self {
    S3ResponseData {
      data: Vec::new(),
      content_type: None,
    }
  }
}

impl S3ResponseData {
  pub fn new_with_data(data: Vec<u8>, content_type: Option<String>) -> Self {
    S3ResponseData { data, content_type }
  }
}
