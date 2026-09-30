use reqwest::Method;
use shared_entity::dto::office_dto::{CreateOfficeSessionRequest, OfficeSessionResponse};
use shared_entity::response::AppResponseError;
use uuid::Uuid;

use crate::{process_response_data, Client};

impl Client {
  pub async fn create_office_session(
    &self,
    workspace_id: &Uuid,
    request: CreateOfficeSessionRequest,
  ) -> Result<OfficeSessionResponse, AppResponseError> {
    let url = format!("{}/api/office/{workspace_id}/session", self.base_url);
    let response = self
      .http_client_with_auth(Method::POST, &url)
      .await?
      .json(&request)
      .send()
      .await?;
    process_response_data::<OfficeSessionResponse>(response).await
  }
}
