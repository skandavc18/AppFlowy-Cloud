use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateOfficeSessionRequest {
  #[serde(default)]
  pub session_id: Option<String>,
  pub storage_url: String,
  pub file_name: String,
  pub editable: bool,
  pub user_name: String,
  pub is_dark: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OfficeSessionResponse {
  pub session_id: String,
  pub document_server_url: String,
  pub editor_config: Value,
}
