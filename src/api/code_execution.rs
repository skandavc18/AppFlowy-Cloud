use actix_web::web::{Data, Json};
use actix_web::{web, Scope};
use app_error::AppError;
use secrecy::ExposeSecret;
use serde::{Deserialize, Serialize};
use shared_entity::response::{AppResponse, JsonAppResponse};

use crate::biz::authentication::jwt::UserUuid;
use crate::state::AppState;

#[derive(Debug, Serialize)]
pub struct CodeExecutionCapabilities {
  pub enabled: bool,
  pub languages: Vec<String>,
  pub max_code_bytes: usize,
  pub max_timeout_ms: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExecuteCodeRequest {
  pub language: String,
  pub code: String,
  #[serde(default)]
  pub stdin: String,
  pub timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExecuteCodeResponse {
  #[serde(default)]
  pub stdout: String,
  #[serde(default)]
  pub stderr: String,
  pub exit_code: Option<i32>,
  pub duration_ms: u64,
}

pub fn code_execution_scope() -> Scope {
  web::scope("/api/code-execution")
    .service(web::resource("/capabilities").route(web::get().to(capabilities_handler)))
    .service(web::resource("/execute").route(web::post().to(execute_handler)))
}

async fn capabilities_handler(
  _user_uuid: UserUuid,
  state: Data<AppState>,
) -> actix_web::Result<JsonAppResponse<CodeExecutionCapabilities>> {
  let setting = &state.config.code_execution;
  let enabled = setting.enabled && setting.runner_url.is_some();
  Ok(
    AppResponse::Ok()
      .with_data(CodeExecutionCapabilities {
        enabled,
        languages: if enabled {
          setting.languages.clone()
        } else {
          Vec::new()
        },
        max_code_bytes: setting.max_code_bytes,
        max_timeout_ms: setting.max_timeout_ms,
      })
      .into(),
  )
}

async fn execute_handler(
  _user_uuid: UserUuid,
  state: Data<AppState>,
  payload: Json<ExecuteCodeRequest>,
) -> actix_web::Result<JsonAppResponse<ExecuteCodeResponse>> {
  let setting = &state.config.code_execution;
  if !setting.enabled {
    return Err(AppError::InvalidRequest("Code execution is disabled".into()).into());
  }
  let runner_url = setting
    .runner_url
    .as_ref()
    .ok_or_else(|| AppError::InvalidRequest("Code execution runner is not configured".into()))?;
  let mut request = payload.into_inner();
  request.language = request.language.trim().to_lowercase();
  if !setting
    .languages
    .iter()
    .any(|item| item == &request.language)
  {
    return Err(
      AppError::InvalidRequest(format!(
        "Unsupported execution language: {}",
        request.language
      ))
      .into(),
    );
  }
  if request.code.len() > setting.max_code_bytes {
    return Err(
      AppError::PayloadTooLarge(format!("Code exceeds {} bytes", setting.max_code_bytes)).into(),
    );
  }
  request.timeout_ms = Some(
    request
      .timeout_ms
      .unwrap_or(setting.max_timeout_ms)
      .clamp(100, setting.max_timeout_ms),
  );

  let client = reqwest::Client::new();
  let mut runner_request = client
    .post(format!("{}/execute", runner_url.trim_end_matches('/')))
    .json(&request);
  if let Some(token) = &setting.runner_token {
    runner_request = runner_request.bearer_auth(token.expose_secret());
  }
  let response = runner_request
    .send()
    .await
    .map_err(|_| AppError::Connect("Code execution runner is unavailable".into()))?;
  if !response.status().is_success() {
    return Err(
      AppError::Unhandled(format!(
        "Code execution runner failed with status {}",
        response.status()
      ))
      .into(),
    );
  }
  let result = response.json::<ExecuteCodeResponse>().await.map_err(|_| {
    AppError::Unhandled("Code execution runner returned an invalid response".into())
  })?;
  Ok(AppResponse::Ok().with_data(result).into())
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn execute_request_serializes_terminal_input() {
    let request = ExecuteCodeRequest {
      language: "javascript".into(),
      code: "console.log(readLine())".into(),
      stdin: "hello".into(),
      timeout_ms: Some(1000),
    };
    let value = serde_json::to_value(request).unwrap();
    assert_eq!(value["stdin"], "hello");
    assert_eq!(value["timeout_ms"], 1000);
  }
}
