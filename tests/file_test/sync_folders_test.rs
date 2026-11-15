use client_api_test::generate_unique_registered_user_client;
use reqwest::header::CONTENT_LENGTH;
use reqwest::{Client, RequestBuilder, Response, StatusCode};
use serde_json::{json, Value};

#[tokio::test]
async fn sync_folder_files_round_trip() {
  let (client, _user) = generate_unique_registered_user_client().await;
  let token = client.access_token().unwrap();
  let base_url = format!("{}/api/sync-folders", client.base_url);
  let http = Client::new();

  let response = http.get(&base_url).send().await.unwrap();
  assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
  let folders = get_json(&http, &base_url, &token).await;
  assert_eq!(folders["data"], json!({ "folders": [] }));

  // Larger than one 8 MiB part, so it is stored with a multipart upload.
  let large: Vec<u8> = (0..9 * 1024 * 1024).map(|i| (i % 251) as u8).collect();
  let uploads = [
    ("afsync1_large", "false", large.clone()),
    ("afsync1_small", "false", b"hello".to_vec()),
    ("photo.png", "true", b"png".to_vec()),
  ];
  let mut stored = Vec::new();
  for (name, blobs, content) in uploads {
    let size = content.len();
    let url = format!("{base_url}/My%20Sync/files/{name}?blobs={blobs}");
    let response = send(http.put(url).body(content), &token).await;
    assert_eq!(response.status(), StatusCode::OK);
    let file: Value = response.json().await.unwrap();
    assert_eq!(file["data"]["name"], name);
    assert_eq!(file["data"]["size"], size);
    stored.push(file["data"].clone());
  }

  let folders = get_json(&http, &base_url, &token).await;
  let expected = json!([{ "name": "My Sync" }]);
  assert_eq!(folders["data"]["folders"], expected);
  let files = get_json(&http, &format!("{base_url}/My%20Sync/files"), &token).await;
  assert_eq!(files["data"]["files"], json!([stored[0], stored[1]]));
  let blobs_url = format!("{base_url}/My%20Sync/files?blobs=true");
  let blobs = get_json(&http, &blobs_url, &token).await;
  assert_eq!(blobs["data"]["files"], json!([stored[2]]));

  let url = format!("{base_url}/My%20Sync/files/afsync1_large");
  let response = send(http.get(url), &token).await;
  assert_eq!(response.status(), StatusCode::OK);
  let length = large.len().to_string();
  assert_eq!(response.headers()[CONTENT_LENGTH], length);
  assert_eq!(response.bytes().await.unwrap(), large);

  let url = format!("{base_url}/My%20Sync/files/afsync1_small");
  for _ in 0..2 {
    let response = send(http.delete(&url), &token).await;
    assert_eq!(response.status(), StatusCode::OK);
  }
  let response = send(http.get(url), &token).await;
  assert_eq!(response.status(), StatusCode::NOT_FOUND);

  let url = format!("{base_url}/My%20Sync/files/..%2Fsecret");
  let response = send(http.get(url), &token).await;
  assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

async fn send(request: RequestBuilder, token: &str) -> Response {
  request.bearer_auth(token).send().await.unwrap()
}

async fn get_json(http: &Client, url: &str, token: &str) -> Value {
  let response = send(http.get(url), token).await;
  assert_eq!(response.status(), StatusCode::OK);
  response.json().await.unwrap()
}
