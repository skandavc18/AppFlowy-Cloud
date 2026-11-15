use serde::{Deserialize, Serialize};

/// Response of `GET /api/sync-folders`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncFolderList {
  pub folders: Vec<SyncFolder>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncFolder {
  pub name: String,
}

/// Response of `GET /api/sync-folders/{folder}/files`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncFolderFileList {
  pub files: Vec<SyncFolderFile>,
}

/// A file in a sync folder, also returned by `PUT /api/sync-folders/{folder}/files/{name}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncFolderFile {
  pub name: String,
  /// Size in bytes.
  pub size: i64,
  /// Last modification time in Unix epoch milliseconds.
  pub modified_at: i64,
}
