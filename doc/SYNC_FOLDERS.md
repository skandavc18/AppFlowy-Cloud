# Sync Folders

The AppFlowy app can keep a workspace that is stored on the user's computer in step through a
plain folder of files, for example on Google Drive, OneDrive or Box. The `/api/sync-folders`
endpoints let a self-hosted AppFlowy Cloud act as such a folder: private, per-user file storage
where the app lists, uploads, downloads and deletes files in named folders.

## Storage

Files live in the S3/MinIO bucket configured by `APPFLOWY_S3_BUCKET`:

- `sync-folders/{user_uuid}/{folder}/{name}` for files
- `sync-folders/{user_uuid}/{folder}/blobs/{name}` for attached files (`?blobs=true`)

`user_uuid` is the GoTrue user id from the access token, so every user only sees their own
folders. Folders exist while they hold at least one file.

## Authentication

Every endpoint requires `Authorization: Bearer <GoTrue access token>`, the same token as the rest
of the AppFlowy Cloud API.

## Endpoints

JSON responses use the usual envelope, `{"code": 0, "message": "", "data": ...}`. Errors use the
same envelope with a non-zero `code` and a matching HTTP status.

| Method and path | Result |
| --- | --- |
| `GET /api/sync-folders` | `data: {"folders": [{"name": "AppFlowy"}]}`, sorted by name |
| `GET /api/sync-folders/{folder}/files?blobs=false` | `data: {"files": [{"name", "size", "modified_at"}]}` |
| `PUT /api/sync-folders/{folder}/files/{name}?blobs=false` | Stores the raw request body, replacing an existing file. `data: {"name", "size", "modified_at"}` |
| `GET /api/sync-folders/{folder}/files/{name}?blobs=false` | The raw file (`application/octet-stream`, with `Content-Length`) |
| `DELETE /api/sync-folders/{folder}/files/{name}?blobs=false` | Deletes the file. Deleting a missing file also succeeds |

- `blobs` is optional and defaults to `false`. With `blobs=true` the request works on the attached
  files of the folder instead.
- `size` is in bytes and `modified_at` is the last modification time in Unix epoch milliseconds.
- Listing a folder returns only its direct files; attached files are listed with `blobs=true`. A
  folder that does not exist lists as empty.
- Uploads must send `Content-Length`. Bodies larger than 8 MiB are streamed to storage as a
  multipart upload, so the server never holds a whole large file in memory.

## Names

Folder and file names are part of the URL path and must be percent-encoded by clients. They are
checked after decoding:

- folder: 1 to 100 ASCII letters, digits, spaces, `.`, `_` or `-`, not starting or ending with a
  space or `.`. Names are case-sensitive.
- file: 1 to 200 characters matching `^[A-Za-z0-9][A-Za-z0-9._-]*$`.

## Errors

| HTTP status | `code` | When |
| --- | --- | --- |
| 400 | 1008 (`InvalidRequest`) | Invalid folder or file name, invalid `blobs`, or a body shorter than its `Content-Length` |
| 401 | 1024 (`UserUnAuthorized`) | Missing or invalid access token |
| 404 | -2 (`RecordNotFound`) | Downloading a file that does not exist |
| 411 | 1008 (`InvalidRequest`) | Upload without `Content-Length` |
| 413 | 1016 (`PayloadTooLarge`) | Upload larger than `APPFLOWY_SYNC_FOLDER_MAX_FILE_SIZE`, or a body longer than its `Content-Length` |
| 5xx | non-zero | Storage failures |

Clients detect support by calling `GET /api/sync-folders`: older servers answer 404.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `APPFLOWY_SYNC_FOLDER_MAX_FILE_SIZE` | `2147483648` (2 GiB) | Largest file, in bytes, that can be uploaded |

## Reverse proxy

Files can be much larger than the 10 MB request limit nginx applies to the rest of the API, so
`/api/sync-folders` needs its own location that allows large bodies and streams them through.
`nginx/nginx.conf` and `external_proxy_config/nginx/appflowy.site.conf` include it:

```nginx
location /api/sync-folders {
    proxy_pass $appflowy_cloud_backend;
    proxy_set_header X-Request-Id $request_id;
    proxy_set_header Host $http_host;

    proxy_read_timeout 600s;
    proxy_connect_timeout 600s;
    proxy_send_timeout 600s;

    proxy_request_buffering off;
    proxy_buffering off;
    proxy_cache off;
    client_max_body_size 2G;
}
```

Keep `client_max_body_size` at least as large as `APPFLOWY_SYNC_FOLDER_MAX_FILE_SIZE`. Deployments
behind another reverse proxy need equivalent settings there.
