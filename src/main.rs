use axum::{
    Router,
    http::{
        StatusCode,
        header::{AUTHORIZATION, USER_AGENT}
    },
    extract::{
        Multipart, State,
        multipart::MultipartError
    },
    response::{IntoResponse, Json, Response},
    routing::post
};
use glc::server::{setup_logging, serve, SpanMaker};
use http::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{
    Client,
    header::ACCEPT
};
use serde::{Serialize, Deserialize};
use mime::TEXT_PLAIN;
use s3::{
    bucket::Bucket,
    error::S3Error,
    creds::{
        Credentials,
        error::CredentialsError
    },
    region::Region
};
use sha1::{Digest, Sha1};
use std::{
    fs,
    future::Future,
    io,
    net::IpAddr,
    sync::Arc,
    time::Duration
};
use tokio::io::AsyncRead;
use tower::ServiceBuilder;
use tower_http::{
    compression::CompressionLayer,
    timeout::TimeoutLayer,
    trace::{DefaultOnFailure, DefaultOnResponse, TraceLayer}
};
use tracing::{error, info, Level, warn};

pub struct AppState {
    client: Client,
    uploader: BucketUploader,
    api_url: String,
    api_token: String,
    log_url: String,
    bucket_base_dir: String,
    max_log_size: usize
}

#[derive(Debug, thiserror::Error)]
pub enum UploadError {
    #[error("Bucket error: {0}")]
    S3Error(#[from] S3Error),
}

pub trait Uploader {
    fn upload<R>(
        &self,
        filename: &str,
        reader: R
    ) -> impl Future<Output = Result<String, UploadError>> + Send
    where
        R: AsyncRead + Unpin + Send;

    fn upload_with_content_type<R>(
        &self,
        filename: &str,
        reader: R,
        content_type: &str
    ) -> impl Future<Output = Result<String, UploadError>> + Send
    where
        R: AsyncRead + Unpin + Send;
}

#[derive(Debug, thiserror::Error)]
pub enum BucketUploaderError {
    #[error("Bucket error: {0}")]
    S3Error(#[from] S3Error),
    #[error("Credentials error: {0}")]
    CredentialsError(#[from] CredentialsError)
}

pub struct BucketUploader {
    bucket: Bucket,
    base_url: String,
    base_dir: String
}

impl BucketUploader {
    pub fn new(
        name: &str,
        region: &str,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        base_url: &str,
        base_dir: &str
    ) -> Result<BucketUploader, BucketUploaderError> {
        let bucket = Bucket::new(
            name,
            Region::Custom {
                region: region.into(),
                endpoint: endpoint.into()
            },
            Credentials::new(
                Some(access_key),
                Some(secret_key),
                None,
                None,
                None
            )?
        )?
        .with_path_style()
        .with_extra_headers(
            HeaderMap::from_iter([(
                HeaderName::from_static("x-amz-acl"),
                HeaderValue::from_static("public-read")
            )])
        )?;

        Ok(
            BucketUploader {
                bucket,
                base_url: base_url.into(),
                base_dir: base_dir.into()
            }
        )
    }
}

impl Uploader for BucketUploader {
    async fn upload<R>(
        &self,
        filename: &str,
        mut reader: R
    ) -> Result<String, UploadError>
    where
        R: AsyncRead + Unpin + Send
    {
        let path = format!("{0}/{filename}", self.base_dir);
        let resp = self.bucket.put_object_stream(&mut reader, &path).await?;
        let url = format!("{0}/{path}", self.base_url);
        info!(
            "{} bytes uploaded to {url}, {}",
            resp.uploaded_bytes(),
            resp.status_code()
        );
        Ok(url)
    }

    async fn upload_with_content_type<R>(
        &self,
        filename: &str,
        mut reader: R,
        content_type: &str
    ) -> Result<String, UploadError>
    where
        R: AsyncRead + Unpin + Send
    {
        let path = format!("{0}/{filename}", self.base_dir);
        let resp = self.bucket.put_object_stream_with_content_type(
            &mut reader,
            &path,
            content_type
        ).await?;
        let url = format!("{0}/{path}", self.base_url);
        info!(
            "{} bytes uploaded to {url}, {}",
            resp.uploaded_bytes(),
            resp.status_code()
        );
        Ok(url)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AppError {
    #[error("{0}")]
    MultipartError(#[from] MultipartError),
    #[error("Missing field {0}")]
    MissingField(&'static str),
    #[error("{0}")]
    RequestError(#[from] reqwest::Error),
    #[error("Payload too large")]
    TooLarge,
    #[error("{0}")]
    UploadError(#[from] UploadError) 
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct HttpError {
    error: String
}

impl From<AppError> for HttpError {
    fn from(err: AppError) -> Self {
        HttpError { error: format!("{}", err) }
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
       let code = StatusCode::from(&self);

        // Log errors
        if code.is_server_error() {
            error!("{}", self);
        }
        else {
            warn!("{}", self);
        }

        let body = Json(HttpError::from(self));
        (code, body).into_response()
    }
}

impl From<&AppError> for StatusCode {
    fn from(err: &AppError) -> Self {
        match err {
            AppError::MissingField(_) => StatusCode::BAD_REQUEST,
            AppError::MultipartError(_) => StatusCode::BAD_REQUEST,
            AppError::RequestError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            AppError::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            AppError::UploadError(_) => StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct ReportReceipt {
    url: String
}

#[derive(Debug, Serialize)]
struct ReportPost {
    title: String,
    labels: Vec<String>,
    body: String
}

async fn post_report(
    State(state): State<Arc<AppState>>,
    mut form: Multipart
) -> Result<Json<ReportReceipt>, AppError>
{
    let mut version = None;
    let mut email = None;
    let mut summary = None;
    let mut desc = None;
    let mut log = None;

    while let Some(field) = form.next_field().await? {
        match field.name() {
            Some("version") => version = Some(field.text().await?),
            Some("email") => email = Some(field.text().await?),
            Some("summary") => summary = Some(field.text().await?),
            Some("description") => desc = Some(field.text().await?),
            Some("log") => log = Some(field.bytes().await?),
            _ => {}
        }
    }

    let version = version.ok_or(AppError::MissingField("version"))?;
    let email = email.ok_or(AppError::MissingField("email"))?;
    let summary = summary.ok_or(AppError::MissingField("summary"))?;
    let desc = desc.ok_or(AppError::MissingField("description"))?;
    let log = log.ok_or(AppError::MissingField("log"))?;

    let log_size = log.len();

    if log_size > state.max_log_size {
        return Err(AppError::TooLarge);
    }

    let mut sha1_hasher = Sha1::new();
    sha1_hasher.update(&log[..]);

    let log_sha1 = format!("{:x}", sha1_hasher.finalize());

    // upload log
    state.uploader.upload_with_content_type(
        &log_sha1,
        &log[..],
        TEXT_PLAIN.as_ref()
    ).await?; 

    // submit report
    let log_url = format!("{}/{log_sha1}", state.log_url);

    let body = format!("| ABR | |\n|-----|-----|\n| Version | {version} |\n| Reporter | {email} |\n| Error Log | [{log_size} bytes]({log_url}) |\n\n<pre>{desc}</pre>");

    let auth = format!("token {}", state.api_token);

    let data = ReportPost {
        title: summary,
        labels: vec![ "ABR".into() ],
        body
    };

    Ok(
        Json(
            state.client
                .post(&state.api_url)
                .json(&data)
                .header(AUTHORIZATION, &auth)
                .header(ACCEPT, "application/vnd.github.v3+json")
                .header(USER_AGENT, "bug-reporter")
                .send()
                .await?
                .error_for_status()?
                .json::<ReportReceipt>()
                .await?
        )
    )
}

fn routes(base_path: &str, log_headers: bool) -> Router<Arc<AppState>> {
    Router::new()
        .route(
            if base_path.is_empty() { "/" } else { base_path },
            post(post_report)
        )
        .layer(
            ServiceBuilder::new()
                .layer(CompressionLayer::new())
                 // ensure requests don't block shutdown
                .layer(TimeoutLayer::with_status_code(
                    StatusCode::REQUEST_TIMEOUT,
                    Duration::from_secs(10)
                ))
        )
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(SpanMaker::new().include_headers(log_headers))
                .on_response(DefaultOnResponse::new().level(Level::INFO))
                .on_failure(DefaultOnFailure::new().level(Level::WARN))
        )
}

#[derive(Debug, Deserialize)]
pub struct Config {
    pub base_path: String,
    pub listen_ip: String,
    pub listen_port: u16,
    pub log_headers: bool,
    pub api_url: String,
    pub api_token: String,
    pub log_url: String,
    pub max_log_size: usize,

    pub bucket_name: String,
    pub bucket_region: String,
    pub bucket_endpoint: String,
    pub bucket_access_key: String,
    pub bucket_secret_key: String,
    pub bucket_base_url: String,
    pub bucket_base_dir: String
}

#[derive(Debug, thiserror::Error)]
enum StartupError {
    #[error("{0}")]
    AddrParse(#[from] std::net::AddrParseError),
    #[error("{0}")]
    TomlParse(#[from] toml::de::Error),
    #[error("{0}")]
    Io(#[from] io::Error),
    #[error("{0}")]
    Client(#[from] reqwest::Error),
    #[error("{0}")]
    BucketUploader(#[from] BucketUploaderError)
}

async fn run() -> Result<(), StartupError> {
    info!("Reading config.toml");
    let config: Config = toml::from_str(&fs::read_to_string("config.toml")?)?;

    let client = Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let uploader = BucketUploader::new(
        &config.bucket_name,
        &config.bucket_region,
        &config.bucket_endpoint,
        &config.bucket_access_key,
        &config.bucket_secret_key,
        &config.bucket_base_url,
        &config.bucket_base_dir
    )?;

    let state = Arc::new(AppState {
        client,
        uploader,
        api_url: config.api_url,
        api_token: config.api_token,
        log_url: config.log_url,
        bucket_base_dir: config.bucket_base_dir,
        max_log_size: config.max_log_size << 20 // MB to bytes
    });

    // set up router
    let app = routes(
        &config.base_path,
        config.log_headers
    )
    .with_state(state);

    // serve pages
    let ip: IpAddr = config.listen_ip.parse()?;
    serve(app, ip, config.listen_port).await?;

    Ok(())
}

#[tokio::main]
async fn main() {
    // set up logging
    let _guard = setup_logging(env!("CARGO_CRATE_NAME"), "", "abr.log");

    info!("Starting");

    if let Err(e) = run().await {
        error!("{}", e);
    }

    info!("Exiting");
}

#[cfg(test)]
mod test {
}
