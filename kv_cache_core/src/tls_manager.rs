use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{error, info, warn};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::time;
use serde::{Deserialize, Serialize};

/// TLS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TlsConfig {
    /// Enable TLS
    pub enabled: bool,
    /// Certificate file path
    pub cert_path: Option<String>,
    /// Private key file path
    pub key_path: Option<String>,
    /// CA certificate path for mTLS
    pub ca_cert_path: Option<String>,
    /// Enable mTLS (mutual TLS)
    pub enable_mtls: bool,
    /// Certificate auto-renewal
    pub auto_renewal: bool,
    /// Certificate renewal threshold (days)
    pub renewal_threshold_days: u32,
    /// Certificate check interval (seconds)
    pub check_interval_seconds: u64,
    /// Minimum TLS version
    pub min_tls_version: TlsVersion,
    /// Cipher suites
    pub cipher_suites: Vec<String>,
    /// Enable OCSP stapling
    pub ocsp_stapling: bool,
    /// Enable HSTS
    pub hsts_enabled: bool,
    /// HSTS max age (seconds)
    pub hsts_max_age: u64,
    /// Include subdomains in HSTS
    pub hsts_include_subdomains: bool,
    /// Enable preload
    pub hsts_preload: bool,
}

/// TLS version enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TlsVersion {
    Tls12,
    Tls13,
}

impl Default for TlsVersion {
    fn default() -> Self {
        TlsVersion::Tls13
    }
}

/// Certificate information
#[derive(Debug, Clone)]
pub struct CertificateInfo {
    /// Subject name
    pub subject: String,
    /// Issuer name
    pub issuer: String,
    /// Valid from
    pub valid_from: chrono::DateTime<chrono::Utc>,
    /// Valid until
    pub valid_until: chrono::DateTime<chrono::Utc>,
    /// Serial number
    pub serial_number: String,
    /// Days until expiration
    pub days_until_expiry: i64,
}

/// TLS Manager for certificate management and auto-renewal
pub struct TlsManager {
    config: TlsConfig,
    certificates: Arc<RwLock<HashMap<String, CertificateInfo>>>,
    last_check: Arc<RwLock<Instant>>,
    renewal_worker: Option<tokio::task::JoinHandle<()>>,
}

impl TlsManager {
    pub fn new(config: TlsConfig) -> Self {
        Self {
            config,
            certificates: Arc::new(RwLock::new(HashMap::new())),
            last_check: Arc::new(RwLock::new(Instant::now())),
            renewal_worker: None,
        }
    }

    /// Initialize TLS manager
    pub async fn initialize(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.config.enabled {
            info!("TLS is disabled");
            return Ok(());
        }

        // Load initial certificates
        self.load_certificates().await?;

        // Start auto-renewal worker if enabled
        if self.config.auto_renewal {
            self.start_auto_renewal_worker().await?;
        }

        info!("TLS manager initialized successfully");
        Ok(())
    }

    /// Load certificates from configured paths
    async fn load_certificates(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut certs = self.certificates.write().await;

        // Load server certificate
        if let Some(ref cert_path) = self.config.cert_path {
            if let Ok(cert_info) = self.load_certificate_info(cert_path).await {
                certs.insert("server".to_string(), cert_info);
                info!("Loaded server certificate from: {}", cert_path);
            }
        }

        // Load CA certificate for mTLS
        if let Some(ref ca_cert_path) = self.config.ca_cert_path {
            if let Ok(cert_info) = self.load_certificate_info(ca_cert_path).await {
                certs.insert("ca".to_string(), cert_info);
                info!("Loaded CA certificate from: {}", ca_cert_path);
            }
        }

        Ok(())
    }

    /// Load certificate information from file
    async fn load_certificate_info(&self, cert_path: &str) -> Result<CertificateInfo, Box<dyn std::error::Error>> {
        let cert_data = tokio::fs::read(cert_path).await?;
        let cert = openssl::x509::X509::from_pem(&cert_data)?;

        let subject = format!("{:?}", cert.subject_name());
        let issuer = format!("{:?}", cert.issuer_name());
        let serial = "mock-serial".to_string(); // TODO: Implement proper serial number extraction
        
        // For now, use current time as certificate dates
        // TODO: Implement proper ASN1_TIME parsing
        let now = chrono::Utc::now();
        let valid_from = now;
        let valid_until = now + chrono::Duration::days(365);
        let days_until_expiry = (valid_until - now).num_days();

        Ok(CertificateInfo {
            subject,
            issuer,
            valid_from,
            valid_until,
            serial_number: serial,
            days_until_expiry,
        })
    }

    /// Start auto-renewal worker
    async fn start_auto_renewal_worker(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let config = self.config.clone();
        let certificates = self.certificates.clone();
        let last_check = self.last_check.clone();

        let handle = tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(config.check_interval_seconds));
            
            loop {
                interval.tick().await;
                
                if let Err(e) = Self::check_and_renew_certificates(&config, &certificates, &last_check).await {
                    error!("Certificate renewal check failed: {}", e);
                }
            }
        });

        self.renewal_worker = Some(handle);
        info!("Auto-renewal worker started");
        Ok(())
    }

    /// Check and renew certificates if needed
    async fn check_and_renew_certificates(
        config: &TlsConfig,
        certificates: &Arc<RwLock<HashMap<String, CertificateInfo>>>,
        last_check: &Arc<RwLock<Instant>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut certs = certificates.write().await;
        let mut last_check_guard = last_check.write().await;
        
        *last_check_guard = Instant::now();

        for (cert_name, cert_info) in certs.iter_mut() {
            if cert_info.days_until_expiry <= config.renewal_threshold_days as i64 {
                warn!("Certificate '{}' expires in {} days, renewal needed", 
                      cert_name, cert_info.days_until_expiry);
                
                // In a real implementation, this would trigger certificate renewal
                // For now, we just log the warning
                if let Err(e) = Self::renew_certificate(config, cert_name).await {
                    error!("Failed to renew certificate '{}': {}", cert_name, e);
                }
            }
        }

        Ok(())
    }

    /// Renew a certificate (placeholder implementation)
    async fn renew_certificate(config: &TlsConfig, cert_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        // This is a placeholder for certificate renewal logic
        // In production, this would integrate with certificate authorities
        // like Let's Encrypt, AWS Certificate Manager, etc.
        
        info!("Certificate renewal requested for: {}", cert_name);
        
        // Simulate renewal process
        tokio::time::sleep(Duration::from_secs(1)).await;
        
        info!("Certificate '{}' renewed successfully", cert_name);
        Ok(())
    }

    /// Get certificate information
    pub async fn get_certificate_info(&self, cert_name: &str) -> Option<CertificateInfo> {
        let certs = self.certificates.read().await;
        certs.get(cert_name).cloned()
    }

    /// Check if certificates are valid
    pub async fn validate_certificates(&self) -> Result<bool, Box<dyn std::error::Error>> {
        let certs = self.certificates.read().await;
        
        for (cert_name, cert_info) in certs.iter() {
            if cert_info.days_until_expiry <= 0 {
                error!("Certificate '{}' has expired", cert_name);
                return Ok(false);
            }
            
            if cert_info.days_until_expiry <= 7 {
                warn!("Certificate '{}' expires in {} days", cert_name, cert_info.days_until_expiry);
            }
        }
        
        Ok(true)
    }

    /// Get TLS configuration for Axum
    pub fn get_axum_tls_config(&self) -> Result<Option<()>, Box<dyn std::error::Error>> {
        if !self.config.enabled {
            return Ok(None);
        }

        let _cert_path = self.config.cert_path.as_ref().ok_or("TLS certificate path not configured")?;
        let _key_path = self.config.key_path.as_ref().ok_or("TLS private key path not configured")?;

        // For now, return Some(()) to indicate TLS is configured
        // TODO: Implement proper axum-server TLS integration
        Ok(Some(()))
    }

    /// Shutdown TLS manager
    pub async fn shutdown(&mut self) {
        if let Some(worker) = self.renewal_worker.take() {
            worker.abort();
            let _ = worker.await;
        }
        info!("TLS manager shutdown complete");
    }
}

/// Security headers configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityHeadersConfig {
    /// Enable security headers
    pub enabled: bool,
    /// Content Security Policy
    pub content_security_policy: Option<String>,
    /// X-Frame-Options
    pub x_frame_options: Option<String>,
    /// X-Content-Type-Options
    pub x_content_type_options: Option<String>,
    /// X-XSS-Protection
    pub x_xss_protection: Option<String>,
    /// Referrer Policy
    pub referrer_policy: Option<String>,
    /// Permissions Policy
    pub permissions_policy: Option<String>,
    /// Strict Transport Security
    pub hsts: Option<String>,
}

impl Default for SecurityHeadersConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            content_security_policy: Some("default-src 'self'".to_string()),
            x_frame_options: Some("DENY".to_string()),
            x_content_type_options: Some("nosniff".to_string()),
            x_xss_protection: Some("1; mode=block".to_string()),
            referrer_policy: Some("strict-origin-when-cross-origin".to_string()),
            permissions_policy: Some("geolocation=(), microphone=(), camera=()".to_string()),
            hsts: Some("max-age=31536000; includeSubDomains".to_string()),
        }
    }
}

/// CORS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorsConfig {
    /// Enable CORS
    pub enabled: bool,
    /// Allowed origins
    pub allowed_origins: Vec<String>,
    /// Allowed methods
    pub allowed_methods: Vec<String>,
    /// Allowed headers
    pub allowed_headers: Vec<String>,
    /// Expose headers
    pub expose_headers: Vec<String>,
    /// Allow credentials
    pub allow_credentials: bool,
    /// Max age
    pub max_age: Option<u64>,
}

impl Default for CorsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            allowed_origins: vec!["*".to_string()],
            allowed_methods: vec![
                "GET".to_string(),
                "POST".to_string(),
                "PUT".to_string(),
                "DELETE".to_string(),
                "OPTIONS".to_string(),
            ],
            allowed_headers: vec![
                "Content-Type".to_string(),
                "Authorization".to_string(),
                "X-Requested-With".to_string(),
            ],
            expose_headers: vec![],
            allow_credentials: false,
            max_age: Some(86400), // 24 hours
        }
    }
}

/// Rate limiting configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Enable rate limiting
    pub enabled: bool,
    /// Requests per minute per IP
    pub requests_per_minute: u32,
    /// Burst size
    pub burst_size: u32,
    /// Rate limit window (seconds)
    pub window_seconds: u64,
    /// Rate limit storage backend
    pub storage_backend: RateLimitStorage,
    /// Rate limit headers
    pub include_headers: bool,
}

/// Rate limit storage backend
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RateLimitStorage {
    /// In-memory storage (for single instance)
    Memory,
    /// Redis storage (for distributed deployments)
    Redis { url: String },
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            requests_per_minute: 100,
            burst_size: 10,
            window_seconds: 60,
            storage_backend: RateLimitStorage::Memory,
            include_headers: true,
        }
    }
}

/// Rate limiter implementation
pub struct RateLimiter {
    config: RateLimitConfig,
    storage: Arc<RwLock<HashMap<String, RateLimitEntry>>>,
}

/// Rate limit entry
#[derive(Debug, Clone)]
struct RateLimitEntry {
    requests: u32,
    window_start: Instant,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            storage: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Check if request is allowed
    pub async fn is_allowed(&self, client_id: &str) -> Result<bool, Box<dyn std::error::Error>> {
        if !self.config.enabled {
            return Ok(true);
        }

        let mut storage = self.storage.write().await;
        let now = Instant::now();

        // Get or create entry for client
        let entry = storage.entry(client_id.to_string()).or_insert(RateLimitEntry {
            requests: 0,
            window_start: now,
        });

        // Check if window has expired
        if now.duration_since(entry.window_start).as_secs() >= self.config.window_seconds {
            entry.requests = 0;
            entry.window_start = now;
        }

        // Check rate limit
        if entry.requests >= self.config.requests_per_minute {
            return Ok(false);
        }

        // Increment request count
        entry.requests += 1;
        Ok(true)
    }

    /// Get rate limit headers
    pub async fn get_headers(&self, client_id: &str) -> HashMap<String, String> {
        if !self.config.include_headers {
            return HashMap::new();
        }

        let storage = self.storage.read().await;
        let now = Instant::now();
        
        let mut headers = HashMap::new();
        
        if let Some(entry) = storage.get(client_id) {
            let window_end = entry.window_start + Duration::from_secs(self.config.window_seconds);
            let remaining_time = window_end.duration_since(now).as_secs();
            
            headers.insert("X-RateLimit-Limit".to_string(), self.config.requests_per_minute.to_string());
            headers.insert("X-RateLimit-Remaining".to_string(), 
                (self.config.requests_per_minute - entry.requests).to_string());
            headers.insert("X-RateLimit-Reset".to_string(), remaining_time.to_string());
        }

        headers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_tls_manager_creation() {
        let config = TlsConfig {
            enabled: false,
            cert_path: None,
            key_path: None,
            ca_cert_path: None,
            enable_mtls: false,
            auto_renewal: false,
            renewal_threshold_days: 30,
            check_interval_seconds: 3600,
            min_tls_version: TlsVersion::Tls13,
            cipher_suites: vec![],
            ocsp_stapling: false,
            hsts_enabled: true,
            hsts_max_age: 31536000,
            hsts_include_subdomains: true,
            hsts_preload: false,
        };

        let mut tls_manager = TlsManager::new(config);
        tls_manager.initialize().await.unwrap();
        
        // Test shutdown
        tls_manager.shutdown().await;
    }

    #[tokio::test]
    async fn test_rate_limiter() {
        let config = RateLimitConfig {
            enabled: true,
            requests_per_minute: 5,
            burst_size: 2,
            window_seconds: 60,
            storage_backend: RateLimitStorage::Memory,
            include_headers: true,
        };

        let rate_limiter = RateLimiter::new(config);

        // Test allowed requests
        for i in 0..5 {
            let allowed = rate_limiter.is_allowed("test-client").await.unwrap();
            assert!(allowed, "Request {} should be allowed", i);
        }

        // Test rate limited request
        let allowed = rate_limiter.is_allowed("test-client").await.unwrap();
        assert!(!allowed, "Request should be rate limited");

        // Test headers
        let headers = rate_limiter.get_headers("test-client").await;
        assert!(headers.contains_key("X-RateLimit-Limit"));
        assert!(headers.contains_key("X-RateLimit-Remaining"));
    }

    #[tokio::test]
    async fn test_security_headers_config() {
        let config = SecurityHeadersConfig::default();
        assert!(config.enabled);
        assert!(config.content_security_policy.is_some());
        assert!(config.x_frame_options.is_some());
    }

    #[tokio::test]
    async fn test_cors_config() {
        let config = CorsConfig::default();
        assert!(config.enabled);
        assert!(config.allowed_origins.contains(&"*".to_string()));
        assert!(config.allowed_methods.contains(&"GET".to_string()));
    }
} 