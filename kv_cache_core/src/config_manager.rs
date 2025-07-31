use crate::config::CacheConfig;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, watch};
use tokio::time;
use tracing::{error, info, warn};

/// Configuration source types
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigSource {
    File(String),
    Environment,
    Default,
    HotReload(String),
}

/// Configuration validation result
#[derive(Debug, Clone)]
pub struct ConfigValidationResult {
    pub is_valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

/// Secrets management for sensitive configuration
#[derive(Debug, Clone)]
pub struct SecretsManager {
    secrets: Arc<RwLock<HashMap<String, String>>>,
    encryption_key: Option<String>,
}

impl SecretsManager {
    pub fn new() -> Self {
        Self {
            secrets: Arc::new(RwLock::new(HashMap::new())),
            encryption_key: None,
        }
    }

    /// Set encryption key for sensitive data
    pub fn set_encryption_key(&mut self, key: String) {
        self.encryption_key = Some(key);
    }

    /// Store a secret
    pub async fn store_secret(&self, key: &str, value: &str) -> Result<(), Box<dyn std::error::Error>> {
        let encrypted_value = if let Some(ref enc_key) = self.encryption_key {
            self.encrypt_value(value, enc_key)?
        } else {
            value.to_string()
        };

        let mut secrets = self.secrets.write().await;
        secrets.insert(key.to_string(), encrypted_value);
        Ok(())
    }

    /// Retrieve a secret
    pub async fn get_secret(&self, key: &str) -> Option<String> {
        let secrets = self.secrets.read().await;
        if let Some(encrypted_value) = secrets.get(key) {
            if let Some(ref enc_key) = self.encryption_key {
                match self.decrypt_value(encrypted_value, enc_key) {
                    Ok(decrypted) => Some(decrypted),
                    Err(_) => None,
                }
            } else {
                Some(encrypted_value.clone())
            }
        } else {
            None
        }
    }

    /// Simple encryption (in production, use proper encryption libraries)
    fn encrypt_value(&self, value: &str, key: &str) -> Result<String, Box<dyn std::error::Error>> {
        // Simple XOR encryption for demo purposes
        // In production, use proper encryption like AES
        let mut encrypted = Vec::new();
        let key_bytes = key.as_bytes();
        
        for (i, byte) in value.bytes().enumerate() {
            let key_byte = key_bytes[i % key_bytes.len()];
            encrypted.push(byte ^ key_byte);
        }
        
        Ok(base64::encode(encrypted))
    }

    /// Simple decryption
    fn decrypt_value(&self, encrypted_value: &str, key: &str) -> Result<String, Box<dyn std::error::Error>> {
        let encrypted_bytes = base64::decode(encrypted_value)?;
        let key_bytes = key.as_bytes();
        let mut decrypted = Vec::new();
        
        for (i, &byte) in encrypted_bytes.iter().enumerate() {
            let key_byte = key_bytes[i % key_bytes.len()];
            decrypted.push(byte ^ key_byte);
        }
        
        String::from_utf8(decrypted).map_err(|e| e.into())
    }

    /// Load secrets from environment variables
    pub async fn load_from_env(&self, prefix: &str) -> Result<(), Box<dyn std::error::Error>> {
        for (key, value) in env::vars() {
            if key.starts_with(prefix) {
                let secret_key = key.trim_start_matches(prefix).to_lowercase();
                self.store_secret(&secret_key, &value).await?;
            }
        }
        Ok(())
    }

    /// Load secrets from file
    pub async fn load_from_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let secrets: HashMap<String, String> = serde_json::from_str(&content)?;
        
        for (key, value) in secrets {
            self.store_secret(&key, &value).await?;
        }
        Ok(())
    }
}

/// Configuration manager with hot-reloading support
pub struct ConfigManager {
    config: Arc<RwLock<CacheConfig>>,
    secrets_manager: SecretsManager,
    config_file_path: Option<String>,
    hot_reload_sender: watch::Sender<CacheConfig>,
    hot_reload_receiver: watch::Receiver<CacheConfig>,
    validation_rules: Vec<Box<dyn ConfigValidator + Send + Sync>>,
    last_modified: Arc<RwLock<Option<Instant>>>,
}

impl ConfigManager {
    pub fn new() -> Self {
        let default_config = CacheConfig::default();
        let (sender, receiver) = watch::channel(default_config.clone());
        
        Self {
            config: Arc::new(RwLock::new(default_config)),
            secrets_manager: SecretsManager::new(),
            config_file_path: None,
            hot_reload_sender: sender,
            hot_reload_receiver: receiver,
            validation_rules: Vec::new(),
            last_modified: Arc::new(RwLock::new(None)),
        }
    }

    /// Load configuration from file
    pub async fn load_from_file(&mut self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.config_file_path = Some(path.to_string());
        
        if Path::new(path).exists() {
            let content = fs::read_to_string(path)?;
            let config: CacheConfig = serde_yaml::from_str(&content)?;
            
            // Validate configuration
            let validation = self.validate_config(&config).await;
            if !validation.is_valid {
                return Err(format!("Configuration validation failed: {:?}", validation.errors).into());
            }
            
            // Update configuration
            {
                let mut config_guard = self.config.write().await;
                *config_guard = config.clone();
            }
            
            // Notify hot-reload subscribers
            let _ = self.hot_reload_sender.send(config);
            
            // Update last modified time
            {
                let mut last_modified = self.last_modified.write().await;
                *last_modified = Some(Instant::now());
            }
            
            info!("Configuration loaded from file: {}", path);
        } else {
            warn!("Configuration file not found: {}", path);
        }
        
        Ok(())
    }

    /// Load configuration from environment variables
    pub async fn load_from_env(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut config = self.config.read().await.clone();
        
        // Server configuration
        if let Ok(port) = env::var("IRONKV_SERVER_PORT") {
            config.server.port = port.parse()?;
        }
        if let Ok(bind_address) = env::var("IRONKV_SERVER_BIND_ADDRESS") {
            config.server.bind_address = bind_address;
        }
        
        // Cluster configuration
        if let Ok(members) = env::var("IRONKV_CLUSTER_MEMBERS") {
            let member_list: Vec<String> = members.split(',').map(|s| s.trim().to_string()).collect();
            for member in member_list {
                if let Some((id, addr)) = member.split_once(':') {
                    config.cluster.members.insert(id.to_string(), addr.to_string());
                }
            }
        }
        
        // Security configuration
        if let Ok(enable_auth) = env::var("IRONKV_SECURITY_ENABLE_AUTH") {
            config.security.enable_auth = enable_auth.parse()?;
        }
        if let Ok(enable_authz) = env::var("IRONKV_SECURITY_ENABLE_AUTHZ") {
            config.security.enable_authz = enable_authz.parse()?;
        }
        
        // Performance configuration
        if let Ok(max_connections) = env::var("IRONKV_PERFORMANCE_MAX_CONNECTIONS") {
            config.performance.max_connections = max_connections.parse()?;
        }
        if let Ok(enable_batching) = env::var("IRONKV_PERFORMANCE_ENABLE_BATCHING") {
            config.performance.enable_batching = enable_batching.parse()?;
        }
        
        // Validate configuration
        let validation = self.validate_config(&config).await;
        if !validation.is_valid {
            return Err(format!("Configuration validation failed: {:?}", validation.errors).into());
        }
        
        // Update configuration
        {
            let mut config_guard = self.config.write().await;
            *config_guard = config.clone();
        }
        
        // Notify hot-reload subscribers
        let _ = self.hot_reload_sender.send(config);
        
        info!("Configuration loaded from environment variables");
        Ok(())
    }

    /// Get current configuration
    pub async fn get_config(&self) -> CacheConfig {
        self.config.read().await.clone()
    }

    /// Get configuration watcher for hot-reloading
    pub fn watch_config(&self) -> watch::Receiver<CacheConfig> {
        self.hot_reload_receiver.clone()
    }

    /// Update configuration
    pub async fn update_config(&mut self, new_config: CacheConfig) -> Result<(), Box<dyn std::error::Error>> {
        // Validate configuration
        let validation = self.validate_config(&new_config).await;
        if !validation.is_valid {
            return Err(format!("Configuration validation failed: {:?}", validation.errors).into());
        }
        
        // Update configuration
        {
            let mut config_guard = self.config.write().await;
            *config_guard = new_config.clone();
        }
        
        // Save to file if path is set
        if let Some(ref path) = self.config_file_path {
            let yaml = serde_yaml::to_string(&new_config)?;
            fs::write(path, yaml)?;
        }
        
        // Notify hot-reload subscribers
        let _ = self.hot_reload_sender.send(new_config);
        
        // Update last modified time
        {
            let mut last_modified = self.last_modified.write().await;
            *last_modified = Some(Instant::now());
        }
        
        info!("Configuration updated successfully");
        Ok(())
    }

    /// Start hot-reload monitoring
    pub async fn start_hot_reload_monitoring(&self) {
        if let Some(ref path) = self.config_file_path {
            let path = path.clone();
            let config_manager = self.clone_for_monitoring();
            
            tokio::spawn(async move {
                let mut interval = time::interval(Duration::from_secs(5));
                
                loop {
                    interval.tick().await;
                    
                    if let Ok(metadata) = fs::metadata(&path) {
                        if let Ok(modified) = metadata.modified() {
                            let modified_instant = Instant::now() - Duration::from_secs(
                                std::time::SystemTime::now()
                                    .duration_since(modified)
                                    .unwrap_or_default()
                                    .as_secs()
                            );
                            
                            let last_modified = config_manager.last_modified.read().await;
                            if last_modified.is_none() || last_modified.unwrap() < modified_instant {
                                if let Err(e) = config_manager.reload_config_file(&path).await {
                                    error!("Failed to reload configuration: {}", e);
                                }
                            }
                        }
                    }
                }
            });
        }
    }

    /// Clone for monitoring (without hot-reload sender)
    fn clone_for_monitoring(&self) -> ConfigManagerMonitor {
        ConfigManagerMonitor {
            config: self.config.clone(),
            last_modified: self.last_modified.clone(),
        }
    }

    /// Reload configuration file
    async fn reload_config_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let config: CacheConfig = serde_yaml::from_str(&content)?;
        
        // Validate configuration
        let validation = self.validate_config(&config).await;
        if !validation.is_valid {
            return Err(format!("Configuration validation failed: {:?}", validation.errors).into());
        }
        
        // Update configuration
        {
            let mut config_guard = self.config.write().await;
            *config_guard = config;
        }
        
        // Update last modified time
        {
            let mut last_modified = self.last_modified.write().await;
            *last_modified = Some(Instant::now());
        }
        
        info!("Configuration hot-reloaded from file: {}", path);
        Ok(())
    }

    /// Add validation rule
    pub fn add_validator(&mut self, validator: Box<dyn ConfigValidator + Send + Sync>) {
        self.validation_rules.push(validator);
    }

    /// Validate configuration
    pub async fn validate_config(&self, config: &CacheConfig) -> ConfigValidationResult {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        
        // Run all validation rules
        for validator in &self.validation_rules {
            let result = validator.validate(config).await;
            errors.extend(result.errors);
            warnings.extend(result.warnings);
        }
        
        ConfigValidationResult {
            is_valid: errors.is_empty(),
            errors,
            warnings,
        }
    }

    /// Get secrets manager
    pub fn secrets_manager(&self) -> &SecretsManager {
        &self.secrets_manager
    }

    /// Get configuration source information
    pub async fn get_config_source(&self) -> ConfigSource {
        if self.config_file_path.is_some() {
            ConfigSource::File(self.config_file_path.clone().unwrap())
        } else {
            ConfigSource::Environment
        }
    }
}

/// Configuration manager for monitoring (without hot-reload sender)
struct ConfigManagerMonitor {
    config: Arc<RwLock<CacheConfig>>,
    last_modified: Arc<RwLock<Option<Instant>>>,
}

impl ConfigManagerMonitor {
    /// Reload configuration file
    async fn reload_config_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let content = fs::read_to_string(path)?;
        let config: CacheConfig = serde_yaml::from_str(&content)?;
        
        // Update configuration (validation will be done by the main config manager)
        {
            let mut config_guard = self.config.write().await;
            *config_guard = config;
        }
        
        // Update last modified time
        {
            let mut last_modified = self.last_modified.write().await;
            *last_modified = Some(Instant::now());
        }
        
        info!("Configuration hot-reloaded from file: {}", path);
        Ok(())
    }
}

/// Trait for configuration validators
#[async_trait::async_trait]
pub trait ConfigValidator {
    async fn validate(&self, config: &CacheConfig) -> ConfigValidationResult;
}

/// Default configuration validator
pub struct DefaultConfigValidator;

#[async_trait::async_trait]
impl ConfigValidator for DefaultConfigValidator {
    async fn validate(&self, config: &CacheConfig) -> ConfigValidationResult {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        
        // Server validation
        if config.server.port == 0 {
            errors.push("Server port cannot be 0".to_string());
        }
        if config.server.port > 65535 {
            errors.push("Server port must be between 1 and 65535".to_string());
        }
        
        // Cluster validation
        if config.cluster.members.is_empty() {
            warnings.push("No cluster members configured".to_string());
        }
        
        // Performance validation
        if config.performance.max_connections == 0 {
            errors.push("Max connections must be greater than 0".to_string());
        }
        if config.performance.batch_min_size > config.performance.batch_max_size {
            errors.push("Batch min size cannot be greater than batch max size".to_string());
        }
        
        // Security validation
        if config.security.enable_authz && !config.security.enable_auth {
            warnings.push("Authorization enabled without authentication".to_string());
        }
        
        ConfigValidationResult {
            is_valid: errors.is_empty(),
            errors,
            warnings,
        }
    }
}

/// Production configuration validator
pub struct ProductionConfigValidator;

#[async_trait::async_trait]
impl ConfigValidator for ProductionConfigValidator {
    async fn validate(&self, config: &CacheConfig) -> ConfigValidationResult {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();
        
        // Production-specific validations
        if config.server.bind_address == "0.0.0.0" {
            warnings.push("Binding to 0.0.0.0 is not recommended for production".to_string());
        }
        
        if config.performance.max_connections < 100 {
            warnings.push("Low max connections for production workload".to_string());
        }
        
        if !config.security.enable_auth {
            warnings.push("Authentication disabled in production".to_string());
        }
        
        if config.cluster.members.len() < 3 {
            warnings.push("Cluster size less than 3 nodes may not provide adequate fault tolerance".to_string());
        }
        
        ConfigValidationResult {
            is_valid: errors.is_empty(),
            errors,
            warnings,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_config_manager_creation() {
        let config_manager = ConfigManager::new();
        let config = config_manager.get_config().await;
        assert_eq!(config.server.port, 6379);
    }

    #[tokio::test]
    async fn test_config_validation() {
        let mut config_manager = ConfigManager::new();
        config_manager.add_validator(Box::new(DefaultConfigValidator));
        
        let config = config_manager.get_config().await;
        let validation = config_manager.validate_config(&config).await;
        
        assert!(validation.is_valid);
    }

    #[tokio::test]
    async fn test_config_from_file() {
        let mut config_manager = ConfigManager::new();
        
        // Create temporary config file
        let temp_file = NamedTempFile::new().unwrap();
        let config_content = r#"
server:
  port: 8080
  bind_address: "127.0.0.1"
"#;
        fs::write(&temp_file, config_content).unwrap();
        
        config_manager.load_from_file(temp_file.path().to_str().unwrap()).await.unwrap();
        let config = config_manager.get_config().await;
        
        assert_eq!(config.server.port, 8080);
        assert_eq!(config.server.bind_address, "127.0.0.1");
    }

    #[tokio::test]
    async fn test_secrets_manager() {
        let mut secrets_manager = SecretsManager::new();
        secrets_manager.set_encryption_key("test-key".to_string());
        
        secrets_manager.store_secret("db_password", "secret123").await.unwrap();
        let password = secrets_manager.get_secret("db_password").await;
        
        assert_eq!(password, Some("secret123".to_string()));
    }

    #[tokio::test]
    async fn test_config_watcher() {
        let config_manager = ConfigManager::new();
        let mut watcher = config_manager.watch_config();
        
        // Initial config should be available
        let initial_config = watcher.borrow().clone();
        assert_eq!(initial_config.server.port, 6379);
    }
} 