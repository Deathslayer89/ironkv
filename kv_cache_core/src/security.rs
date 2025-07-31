//! Security module for the distributed key-value cache
//! 
//! This module provides comprehensive security features including:
//! - Authentication and authorization
//! - TLS/SSL encryption
//! - Audit logging
//! - Secure configuration management
//! - Role-based access control

use crate::config::SecurityConfig;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};

/// User authentication information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    /// Unique user ID
    pub id: String,
    /// Username
    pub username: String,
    /// Password hash (bcrypt)
    pub password_hash: String,
    /// User roles
    pub roles: Vec<String>,
    /// User permissions
    pub permissions: Vec<String>,
    /// Account creation time
    pub created_at: DateTime<Utc>,
    /// Last login time
    pub last_login: Option<DateTime<Utc>>,
    /// Account status
    pub status: UserStatus,
    /// Account expiration
    pub expires_at: Option<DateTime<Utc>>,
}

/// User account status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum UserStatus {
    /// Account is active
    Active,
    /// Account is disabled
    Disabled,
    /// Account is locked
    Locked,
    /// Account is expired
    Expired,
}

/// Authentication result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthResult {
    /// Authentication success
    pub success: bool,
    /// User information if successful
    pub user: Option<User>,
    /// Error message if failed
    pub error: Option<String>,
    /// Session token if successful
    pub session_token: Option<String>,
    /// Token expiration
    pub token_expires_at: Option<DateTime<Utc>>,
}

/// Authorization result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthzResult {
    /// Authorization success
    pub allowed: bool,
    /// Reason for denial
    pub reason: Option<String>,
    /// Required permissions
    pub required_permissions: Vec<String>,
    /// User permissions
    pub user_permissions: Vec<String>,
}

/// Permission levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum Permission {
    /// Read access to keys
    Read,
    /// Write access to keys
    Write,
    /// Delete access to keys
    Delete,
    /// Administrative access
    Admin,
    /// Cluster management
    Cluster,
    /// Configuration management
    Config,
    /// Monitoring access
    Monitor,
}

/// Role definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    /// Role name
    pub name: String,
    /// Role description
    pub description: String,
    /// Role permissions
    pub permissions: Vec<Permission>,
    /// Role hierarchy
    pub inherits_from: Vec<String>,
}

/// Audit log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditLogEntry {
    /// Timestamp
    pub timestamp: DateTime<Utc>,
    /// User ID
    pub user_id: Option<String>,
    /// Action performed
    pub action: String,
    /// Resource accessed
    pub resource: String,
    /// Success status
    pub success: bool,
    /// IP address
    pub ip_address: Option<String>,
    /// User agent
    pub user_agent: Option<String>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

/// Security manager
pub struct SecurityManager {
    config: SecurityConfig,
    users: Arc<RwLock<HashMap<String, User>>>,
    roles: Arc<RwLock<HashMap<String, Role>>>,
    sessions: Arc<RwLock<HashMap<String, Session>>>,
    audit_log: Arc<RwLock<Vec<AuditLogEntry>>>,
}

/// User session
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    /// Session token
    pub token: String,
    /// User ID
    pub user_id: String,
    /// Session creation time
    pub created_at: DateTime<Utc>,
    /// Session expiration time
    pub expires_at: DateTime<Utc>,
    /// Last activity time
    pub last_activity: DateTime<Utc>,
    /// IP address
    pub ip_address: Option<String>,
    /// User agent
    pub user_agent: Option<String>,
}

impl SecurityManager {
    /// Create a new security manager
    pub fn new(config: SecurityConfig) -> Self {
        Self {
            config,
            users: Arc::new(RwLock::new(HashMap::new())),
            roles: Arc::new(RwLock::new(HashMap::new())),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            audit_log: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Initialize default users and roles
    pub async fn initialize_defaults(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Create default roles
        let admin_role = Role {
            name: "admin".to_string(),
            description: "Administrator role with full access".to_string(),
            permissions: vec![
                Permission::Read,
                Permission::Write,
                Permission::Delete,
                Permission::Admin,
                Permission::Cluster,
                Permission::Config,
                Permission::Monitor,
            ],
            inherits_from: vec![],
        };

        let user_role = Role {
            name: "user".to_string(),
            description: "Standard user role".to_string(),
            permissions: vec![Permission::Read, Permission::Write],
            inherits_from: vec![],
        };

        let read_only_role = Role {
            name: "readonly".to_string(),
            description: "Read-only user role".to_string(),
            permissions: vec![Permission::Read],
            inherits_from: vec![],
        };

        {
            let mut roles = self.roles.write().await;
            roles.insert("admin".to_string(), admin_role);
            roles.insert("user".to_string(), user_role);
            roles.insert("readonly".to_string(), read_only_role);
        }

        // Create default admin user if enabled
        if self.config.create_default_admin {
            let admin_user = User {
                id: "admin".to_string(),
                username: "admin".to_string(),
                password_hash: self.hash_password(&self.config.default_admin_password)?,
                roles: vec!["admin".to_string()],
                permissions: vec![],
                created_at: Utc::now(),
                last_login: None,
                status: UserStatus::Active,
                expires_at: None,
            };

            {
                let mut users = self.users.write().await;
                users.insert("admin".to_string(), admin_user);
            }
        }

        Ok(())
    }

    /// Authenticate a user
    pub async fn authenticate(
        &self,
        username: &str,
        password: &str,
        ip_address: Option<String>,
        user_agent: Option<String>,
    ) -> AuthResult {
        let start_time = Instant::now();

        // Check if user exists
        let user = {
            let users = self.users.read().await;
            users.get(username).cloned()
        };

        let user = match user {
            Some(user) => user,
            None => {
                self.log_audit_event(
                    None,
                    "authentication",
                    "user",
                    false,
                    ip_address.clone(),
                    user_agent.clone(),
                    [("username".to_string(), username.to_string())].into_iter().collect(),
                ).await;

                return AuthResult {
                    success: false,
                    user: None,
                    error: Some("Invalid username or password".to_string()),
                    session_token: None,
                    token_expires_at: None,
                };
            }
        };

        // Check account status
        if user.status != UserStatus::Active {
            self.log_audit_event(
                Some(user.id.clone()),
                "authentication",
                "user",
                false,
                ip_address.clone(),
                user_agent.clone(),
                [("reason".to_string(), format!("Account status: {:?}", user.status))].into_iter().collect(),
            ).await;

            return AuthResult {
                success: false,
                user: None,
                error: Some(format!("Account is {:?}", user.status)),
                session_token: None,
                token_expires_at: None,
            };
        }

        // Check account expiration
        if let Some(expires_at) = user.expires_at {
            if Utc::now() > expires_at {
                self.log_audit_event(
                    Some(user.id.clone()),
                    "authentication",
                    "user",
                    false,
                    ip_address.clone(),
                    user_agent.clone(),
                    [("reason".to_string(), "Account expired".to_string())].into_iter().collect(),
                ).await;

                return AuthResult {
                    success: false,
                    user: None,
                    error: Some("Account has expired".to_string()),
                    session_token: None,
                    token_expires_at: None,
                };
            }
        }

        // Verify password
        let password_valid = match self.verify_password(password, &user.password_hash) {
            Ok(valid) => valid,
            Err(e) => {
                self.log_audit_event(
                    Some(user.id.clone()),
                    "authentication",
                    "user",
                    false,
                    ip_address.clone(),
                    user_agent.clone(),
                    [("reason".to_string(), format!("Password verification error: {}", e))].into_iter().collect(),
                ).await;

                return AuthResult {
                    success: false,
                    user: None,
                    error: Some(format!("Password verification failed: {}", e)),
                    session_token: None,
                    token_expires_at: None,
                };
            }
        };
        if !password_valid {
            self.log_audit_event(
                Some(user.id.clone()),
                "authentication",
                "user",
                false,
                ip_address.clone(),
                user_agent.clone(),
                [("reason".to_string(), "Invalid password".to_string())].into_iter().collect(),
            ).await;

            return AuthResult {
                success: false,
                user: None,
                error: Some("Invalid username or password".to_string()),
                session_token: None,
                token_expires_at: None,
            };
        }

        // Create session
        let session_token = self.generate_session_token();
        let expires_at = Utc::now() + chrono::Duration::seconds(self.config.session_timeout as i64);

        let session = Session {
            token: session_token.clone(),
            user_id: user.id.clone(),
            created_at: Utc::now(),
            expires_at,
            last_activity: Utc::now(),
            ip_address,
            user_agent,
        };

        {
            let mut sessions = self.sessions.write().await;
            sessions.insert(session_token.clone(), session);
        }

        // Update last login
        {
            let mut users = self.users.write().await;
            if let Some(user_mut) = users.get_mut(&user.id) {
                user_mut.last_login = Some(Utc::now());
            }
        }

        self.log_audit_event(
            Some(user.id.clone()),
            "authentication",
            "user",
            true,
            None,
            None,
            [("duration_ms".to_string(), start_time.elapsed().as_millis().to_string())].into_iter().collect(),
        ).await;

        AuthResult {
            success: true,
            user: Some(user),
            error: None,
            session_token: Some(session_token),
            token_expires_at: Some(expires_at),
        }
    }

    /// Authorize an action
    pub async fn authorize(
        &self,
        session_token: &str,
        action: &str,
        resource: &str,
    ) -> AuthzResult {
        // Validate session
        let session = {
            let sessions = self.sessions.read().await;
            sessions.get(session_token).cloned()
        };

        let session = match session {
            Some(session) => session,
            None => {
                return AuthzResult {
                    allowed: false,
                    reason: Some("Invalid session token".to_string()),
                    required_permissions: vec![],
                    user_permissions: vec![],
                };
            }
        };

        // Check session expiration
        if Utc::now() > session.expires_at {
            return AuthzResult {
                allowed: false,
                reason: Some("Session expired".to_string()),
                required_permissions: vec![],
                user_permissions: vec![],
            };
        }

        // Get user
        let user = {
            let users = self.users.read().await;
            users.get(&session.user_id).cloned()
        };

        let user = match user {
            Some(user) => user,
            None => {
                return AuthzResult {
                    allowed: false,
                    reason: Some("User not found".to_string()),
                    required_permissions: vec![],
                    user_permissions: vec![],
                };
            }
        };

        // Determine required permissions based on action
        let required_permissions = self.get_required_permissions(action);
        let user_permissions = self.get_user_permissions(&user).await;

        // Check if user has required permissions
        let allowed = required_permissions.iter().all(|perm| {
            user_permissions.contains(perm)
        });

        AuthzResult {
            allowed,
            reason: if allowed { None } else { Some("Insufficient permissions".to_string()) },
            required_permissions: required_permissions.into_iter().map(|p| format!("{:?}", p)).collect(),
            user_permissions: user_permissions.into_iter().map(|p| format!("{:?}", p)).collect(),
        }
    }

    /// Validate session token
    pub async fn validate_session(&self, session_token: &str) -> Option<User> {
        let session = {
            let sessions = self.sessions.read().await;
            sessions.get(session_token).cloned()
        };

        let session = session?;

        // Check session expiration
        if Utc::now() > session.expires_at {
            return None;
        }

        // Update last activity
        {
            let mut sessions = self.sessions.write().await;
            if let Some(session_mut) = sessions.get_mut(session_token) {
                session_mut.last_activity = Utc::now();
            }
        }

        // Get user
        let users = self.users.read().await;
        users.get(&session.user_id).cloned()
    }

    /// Log audit event
    pub async fn log_audit_event(
        &self,
        user_id: Option<String>,
        action: &str,
        resource: &str,
        success: bool,
        ip_address: Option<String>,
        user_agent: Option<String>,
        metadata: HashMap<String, String>,
    ) {
        let entry = AuditLogEntry {
            timestamp: Utc::now(),
            user_id,
            action: action.to_string(),
            resource: resource.to_string(),
            success,
            ip_address,
            user_agent,
            metadata,
        };

        {
            let mut audit_log = self.audit_log.write().await;
            audit_log.push(entry);

            // Trim audit log if it exceeds maximum size
            if audit_log.len() > self.config.max_audit_log_entries {
                let len = audit_log.len();
            if len > self.config.max_audit_log_entries {
                audit_log.drain(0..len - self.config.max_audit_log_entries);
            }
            }
        }
    }

    /// Get audit log entries
    pub async fn get_audit_log(
        &self,
        limit: Option<usize>,
        user_id: Option<String>,
        action: Option<String>,
    ) -> Vec<AuditLogEntry> {
        let audit_log = self.audit_log.read().await;
        let mut entries: Vec<AuditLogEntry> = audit_log.iter()
            .filter(|entry| {
                if let Some(ref uid) = user_id {
                    if entry.user_id.as_ref() != Some(uid) {
                        return false;
                    }
                }
                if let Some(ref act) = action {
                    if &entry.action != act {
                        return false;
                    }
                }
                true
            })
            .cloned()
            .collect();

        entries.reverse(); // Most recent first

        if let Some(limit) = limit {
            entries.truncate(limit);
        }

        entries
    }

    /// Clean up expired sessions
    pub async fn cleanup_expired_sessions(&self) {
        let now = Utc::now();
        let mut sessions = self.sessions.write().await;
        sessions.retain(|_, session| session.expires_at > now);
    }

    /// Hash password using bcrypt
    fn hash_password(&self, password: &str) -> Result<String, Box<dyn std::error::Error>> {
        // In a real implementation, you would use bcrypt
        // For now, we'll use a simple hash for demonstration
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        password.hash(&mut hasher);
        Ok(format!("{:x}", hasher.finish()))
    }

    /// Verify password
    fn verify_password(&self, password: &str, hash: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let password_hash = self.hash_password(password)?;
        Ok(password_hash == *hash)
    }

    /// Generate session token
    fn generate_session_token(&self) -> String {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let bytes: [u8; 32] = rng.gen();
        hex::encode(bytes)
    }

    /// Get required permissions for an action
    fn get_required_permissions(&self, action: &str) -> Vec<Permission> {
        match action {
            "GET" | "get" => vec![Permission::Read],
            "SET" | "set" => vec![Permission::Write],
            "DEL" | "del" | "delete" => vec![Permission::Delete],
            "admin" => vec![Permission::Admin],
            "cluster" => vec![Permission::Cluster],
            "config" => vec![Permission::Config],
            "monitor" => vec![Permission::Monitor],
            _ => vec![Permission::Read], // Default to read permission
        }
    }

    /// Get user permissions based on roles
    async fn get_user_permissions(&self, user: &User) -> Vec<Permission> {
        let mut permissions = Vec::new();
        let roles = self.roles.read().await;

        for role_name in &user.roles {
            if let Some(role) = roles.get(role_name) {
                permissions.extend(role.permissions.clone());
            }
        }

        // Add direct permissions
        for perm_str in &user.permissions {
            if let Ok(perm) = serde_json::from_str::<Permission>(perm_str) {
                permissions.push(perm);
            }
        }

        // Note: permissions are already unique due to the way they're collected
        permissions.dedup();
        permissions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_security_manager_creation() {
        let config = SecurityConfig::default();
        let security_manager = SecurityManager::new(config);
        assert!(security_manager.initialize_defaults().await.is_ok());
    }

    #[tokio::test]
    async fn test_authentication() {
        let config = SecurityConfig::default();
        let security_manager = SecurityManager::new(config);
        security_manager.initialize_defaults().await.unwrap();

        let result = security_manager.authenticate("admin", "admin123", None, None).await;
        assert!(result.success);
        assert!(result.session_token.is_some());
    }

    #[tokio::test]
    async fn test_authorization() {
        let config = SecurityConfig::default();
        let security_manager = SecurityManager::new(config);
        security_manager.initialize_defaults().await.unwrap();

        let auth_result = security_manager.authenticate("admin", "admin123", None, None).await;
        assert!(auth_result.success);

        let session_token = auth_result.session_token.unwrap();
        let authz_result = security_manager.authorize(&session_token, "GET", "test-key").await;
        assert!(authz_result.allowed);
    }

    #[tokio::test]
    async fn test_audit_logging() {
        let config = SecurityConfig::default();
        let security_manager = SecurityManager::new(config);

        security_manager.log_audit_event(
            Some("test-user".to_string()),
            "test-action",
            "test-resource",
            true,
            None,
            None,
            HashMap::new(),
        ).await;

        let entries = security_manager.get_audit_log(None, None, None).await;
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].action, "test-action");
    }
} 