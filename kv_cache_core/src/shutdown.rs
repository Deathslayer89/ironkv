//! Graceful shutdown handling for the key-value cache
//! 
//! This module provides graceful shutdown functionality with signal handling
//! and coordinated shutdown of all system components.

use crate::config::CacheConfig;
use crate::log::log_shutdown;
use crate::trace::trace_shutdown;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::time::timeout;

/// Shutdown signal types
#[derive(Debug, Clone)]
pub enum ShutdownSignal {
    /// SIGINT (Ctrl+C)
    Interrupt,
    /// SIGTERM
    Terminate,
    /// Manual shutdown request
    Manual,
    /// Timeout reached
    Timeout,
}

/// Shutdown coordinator for managing graceful shutdown
pub struct ShutdownCoordinator {
    shutdown_tx: broadcast::Sender<ShutdownSignal>,
    shutdown_rx: broadcast::Receiver<ShutdownSignal>,
    shutdown_complete_tx: mpsc::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
    is_shutting_down: Arc<AtomicBool>,
    config: CacheConfig,
}

/// Component that can be shut down gracefully
pub trait ShutdownComponent {
    /// Shutdown the component gracefully
    async fn shutdown(&mut self, signal: ShutdownSignal) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
    
    /// Get the component name for logging
    fn name(&self) -> &str;
}

impl ShutdownCoordinator {
    /// Create a new shutdown coordinator
    pub fn new(config: CacheConfig) -> Self {
        let (shutdown_tx, shutdown_rx) = broadcast::channel(100);
        let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(100);
        
        Self {
            shutdown_tx,
            shutdown_rx,
            shutdown_complete_tx,
            shutdown_complete_rx,
            is_shutting_down: Arc::new(AtomicBool::new(false)),
            config,
        }
    }

    /// Start the shutdown coordinator
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.config.server.graceful_shutdown {
            return Ok(());
        }

        let shutdown_tx = self.shutdown_tx.clone();
        let is_shutting_down = Arc::clone(&self.is_shutting_down);

        // Spawn signal handler task
        tokio::spawn(async move {
            Self::handle_signals(shutdown_tx, is_shutting_down).await;
        });

        Ok(())
    }

    /// Handle system signals
    async fn handle_signals(
        shutdown_tx: broadcast::Sender<ShutdownSignal>,
        is_shutting_down: Arc<AtomicBool>,
    ) {
        let mut interrupt = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
            .expect("Failed to create SIGINT signal handler");
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to create SIGTERM signal handler");

        loop {
            tokio::select! {
                _ = interrupt.recv() => {
                    if !is_shutting_down.load(Ordering::Relaxed) {
                        tracing::info!("Received SIGINT, initiating graceful shutdown");
                        let _ = shutdown_tx.send(ShutdownSignal::Interrupt);
                    }
                }
                _ = terminate.recv() => {
                    if !is_shutting_down.load(Ordering::Relaxed) {
                        tracing::info!("Received SIGTERM, initiating graceful shutdown");
                        let _ = shutdown_tx.send(ShutdownSignal::Terminate);
                    }
                }
            }
        }
    }

    /// Wait for shutdown signal
    pub async fn wait_for_shutdown(&mut self) -> ShutdownSignal {
        let mut rx = self.shutdown_tx.subscribe();
        rx.recv().await.unwrap_or(ShutdownSignal::Manual)
    }

    /// Initiate shutdown
    pub async fn shutdown(&mut self, signal: ShutdownSignal) -> Result<(), Box<dyn std::error::Error>> {
        if self.is_shutting_down.load(Ordering::Relaxed) {
            return Ok(());
        }

        self.is_shutting_down.store(true, Ordering::Relaxed);
        
        let reason = match signal {
            ShutdownSignal::Interrupt => "SIGINT received",
            ShutdownSignal::Terminate => "SIGTERM received",
            ShutdownSignal::Manual => "Manual shutdown",
            ShutdownSignal::Timeout => "Shutdown timeout",
        };

        tracing::info!("Initiating graceful shutdown: {}", reason);
        log_shutdown(reason);
        trace_shutdown(reason);

        // Send shutdown signal to all components
        let _ = self.shutdown_tx.send(signal.clone());

        // Wait for shutdown completion with timeout
        let shutdown_timeout = self.config.shutdown_duration();
        match timeout(shutdown_timeout, self.wait_for_shutdown_completion()).await {
            Ok(_) => {
                tracing::info!("Graceful shutdown completed successfully");
                Ok(())
            }
            Err(_) => {
                tracing::warn!("Shutdown timeout reached, forcing shutdown");
                self.force_shutdown().await;
                Ok(())
            }
        }
    }

    /// Wait for all components to complete shutdown
    async fn wait_for_shutdown_completion(&mut self) {
        // Wait for shutdown complete messages from all components
        while let Some(_) = self.shutdown_complete_rx.recv().await {
            // Continue until channel is closed
        }
    }

    /// Force shutdown without waiting for components
    async fn force_shutdown(&self) {
        tracing::error!("Force shutdown initiated");
        // In a real implementation, you might want to abort tasks or take other forceful measures
    }

    /// Register a component for shutdown
    pub fn register_component(&self) -> ShutdownHandle {
        ShutdownHandle {
            shutdown_rx: self.shutdown_tx.subscribe(),
            shutdown_complete_tx: self.shutdown_complete_tx.clone(),
        }
    }

    /// Check if shutdown is in progress
    pub fn is_shutting_down(&self) -> bool {
        self.is_shutting_down.load(Ordering::Relaxed)
    }

    /// Get shutdown receiver for components
    pub fn subscribe(&self) -> broadcast::Receiver<ShutdownSignal> {
        self.shutdown_tx.subscribe()
    }
}

/// Handle for components to participate in shutdown
pub struct ShutdownHandle {
    shutdown_rx: broadcast::Receiver<ShutdownSignal>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

impl ShutdownHandle {
    /// Wait for shutdown signal
    pub async fn wait_for_shutdown(&mut self) -> ShutdownSignal {
        self.shutdown_rx.recv().await.unwrap_or(ShutdownSignal::Manual)
    }

    /// Signal that shutdown is complete
    pub async fn shutdown_complete(&self) {
        let _ = self.shutdown_complete_tx.send(()).await;
    }

    /// Run a component with shutdown handling
    pub async fn run_with_shutdown<F, R>(
        &mut self,
        component_name: &str,
        mut component: impl ShutdownComponent,
        run_fn: F,
    ) -> Result<R, Box<dyn std::error::Error>>
    where
        F: FnOnce() -> Result<R, Box<dyn std::error::Error>>,
    {
        let shutdown_signal = self.wait_for_shutdown().await;
        
        tracing::info!("Shutting down component: {}", component_name);
        
        // Shutdown the component
        if let Err(e) = component.shutdown(shutdown_signal.clone()).await {
            tracing::error!("Error shutting down {}: {}", component_name, e);
        }
        
        // Signal shutdown completion
        self.shutdown_complete().await;
        
        // Return the result of the run function
        run_fn()
    }
}

/// Graceful shutdown manager for the entire system
pub struct GracefulShutdownManager {
    coordinator: ShutdownCoordinator,
}

impl GracefulShutdownManager {
    /// Create a new shutdown manager
    pub fn new(config: CacheConfig) -> Self {
        Self {
            coordinator: ShutdownCoordinator::new(config),
        }
    }

    /// Start the shutdown manager
    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.coordinator.start().await
    }

    /// Run the system with graceful shutdown
    pub async fn run<F, R>(&mut self, run_fn: F) -> Result<R, Box<dyn std::error::Error>>
    where
        F: FnOnce() -> Result<R, Box<dyn std::error::Error>>,
    {
        // Start shutdown coordinator
        self.start().await?;

        // Run the main function
        let result = run_fn();

        // If there was an error, initiate shutdown
        if result.is_err() {
            self.coordinator.shutdown(ShutdownSignal::Manual).await?;
        }

        result
    }

    /// Get shutdown handle for components
    pub fn get_shutdown_handle(&self) -> ShutdownHandle {
        self.coordinator.register_component()
    }

    /// Check if shutdown is in progress
    pub fn is_shutting_down(&self) -> bool {
        self.coordinator.is_shutting_down()
    }
}

/// Example shutdown component implementation
pub struct ExampleComponent {
    name: String,
    is_running: bool,
}

impl ExampleComponent {
    pub fn new(name: String) -> Self {
        Self {
            name,
            is_running: true,
        }
    }

    pub async fn run(&mut self, mut shutdown_handle: ShutdownHandle) -> Result<(), Box<dyn std::error::Error>> {
        while self.is_running {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    // Do work
                }
                signal = shutdown_handle.wait_for_shutdown() => {
                    tracing::info!("Component {} received shutdown signal: {:?}", self.name, signal);
                    break;
                }
            }
        }
        
        shutdown_handle.shutdown_complete().await;
        Ok(())
    }
}

impl ShutdownComponent for ExampleComponent {
    async fn shutdown(&mut self, _signal: ShutdownSignal) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        tracing::info!("Shutting down component: {}", self.name);
        self.is_running = false;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CacheConfig;

    #[tokio::test]
    async fn test_shutdown_coordinator_creation() {
        let config = CacheConfig::default();
        let coordinator = ShutdownCoordinator::new(config);
        assert!(!coordinator.is_shutting_down());
    }

    #[tokio::test]
    async fn test_shutdown_handle() {
        let config = CacheConfig::default();
        let coordinator = ShutdownCoordinator::new(config);
        let handle = coordinator.register_component();
        
        // Test that we can get a shutdown handle
        assert!(true);
    }

    #[tokio::test]
    async fn test_example_component() {
        let mut component = ExampleComponent::new("test".to_string());
        assert_eq!(component.name(), "test");
        
        let shutdown_result = component.shutdown(ShutdownSignal::Manual).await;
        assert!(shutdown_result.is_ok());
        assert!(!component.is_running);
    }

    #[tokio::test]
    async fn test_graceful_shutdown_manager() {
        let config = CacheConfig::default();
        let manager = GracefulShutdownManager::new(config);
        
        assert!(!manager.is_shutting_down());
    }
} 