//! Raft log management for the consensus system
//! 
//! This module provides log entry management, persistence, and log operations
//! for the Raft consensus algorithm.

use crate::consensus::state::RaftTerm;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

/// Log index identifier

/// Log index identifier
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize)]
pub struct LogIndex(pub u64);

impl LogIndex {
    /// Increment the index
    pub fn increment(&mut self) {
        self.0 += 1;
    }

    /// Get the index value
    pub fn value(&self) -> u64 {
        self.0
    }

    /// Check if this is the first index
    pub fn is_first(&self) -> bool {
        self.0 == 0
    }
}

impl std::fmt::Display for LogIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Log term identifier (same as RaftTerm but for log entries)
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize)]
pub struct LogTerm(pub u64);

impl LogTerm {
    /// Get the term value
    pub fn value(&self) -> u64 {
        self.0
    }
}

impl From<RaftTerm> for LogTerm {
    fn from(term: RaftTerm) -> Self {
        LogTerm(term.value())
    }
}

impl From<LogTerm> for RaftTerm {
    fn from(term: LogTerm) -> Self {
        RaftTerm(term.0)
    }
}

/// Log entry in the Raft log
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    /// Term when entry was received by leader
    pub term: RaftTerm,
    /// Index of the entry in the log
    pub index: LogIndex,
    /// Command to be applied to state machine
    pub command: Vec<u8>,
    /// Timestamp when entry was created
    pub timestamp: u64,
}

impl LogEntry {
    /// Create a new log entry
    pub fn new(term: RaftTerm, index: LogIndex, command: Vec<u8>) -> Self {
        Self {
            term,
            index,
            command,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        }
    }

    /// Get the entry size in bytes
    pub fn size(&self) -> usize {
        self.command.len()
    }

    /// Check if this is a no-op entry
    pub fn is_noop(&self) -> bool {
        self.command.is_empty()
    }
}

/// Raft log implementation
#[derive(Debug)]
pub struct RaftLog {
    /// Log entries (index -> entry)
    entries: HashMap<u64, LogEntry>,
    /// Next index to be assigned
    next_index: LogIndex,
    /// Last index in the log
    last_index: LogIndex,
    /// Last term in the log
    last_term: LogTerm,
    /// Total size of the log in bytes
    total_size: usize,
    /// Maximum log size in bytes
    max_size: usize,
    /// Whether the log is compacted
    compacted: bool,
}

impl RaftLog {
    /// Create a new Raft log
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            next_index: LogIndex(1), // Start from 1, 0 is reserved
            last_index: LogIndex(0),
            last_term: LogTerm(0),
            total_size: 0,
            max_size: 1024 * 1024 * 100, // 100MB default
            compacted: false,
        }
    }

    /// Create a new Raft log with custom max size
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            entries: HashMap::new(),
            next_index: LogIndex(1),
            last_index: LogIndex(0),
            last_term: LogTerm(0),
            total_size: 0,
            max_size,
            compacted: false,
        }
    }

    /// Append a log entry
    pub async fn append(&mut self, entry: LogEntry) -> Result<LogIndex, Box<dyn std::error::Error>> {
        // Validate entry
        if entry.index.value() != self.next_index.value() {
            return Err(format!(
                "Invalid log index: expected {}, got {}",
                self.next_index, entry.index
            ).into());
        }

        // Check if we need to truncate conflicting entries
        if let Some(existing_entry) = self.entries.get(&entry.index.value()) {
            if existing_entry.term != entry.term {
                // Truncate from this index onwards
                self.truncate_from(entry.index).await?;
            }
        }

        // Add the entry
        let entry_size = entry.size();
        self.entries.insert(entry.index.value(), entry.clone());
        self.total_size += entry_size;
        self.next_index.increment();
        self.last_index = entry.index;
        self.last_term = LogTerm::from(entry.term);

        // Check if we need to compact
        if self.total_size > self.max_size {
            self.compact().await?;
        }

        Ok(entry.index)
    }

    /// Get a log entry by index
    pub async fn get_entry(&self, index: LogIndex) -> Result<Option<LogEntry>, Box<dyn std::error::Error>> {
        if index.is_first() {
            // Return a dummy entry for index 0
            return Ok(Some(LogEntry::new(RaftTerm(0), LogIndex(0), vec![])));
        }

        Ok(self.entries.get(&index.value()).cloned())
    }

    /// Get the last log entry
    pub async fn get_last_entry(&self) -> Result<LogEntry, Box<dyn std::error::Error>> {
        if self.last_index.is_first() {
            return Ok(LogEntry::new(RaftTerm(0), LogIndex(0), vec![]));
        }

        self.get_entry(self.last_index).await?.ok_or_else(|| {
            "Last entry not found".into()
        })
    }

    /// Get the next index to be assigned
    pub fn get_next_index(&self) -> LogIndex {
        self.next_index
    }

    /// Get the last index in the log
    pub fn get_last_index(&self) -> LogIndex {
        self.last_index
    }

    /// Get the last term in the log
    pub fn get_last_term(&self) -> LogTerm {
        self.last_term
    }

    /// Get the size of the log
    pub fn get_size(&self) -> Result<usize, Box<dyn std::error::Error>> {
        Ok(self.entries.len())
    }

    /// Get the total size in bytes
    pub fn get_total_size(&self) -> usize {
        self.total_size
    }

    /// Truncate the log from a specific index
    pub async fn truncate_from(&mut self, index: LogIndex) -> Result<(), Box<dyn std::error::Error>> {
        let mut to_remove = Vec::new();
        
        for (entry_index, entry) in &self.entries {
            if *entry_index >= index.value() {
                to_remove.push(*entry_index);
                self.total_size = self.total_size.saturating_sub(entry.size());
            }
        }

        for index_to_remove in to_remove {
            self.entries.remove(&index_to_remove);
        }

        // Update last index and term
        if let Some(max_index) = self.entries.keys().max() {
            self.last_index = LogIndex(*max_index);
            if let Some(last_entry) = self.entries.get(max_index) {
                self.last_term = LogTerm::from(last_entry.term);
            }
        } else {
            self.last_index = LogIndex(0);
            self.last_term = LogTerm(0);
        }

        self.next_index = LogIndex(self.last_index.value() + 1);

        Ok(())
    }

    /// Compact the log by removing old entries
    pub async fn compact(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.compacted {
            return Ok(());
        }

        // Simple compaction: keep only the last 1000 entries
        let max_entries = 1000;
        if self.entries.len() <= max_entries {
            return Ok(());
        }

        let mut sorted_indices: Vec<u64> = self.entries.keys().cloned().collect();
        sorted_indices.sort();

        let entries_to_keep = sorted_indices.len().saturating_sub(max_entries);
        let mut to_remove = Vec::new();

        for (i, &index) in sorted_indices.iter().enumerate() {
            if i < entries_to_keep {
                if let Some(entry) = self.entries.get(&index) {
                    self.total_size = self.total_size.saturating_sub(entry.size());
                }
                to_remove.push(index);
            }
        }

        for index in to_remove {
            self.entries.remove(&index);
        }

        self.compacted = true;
        tracing::info!("Log compacted: removed {} entries", entries_to_keep);

        Ok(())
    }

    /// Get log entries in a range
    pub async fn get_entries(
        &self,
        start_index: LogIndex,
        end_index: LogIndex,
    ) -> Result<Vec<LogEntry>, Box<dyn std::error::Error>> {
        let mut entries = Vec::new();
        
        for index in start_index.value()..=end_index.value() {
            if let Some(entry) = self.entries.get(&index) {
                entries.push(entry.clone());
            }
        }

        Ok(entries)
    }

    /// Check if the log contains a specific entry
    pub fn contains_entry(&self, index: LogIndex, term: RaftTerm) -> bool {
        if let Some(entry) = self.entries.get(&index.value()) {
            entry.term == term
        } else {
            false
        }
    }

    /// Get log statistics
    pub fn get_stats(&self) -> LogStats {
        LogStats {
            total_entries: self.entries.len(),
            total_size_bytes: self.total_size,
            last_index: self.last_index,
            last_term: self.last_term,
            next_index: self.next_index,
            compacted: self.compacted,
        }
    }

    /// Clear the log
    pub fn clear(&mut self) {
        self.entries.clear();
        self.next_index = LogIndex(1);
        self.last_index = LogIndex(0);
        self.last_term = LogTerm(0);
        self.total_size = 0;
        self.compacted = false;
    }
}

/// Log statistics
#[derive(Debug, Clone)]
pub struct LogStats {
    pub total_entries: usize,
    pub total_size_bytes: usize,
    pub last_index: LogIndex,
    pub last_term: LogTerm,
    pub next_index: LogIndex,
    pub compacted: bool,
}

impl std::fmt::Display for LogStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Log Stats: entries={}, size={}B, last_index={}, last_term={:?}, next_index={}, compacted={}",
            self.total_entries,
            self.total_size_bytes,
            self.last_index,
            self.last_term,
            self.next_index,
            self.compacted
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_log_creation() {
        let log = RaftLog::new();
        assert_eq!(log.get_next_index(), LogIndex(1));
        assert_eq!(log.get_last_index(), LogIndex(0));
        assert_eq!(log.get_size().unwrap(), 0);
    }

    #[tokio::test]
    async fn test_log_append() {
        let mut log = RaftLog::new();
        
        let entry = LogEntry::new(RaftTerm(1), LogIndex(1), vec![1, 2, 3]);
        let index = log.append(entry).await.unwrap();
        
        assert_eq!(index, LogIndex(1));
        assert_eq!(log.get_next_index(), LogIndex(2));
        assert_eq!(log.get_last_index(), LogIndex(1));
        assert_eq!(log.get_size().unwrap(), 1);
    }

    #[tokio::test]
    async fn test_log_get_entry() {
        let mut log = RaftLog::new();
        
        let entry = LogEntry::new(RaftTerm(1), LogIndex(1), vec![1, 2, 3]);
        log.append(entry.clone()).await.unwrap();
        
        let retrieved = log.get_entry(LogIndex(1)).await.unwrap().unwrap();
        assert_eq!(retrieved.term, entry.term);
        assert_eq!(retrieved.index, entry.index);
        assert_eq!(retrieved.command, entry.command);
    }

    #[tokio::test]
    async fn test_log_truncate() {
        let mut log = RaftLog::new();
        
        // Add some entries
        for i in 1..=5 {
            let entry = LogEntry::new(RaftTerm(1), LogIndex(i), vec![i as u8]);
            log.append(entry).await.unwrap();
        }
        
        assert_eq!(log.get_size().unwrap(), 5);
        
        // Truncate from index 3
        log.truncate_from(LogIndex(3)).await.unwrap();
        
        assert_eq!(log.get_size().unwrap(), 2);
        assert_eq!(log.get_last_index(), LogIndex(2));
        assert_eq!(log.get_next_index(), LogIndex(3));
    }

    #[tokio::test]
    async fn test_log_compaction() {
        let mut log = RaftLog::with_max_size(1000); // Small max size to trigger compaction
        
        // Add many entries
        for i in 1..=1500 {
            let entry = LogEntry::new(RaftTerm(1), LogIndex(i), vec![i as u8]);
            log.append(entry).await.unwrap();
        }
        
        // Compaction should have been triggered
        assert!(log.get_size().unwrap() < 1500);
    }

    #[tokio::test]
    async fn test_log_entries_range() {
        let mut log = RaftLog::new();
        
        // Add some entries
        for i in 1..=5 {
            let entry = LogEntry::new(RaftTerm(1), LogIndex(i), vec![i as u8]);
            log.append(entry).await.unwrap();
        }
        
        let entries = log.get_entries(LogIndex(2), LogIndex(4)).await.unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].index, LogIndex(2));
        assert_eq!(entries[2].index, LogIndex(4));
    }

    #[tokio::test]
    async fn test_log_stats() {
        let mut log = RaftLog::new();
        
        let entry = LogEntry::new(RaftTerm(1), LogIndex(1), vec![1, 2, 3]);
        log.append(entry).await.unwrap();
        
        let stats = log.get_stats();
        assert_eq!(stats.total_entries, 1);
        assert_eq!(stats.last_index, LogIndex(1));
        assert_eq!(stats.next_index, LogIndex(2));
    }
} 