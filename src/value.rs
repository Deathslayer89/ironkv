use std::collections::{HashMap, HashSet, VecDeque};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    String(String),
    List(VecDeque<String>),
    Hash(HashMap<String, String>),
    Set(HashSet<String>),
}

impl Value {
    // String operations
    pub fn as_string(&self) -> Option<&String> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_string_mut(&mut self) -> Option<&mut String> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    // List operations
    pub fn as_list(&self) -> Option<&VecDeque<String>> {
        match self {
            Value::List(list) => Some(list),
            _ => None,
        }
    }

    pub fn as_list_mut(&mut self) -> Option<&mut VecDeque<String>> {
        match self {
            Value::List(list) => Some(list),
            _ => None,
        }
    }

    // Hash operations
    pub fn as_hash(&self) -> Option<&HashMap<String, String>> {
        match self {
            Value::Hash(hash) => Some(hash),
            _ => None,
        }
    }

    pub fn as_hash_mut(&mut self) -> Option<&mut HashMap<String, String>> {
        match self {
            Value::Hash(hash) => Some(hash),
            _ => None,
        }
    }

    // Set operations
    pub fn as_set(&self) -> Option<&HashSet<String>> {
        match self {
            Value::Set(set) => Some(set),
            _ => None,
        }
    }

    pub fn as_set_mut(&mut self) -> Option<&mut HashSet<String>> {
        match self {
            Value::Set(set) => Some(set),
            _ => None,
        }
    }

    // Type checking
    pub fn is_string(&self) -> bool {
        matches!(self, Value::String(_))
    }

    pub fn is_list(&self) -> bool {
        matches!(self, Value::List(_))
    }

    pub fn is_hash(&self) -> bool {
        matches!(self, Value::Hash(_))
    }

    pub fn is_set(&self) -> bool {
        matches!(self, Value::Set(_))
    }

    // Conversion helpers
    pub fn from_string(s: String) -> Self {
        Value::String(s)
    }

    pub fn from_list(list: VecDeque<String>) -> Self {
        Value::List(list)
    }

    pub fn from_hash(hash: HashMap<String, String>) -> Self {
        Value::Hash(hash)
    }

    pub fn from_set(set: HashSet<String>) -> Self {
        Value::Set(set)
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_string())
    }
}

impl From<VecDeque<String>> for Value {
    fn from(list: VecDeque<String>) -> Self {
        Value::List(list)
    }
}

impl From<HashMap<String, String>> for Value {
    fn from(hash: HashMap<String, String>) -> Self {
        Value::Hash(hash)
    }
}

impl From<HashSet<String>> for Value {
    fn from(set: HashSet<String>) -> Self {
        Value::Set(set)
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(s) => write!(f, "{}", s),
            Value::List(list) => write!(f, "[{}]", list.iter().map(|s| format!("\"{}\"", s)).collect::<Vec<_>>().join(", ")),
            Value::Hash(hash) => {
                let pairs: Vec<String> = hash.iter().map(|(k, v)| format!("\"{}\": \"{}\"", k, v)).collect();
                write!(f, "{{{}}}", pairs.join(", "))
            }
            Value::Set(set) => {
                let items: Vec<String> = set.iter().map(|s| format!("\"{}\"", s)).collect();
                write!(f, "{{{}}}", items.join(", "))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_string_value() {
        let value = Value::from("hello");
        assert!(value.is_string());
        assert_eq!(value.as_string(), Some(&"hello".to_string()));
        assert_eq!(value.to_string(), "hello");
    }

    #[test]
    fn test_list_value() {
        let mut list = VecDeque::new();
        list.push_back("item1".to_string());
        list.push_back("item2".to_string());
        
        let value = Value::from(list.clone());
        assert!(value.is_list());
        assert_eq!(value.as_list(), Some(&list));
        assert_eq!(value.to_string(), "[\"item1\", \"item2\"]");
    }

    #[test]
    fn test_hash_value() {
        let mut hash = HashMap::new();
        hash.insert("key1".to_string(), "value1".to_string());
        hash.insert("key2".to_string(), "value2".to_string());
        
        let value = Value::from(hash.clone());
        assert!(value.is_hash());
        assert_eq!(value.as_hash(), Some(&hash));
    }

    #[test]
    fn test_set_value() {
        let mut set = HashSet::new();
        set.insert("item1".to_string());
        set.insert("item2".to_string());
        
        let value = Value::from(set.clone());
        assert!(value.is_set());
        assert_eq!(value.as_set(), Some(&set));
    }

    #[test]
    fn test_type_checking() {
        let string_val = Value::String("test".to_string());
        let list_val = Value::List(VecDeque::new());
        let hash_val = Value::Hash(HashMap::new());
        let set_val = Value::Set(HashSet::new());

        assert!(string_val.is_string());
        assert!(!string_val.is_list());
        assert!(!string_val.is_hash());
        assert!(!string_val.is_set());

        assert!(list_val.is_list());
        assert!(!list_val.is_string());
        assert!(!list_val.is_hash());
        assert!(!list_val.is_set());

        assert!(hash_val.is_hash());
        assert!(!hash_val.is_string());
        assert!(!hash_val.is_list());
        assert!(!hash_val.is_set());

        assert!(set_val.is_set());
        assert!(!set_val.is_string());
        assert!(!set_val.is_list());
        assert!(!set_val.is_hash());
    }
} 