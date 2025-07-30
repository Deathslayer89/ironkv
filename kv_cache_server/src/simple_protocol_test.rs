use crate::protocol::{RespValue, RespCommand, parse_command, create_response, create_ok_response, create_error_response, create_integer_response, create_null_response};
use kv_cache_core::Value;

#[test]
fn test_basic_protocol_functionality() {
    // Test RespValue creation
    assert_eq!(RespValue::ok(), RespValue::SimpleString("OK".to_string()));
    assert_eq!(RespValue::null(), RespValue::BulkString(None));
    
    // Test command parsing
    let command = parse_command("SET key value").unwrap();
    assert_eq!(command.command, "SET");
    assert_eq!(command.args, vec!["key", "value"]);
    
    // Test response creation
    let value = Value::String("test".to_string());
    let response = create_response(&value);
    assert_eq!(response, RespValue::BulkString(Some("test".to_string())));
    
    // Test utility functions
    assert_eq!(create_ok_response(), RespValue::SimpleString("OK".to_string()));
    assert_eq!(create_error_response("test error"), RespValue::Error("test error".to_string()));
    assert_eq!(create_integer_response(42), RespValue::Integer(42));
    assert_eq!(create_null_response(), RespValue::BulkString(None));
}

#[test]
fn test_resp_value_to_string() {
    assert_eq!(RespValue::SimpleString("OK".to_string()).to_string(), "+OK\r\n");
    assert_eq!(RespValue::Error("ERR test".to_string()).to_string(), "-ERR test\r\n");
    assert_eq!(RespValue::Integer(123).to_string(), ":123\r\n");
    assert_eq!(RespValue::BulkString(Some("test".to_string())).to_string(), "$4\r\ntest\r\n");
    assert_eq!(RespValue::BulkString(None).to_string(), "$-1\r\n");
}

#[test]
fn test_command_conversion() {
    let command = RespCommand::new("GET".to_string(), vec!["key".to_string()]);
    let array = command.to_array();
    
    match array {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], RespValue::BulkString(Some("GET".to_string())));
            assert_eq!(items[1], RespValue::BulkString(Some("key".to_string())));
        }
        _ => panic!("Expected array"),
    }
} 