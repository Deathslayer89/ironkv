use crate::protocol::*;
use bytes::BytesMut;
use kv_cache_core::Value;
use std::collections::{HashMap, VecDeque, HashSet};
use tokio_util::codec::{Decoder, Encoder};

// #[tokio::test]
// async fn test_resp_protocol_comprehensive() {
//     // Test all RESP types
//     test_simple_strings().await;
//     test_errors().await;
//     test_integers().await;
//     test_bulk_strings().await;
//     test_arrays().await;
//     test_commands().await;
//     test_responses().await;
//     test_codec_integration().await;
// }

#[allow(dead_code)]
async fn test_simple_strings() {
    let mut codec = RespCodec;
    let mut buf = BytesMut::from("+OK\r\n+PONG\r\n");
    
    let result1 = codec.decode(&mut buf).unwrap();
    assert_eq!(result1, Some(RespValue::SimpleString("OK".to_string())));
    
    let result2 = codec.decode(&mut buf).unwrap();
    assert_eq!(result2, Some(RespValue::SimpleString("PONG".to_string())));
}

#[allow(dead_code)]
async fn test_errors() {
    let mut codec = RespCodec;
    let mut buf = BytesMut::from("-ERR unknown command\r\n-ERR wrong number of arguments\r\n");
    
    let result1 = codec.decode(&mut buf).unwrap();
    assert_eq!(result1, Some(RespValue::Error("ERR unknown command".to_string())));
    
    let result2 = codec.decode(&mut buf).unwrap();
    assert_eq!(result2, Some(RespValue::Error("ERR wrong number of arguments".to_string())));
}

#[allow(dead_code)]
async fn test_integers() {
    let mut codec = RespCodec;
    let mut buf = BytesMut::from(":123\r\n:-456\r\n:0\r\n");
    
    let result1 = codec.decode(&mut buf).unwrap();
    assert_eq!(result1, Some(RespValue::Integer(123)));
    
    let result2 = codec.decode(&mut buf).unwrap();
    assert_eq!(result2, Some(RespValue::Integer(-456)));
    
    let result3 = codec.decode(&mut buf).unwrap();
    assert_eq!(result3, Some(RespValue::Integer(0)));
}

#[allow(dead_code)]
async fn test_bulk_strings() {
    let mut codec = RespCodec;
    let mut buf = BytesMut::from("$5\r\nHello\r\n$0\r\n\r\n$-1\r\n");
    
    let result1 = codec.decode(&mut buf).unwrap();
    assert_eq!(result1, Some(RespValue::BulkString(Some("Hello".to_string()))));
    
    let result2 = codec.decode(&mut buf).unwrap();
    assert_eq!(result2, Some(RespValue::BulkString(Some("".to_string()))));
    
    let result3 = codec.decode(&mut buf).unwrap();
    assert_eq!(result3, Some(RespValue::BulkString(None)));
}

#[allow(dead_code)]
async fn test_arrays() {
    let mut codec = RespCodec;
    let mut buf = BytesMut::from("*2\r\n$3\r\nGET\r\n$4\r\nname\r\n");
    
    let result = codec.decode(&mut buf).unwrap();
    let expected = RespValue::Array(vec![
        RespValue::BulkString(Some("GET".to_string())),
        RespValue::BulkString(Some("name".to_string())),
    ]);
    assert_eq!(result, Some(expected));
}

#[allow(dead_code)]
async fn test_commands() {
    // Test command parsing
    let input = "SET key value EX 60";
    let command = parse_command(input).unwrap();
    assert_eq!(command.command, "SET");
    assert_eq!(command.args, vec!["key", "value", "EX", "60"]);
    
    // Test command to array conversion
    let array = command.to_array();
    let expected = RespValue::Array(vec![
        RespValue::BulkString(Some("SET".to_string())),
        RespValue::BulkString(Some("key".to_string())),
        RespValue::BulkString(Some("value".to_string())),
        RespValue::BulkString(Some("EX".to_string())),
        RespValue::BulkString(Some("60".to_string())),
    ]);
    assert_eq!(array, expected);
    
    // Test array to command conversion
    let command_from_array = RespCommand::from_array(vec![
        RespValue::BulkString(Some("SET".to_string())),
        RespValue::BulkString(Some("key".to_string())),
        RespValue::BulkString(Some("value".to_string())),
        RespValue::BulkString(Some("EX".to_string())),
        RespValue::BulkString(Some("60".to_string())),
    ]).unwrap();
    assert_eq!(command_from_array.command, "SET");
    assert_eq!(command_from_array.args, vec!["key", "value", "EX", "60"]);
}

#[allow(dead_code)]
async fn test_responses() {
    // Test string response
    let value = Value::String("test_value".to_string());
    let response = create_response(&value);
    assert_eq!(response, RespValue::BulkString(Some("test_value".to_string())));
    
    // Test list response
    let mut list = VecDeque::new();
    list.push_back("item1".to_string());
    list.push_back("item2".to_string());
    let value = Value::List(list);
    let response = create_response(&value);
    let expected = RespValue::Array(vec![
        RespValue::BulkString(Some("item1".to_string())),
        RespValue::BulkString(Some("item2".to_string())),
    ]);
    assert_eq!(response, expected);
    
    // Test hash response
    let mut hash = HashMap::new();
    hash.insert("field1".to_string(), "value1".to_string());
    hash.insert("field2".to_string(), "value2".to_string());
    let value = Value::Hash(hash);
    let response = create_response(&value);
    // Note: HashMap iteration order is not guaranteed, so we check the structure
    match response {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 4); // 2 key-value pairs = 4 items
            assert!(items.iter().all(|item| matches!(item, RespValue::BulkString(Some(_)))));
        }
        _ => panic!("Expected array response"),
    }
    
    // Test set response
    let mut set = HashSet::new();
    set.insert("member1".to_string());
    set.insert("member2".to_string());
    let value = Value::Set(set);
    let response = create_response(&value);
    // Note: HashSet iteration order is not guaranteed, so we check the structure
    match response {
        RespValue::Array(items) => {
            assert_eq!(items.len(), 2);
            assert!(items.iter().all(|item| matches!(item, RespValue::BulkString(Some(_)))));
        }
        _ => panic!("Expected array response"),
    }
    
    // Test utility response functions
    assert_eq!(create_error_response("test error"), RespValue::Error("test error".to_string()));
    assert_eq!(create_ok_response(), RespValue::SimpleString("OK".to_string()));
    assert_eq!(create_integer_response(42), RespValue::Integer(42));
    assert_eq!(create_null_response(), RespValue::BulkString(None));
}

#[allow(dead_code)]
async fn test_codec_integration() {
    let mut codec = RespCodec;
    let mut buf = BytesMut::new();
    
    // Test encoding
    let value = RespValue::SimpleString("OK".to_string());
    codec.encode(value, &mut buf).unwrap();
    assert_eq!(buf, BytesMut::from("+OK\r\n".as_bytes()));
    
    // Test encoding and decoding round trip
    let mut buf = BytesMut::new();
    let original = RespValue::Array(vec![
        RespValue::BulkString(Some("SET".to_string())),
        RespValue::BulkString(Some("key".to_string())),
        RespValue::BulkString(Some("value".to_string())),
    ]);
    
    codec.encode(original.clone(), &mut buf).unwrap();
    let decoded = codec.decode(&mut buf).unwrap();
    assert_eq!(decoded, Some(original));
}

#[test]
fn test_resp_value_methods() {
    // Test utility methods
    assert_eq!(RespValue::from_string("test"), RespValue::BulkString(Some("test".to_string())));
    assert_eq!(RespValue::from_error("test error"), RespValue::Error("test error".to_string()));
    assert_eq!(RespValue::ok(), RespValue::SimpleString("OK".to_string()));
    assert_eq!(RespValue::null(), RespValue::BulkString(None));
    
    // Test to_string method
    assert_eq!(RespValue::SimpleString("OK".to_string()).to_string(), "+OK\r\n");
    assert_eq!(RespValue::Error("ERR test".to_string()).to_string(), "-ERR test\r\n");
    assert_eq!(RespValue::Integer(123).to_string(), ":123\r\n");
    assert_eq!(RespValue::BulkString(Some("test".to_string())).to_string(), "$4\r\ntest\r\n");
    assert_eq!(RespValue::BulkString(None).to_string(), "$-1\r\n");
}

#[test]
fn test_edge_cases() {
    // Test empty array
    let mut codec = RespCodec;
    let mut buf = BytesMut::from("*0\r\n");
    let result = codec.decode(&mut buf).unwrap();
    assert_eq!(result, Some(RespValue::Array(vec![])));
    
    // Test null array
    let mut buf = BytesMut::from("*-1\r\n");
    let result = codec.decode(&mut buf).unwrap();
    assert_eq!(result, Some(RespValue::Array(vec![])));
    
    // Test empty bulk string
    let mut buf = BytesMut::from("$0\r\n\r\n");
    let result = codec.decode(&mut buf).unwrap();
    assert_eq!(result, Some(RespValue::BulkString(Some("".to_string()))));
    
    // Test command with no args
    let input = "PING";
    let command = parse_command(input).unwrap();
    assert_eq!(command.command, "PING");
    let empty_args: Vec<String> = vec![];
    assert_eq!(command.args, empty_args);
    
    // Test empty command
    let input = "";
    let result = parse_command(input);
    assert!(result.is_err());
}

#[test]
fn test_invalid_resp() {
    let mut codec = RespCodec;
    
    // Test invalid integer
    let mut buf = BytesMut::from(":invalid\r\n");
    let result = codec.decode(&mut buf);
    assert!(result.is_err());
    
    // Test invalid bulk string length
    let mut buf = BytesMut::from("$-2\r\n");
    let result = codec.decode(&mut buf);
    assert!(result.is_err());
    
    // Test incomplete bulk string
    let mut buf = BytesMut::from("$5\r\nHel");
    let result = codec.decode(&mut buf).unwrap();
    assert_eq!(result, None); // Not enough data
    
    // Test unknown RESP type
    let mut buf = BytesMut::from("?invalid\r\n");
    let result = codec.decode(&mut buf);
    assert!(result.is_err());
} 