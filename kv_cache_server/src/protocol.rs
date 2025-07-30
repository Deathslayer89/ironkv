use bytes::{Buf, BufMut, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};
use kv_cache_core::value::Value;

#[derive(Debug, Clone, PartialEq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>),
    Array(Vec<RespValue>),
}

#[derive(Debug, Clone)]
pub struct RespCommand {
    pub command: String,
    pub args: Vec<String>,
}

impl RespValue {
    pub fn to_string(&self) -> String {
        match self {
            RespValue::SimpleString(s) => format!("+{}\r\n", s),
            RespValue::Error(e) => format!("-{}\r\n", e),
            RespValue::Integer(i) => format!(":{}\r\n", i),
            RespValue::BulkString(Some(s)) => format!("${}\r\n{}\r\n", s.len(), s),
            RespValue::BulkString(None) => "$-1\r\n".to_string(),
            RespValue::Array(arr) => {
                let mut result = format!("*{}\r\n", arr.len());
                for item in arr {
                    result.push_str(&item.to_string());
                }
                result
            }
        }
    }

    #[allow(dead_code)]
    pub fn from_string(s: &str) -> RespValue {
        RespValue::BulkString(Some(s.to_string()))
    }

    #[allow(dead_code)]
    pub fn from_error(e: &str) -> RespValue {
        RespValue::Error(e.to_string())
    }

    #[allow(dead_code)]
    pub fn ok() -> RespValue {
        RespValue::SimpleString("OK".to_string())
    }

    #[allow(dead_code)]
    pub fn null() -> RespValue {
        RespValue::BulkString(None)
    }
}

impl RespCommand {
    #[allow(dead_code)]
    pub fn new(command: String, args: Vec<String>) -> Self {
        Self { command, args }
    }

    #[allow(dead_code)]
    pub fn from_array(array: Vec<RespValue>) -> Option<Self> {
        if array.is_empty() {
            return None;
        }

        let command = match &array[0] {
            RespValue::BulkString(Some(cmd)) => cmd.to_uppercase(),
            _ => return None,
        };

        let args: Vec<String> = array[1..]
            .iter()
            .filter_map(|arg| match arg {
                RespValue::BulkString(Some(s)) => Some(s.clone()),
                _ => None,
            })
            .collect();

        Some(RespCommand::new(command, args))
    }

    #[allow(dead_code)]
    pub fn to_array(&self) -> RespValue {
        let mut array = vec![RespValue::BulkString(Some(self.command.clone()))];
        for arg in &self.args {
            array.push(RespValue::BulkString(Some(arg.clone())));
        }
        RespValue::Array(array)
    }
}

pub struct RespCodec;

impl Decoder for RespCodec {
    type Item = RespValue;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        // Look for the first byte to determine the type
        let first_byte = src[0];
        let mut consumed = 0;

        let result = match first_byte {
            b'+' => {
                // Simple String
                if let Some(pos) = find_crlf(src, 1) {
                    let string = String::from_utf8_lossy(&src[1..pos]).to_string();
                    consumed = pos + 2;
                    Ok(Some(RespValue::SimpleString(string)))
                } else {
                    Ok(None)
                }
            }
            b'-' => {
                // Error
                if let Some(pos) = find_crlf(src, 1) {
                    let error = String::from_utf8_lossy(&src[1..pos]).to_string();
                    consumed = pos + 2;
                    Ok(Some(RespValue::Error(error)))
                } else {
                    Ok(None)
                }
            }
            b':' => {
                // Integer
                if let Some(pos) = find_crlf(src, 1) {
                    let int_str = String::from_utf8_lossy(&src[1..pos]);
                    match int_str.parse::<i64>() {
                        Ok(int_val) => {
                            consumed = pos + 2;
                            Ok(Some(RespValue::Integer(int_val)))
                        }
                        Err(_) => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid integer")),
                    }
                } else {
                    Ok(None)
                }
            }
            b'$' => {
                // Bulk String
                if let Some(len_end) = find_crlf(src, 1) {
                    let len_str = String::from_utf8_lossy(&src[1..len_end]);
                    match len_str.parse::<i64>() {
                        Ok(-1) => {
                            // Null bulk string
                            consumed = len_end + 2;
                            Ok(Some(RespValue::BulkString(None)))
                        }
                        Ok(len) if len >= 0 => {
                            let data_start = len_end + 2;
                            let data_end = data_start + len as usize;
                            let crlf_end = data_end + 2;

                            if src.len() >= crlf_end {
                                // Check if the data ends with \r\n
                                if src[data_end] == b'\r' && src[data_end + 1] == b'\n' {
                                    let string = String::from_utf8_lossy(&src[data_start..data_end]).to_string();
                                    consumed = crlf_end;
                                    Ok(Some(RespValue::BulkString(Some(string))))
                                } else {
                                    Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid bulk string format"))
                                }
                            } else {
                                Ok(None)
                            }
                        }
                        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid bulk string length")),
                    }
                } else {
                    Ok(None)
                }
            }
            b'*' => {
                // Array
                if let Some(len_end) = find_crlf(src, 1) {
                    let len_str = String::from_utf8_lossy(&src[1..len_end]);
                    match len_str.parse::<i64>() {
                        Ok(-1) => {
                            // Null array
                            consumed = len_end + 2;
                            Ok(Some(RespValue::Array(vec![])))
                        }
                        Ok(len) if len >= 0 => {
                            let mut array = Vec::new();
                            let mut pos = len_end + 2;
                            let mut temp_src = BytesMut::from(&src[pos..]);

                            for _ in 0..len {
                                let mut temp_codec = RespCodec;
                                match temp_codec.decode(&mut temp_src)? {
                                    Some(item) => {
                                        array.push(item);
                                        pos += temp_src.len();
                                    }
                                    None => {
                                        // Not enough data
                                        return Ok(None);
                                    }
                                }
                            }

                            consumed = pos;
                            Ok(Some(RespValue::Array(array)))
                        }
                        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid array length")),
                    }
                } else {
                    Ok(None)
                }
            }
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown RESP type")),
        };

        if consumed > 0 {
            src.advance(consumed);
        }

        result
    }
}

impl Encoder<RespValue> for RespCodec {
    type Error = io::Error;

    fn encode(&mut self, item: RespValue, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let encoded = item.to_string();
        dst.put_slice(encoded.as_bytes());
        Ok(())
    }
}

fn find_crlf(src: &[u8], start: usize) -> Option<usize> {
    for i in start..src.len() - 1 {
        if src[i] == b'\r' && src[i + 1] == b'\n' {
            return Some(i);
        }
    }
    None
}

#[allow(dead_code)]
pub fn parse_command(input: &str) -> Result<RespCommand, String> {
    // Simple line-based parser for backward compatibility
    let parts: Vec<&str> = input.trim().split_whitespace().collect();
    if parts.is_empty() {
        return Err("Empty command".to_string());
    }

    let command = parts[0].to_string();
    let args: Vec<String> = parts[1..].iter().map(|s| s.to_string()).collect();

    Ok(RespCommand::new(command, args))
}

#[allow(dead_code)]
pub fn create_response(value: &Value) -> RespValue {
    match value {
        Value::String(s) => RespValue::BulkString(Some(s.clone())),
        Value::List(list) => {
            let items: Vec<RespValue> = list.iter()
                .map(|item| RespValue::BulkString(Some(item.clone())))
                .collect();
            RespValue::Array(items)
        }
        Value::Hash(hash) => {
            let mut items = Vec::new();
            for (key, value) in hash {
                items.push(RespValue::BulkString(Some(key.clone())));
                items.push(RespValue::BulkString(Some(value.clone())));
            }
            RespValue::Array(items)
        }
        Value::Set(set) => {
            let items: Vec<RespValue> = set.iter()
                .map(|item| RespValue::BulkString(Some(item.clone())))
                .collect();
            RespValue::Array(items)
        }
    }
}

#[allow(dead_code)]
pub fn create_error_response(error: &str) -> RespValue {
    RespValue::Error(error.to_string())
}

#[allow(dead_code)]
pub fn create_ok_response() -> RespValue {
    RespValue::ok()
}

#[allow(dead_code)]
pub fn create_integer_response(value: i64) -> RespValue {
    RespValue::Integer(value)
}

#[allow(dead_code)]
pub fn create_null_response() -> RespValue {
    RespValue::null()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_simple_string_parsing() {
        let mut codec = RespCodec;
        let mut buf = BytesMut::from("+OK\r\n");
        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(result, Some(RespValue::SimpleString("OK".to_string())));
    }

    #[test]
    fn test_error_parsing() {
        let mut codec = RespCodec;
        let mut buf = BytesMut::from("-ERR unknown command\r\n");
        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(result, Some(RespValue::Error("ERR unknown command".to_string())));
    }

    #[test]
    fn test_integer_parsing() {
        let mut codec = RespCodec;
        let mut buf = BytesMut::from(":123\r\n");
        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(result, Some(RespValue::Integer(123)));
    }

    #[test]
    fn test_bulk_string_parsing() {
        let mut codec = RespCodec;
        let mut buf = BytesMut::from("$5\r\nHello\r\n");
        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(result, Some(RespValue::BulkString(Some("Hello".to_string()))));
    }

    #[test]
    fn test_null_bulk_string_parsing() {
        let mut codec = RespCodec;
        let mut buf = BytesMut::from("$-1\r\n");
        let result = codec.decode(&mut buf).unwrap();
        assert_eq!(result, Some(RespValue::BulkString(None)));
    }

    #[test]
    fn test_array_parsing() {
        let mut codec = RespCodec;
        let mut buf = BytesMut::from("*2\r\n$3\r\nGET\r\n$4\r\nname\r\n");
        let result = codec.decode(&mut buf).unwrap();
        let expected = RespValue::Array(vec![
            RespValue::BulkString(Some("GET".to_string())),
            RespValue::BulkString(Some("name".to_string())),
        ]);
        assert_eq!(result, Some(expected));
    }

    #[test]
    fn test_command_parsing() {
        let input = "SET key value";
        let command = parse_command(input).unwrap();
        assert_eq!(command.command, "SET");
        assert_eq!(command.args, vec!["key", "value"]);
    }

    #[test]
    fn test_command_to_array() {
        let command = RespCommand::new("SET".to_string(), vec!["key".to_string(), "value".to_string()]);
        let array = command.to_array();
        let expected = RespValue::Array(vec![
            RespValue::BulkString(Some("SET".to_string())),
            RespValue::BulkString(Some("key".to_string())),
            RespValue::BulkString(Some("value".to_string())),
        ]);
        assert_eq!(array, expected);
    }

    #[test]
    fn test_response_creation() {
        let value = Value::String("test".to_string());
        let response = create_response(&value);
        assert_eq!(response, RespValue::BulkString(Some("test".to_string())));
    }

    #[test]
    fn test_encoding() {
        let mut codec = RespCodec;
        let mut buf = BytesMut::new();
        let value = RespValue::SimpleString("OK".to_string());
        codec.encode(value, &mut buf).unwrap();
        assert_eq!(buf, BytesMut::from("+OK\r\n"));
    }
} 