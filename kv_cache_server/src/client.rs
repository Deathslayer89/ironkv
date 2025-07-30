use std::error::Error;
use std::io::{self, Write};
use tokio::io::{AsyncWriteExt, AsyncReadExt, BufReader};
use tokio::net::TcpStream;

pub async fn run_client() -> Result<(), Box<dyn Error>> {
    println!("Connecting to IRONKV server...");
    
    let mut stream = TcpStream::connect("127.0.0.1:6379").await?;
    println!("Connected! Type commands (or 'QUIT' to exit):");
    
    let (reader, mut writer) = stream.split();
    let mut reader = BufReader::new(reader);
    let mut line = String::new();
    
    loop {
        print!("IRONKV> ");
        io::stdout().flush()?;
        
        line.clear();
        let bytes_read = std::io::stdin().read_line(&mut line)?;
        
        if bytes_read == 0 {
            break;
        }
        
        let command = line.trim();
        if command.is_empty() {
            continue;
        }
        
        if command.to_uppercase() == "QUIT" {
            break;
        }
        
        // Send command to server
        let command_with_newline = format!("{}\r\n", command);
        writer.write_all(command_with_newline.as_bytes()).await?;
        writer.flush().await?;
        
        // Read response
        let mut response = String::new();
        let mut buffer = [0; 1024];
        let n = reader.read(&mut buffer).await?;
        response.push_str(&String::from_utf8_lossy(&buffer[..n]));
        
        // Parse and display response
        display_response(&response);
    }
    
    println!("Goodbye!");
    Ok(())
}

fn display_response(response: &str) {
    // Simple RESP protocol parsing for display
    if response.starts_with("+") {
        // Simple string
        println!("{}", &response[1..response.len()-2]);
    } else if response.starts_with(":") {
        // Integer
        println!("{}", &response[1..response.len()-2]);
    } else if response.starts_with("$") {
        // Bulk string
        if response.starts_with("$-1") {
            println!("(nil)");
        } else {
            // Find the first \r\n to get length
            if let Some(pos) = response.find("\r\n") {
                if let Ok(len) = response[1..pos].parse::<usize>() {
                    if len > 0 {
                        // Extract the actual string content
                        let content_start = pos + 2;
                        let content_end = content_start + len;
                        if content_end < response.len() {
                            println!("{}", &response[content_start..content_end]);
                        }
                    } else {
                        println!("(empty string)");
                    }
                }
            }
        }
    } else if response.starts_with("*") {
        // Array
        if response.starts_with("*-1") {
            println!("(nil)");
        } else {
            // Find the first \r\n to get array length
            if let Some(pos) = response.find("\r\n") {
                if let Ok(len) = response[1..pos].parse::<usize>() {
                    println!("(array with {} elements)", len);
                    // For simplicity, just show the raw response for arrays
                    print!("{}", response);
                }
            }
        }
    } else if response.starts_with("-") {
        // Error
        println!("ERROR: {}", &response[1..response.len()-2]);
    } else {
        // Unknown format, print as-is
        print!("{}", response);
    }
} 