#!/usr/bin/env python3
"""
Simple test script for IRONKV server
"""

import socket
import time

def send_command(command):
    """Send a command to IRONKV server"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 6379))
        sock.send(command.encode('utf-8'))
        response = sock.recv(1024).decode('utf-8')
        sock.close()
        return response
    except Exception as e:
        return f"Error: {e}"

def main():
    print("🚀 Testing IRONKV Server")
    print("=" * 40)
    
    # Test basic operations
    commands = [
        "SET test_key test_value\r\n",
        "GET test_key\r\n",
        "SET user:123 '{\"name\": \"John\", \"age\": 30}'\r\n",
        "GET user:123\r\n",
        "EXISTS test_key\r\n",
        "DEL test_key\r\n",
        "GET test_key\r\n",
        "PING\r\n",
        "QUIT\r\n"
    ]
    
    for cmd in commands:
        print(f"Command: {cmd.strip()}")
        response = send_command(cmd)
        print(f"Response: {response.strip()}")
        print("-" * 20)
        time.sleep(0.1)

if __name__ == "__main__":
    main() 