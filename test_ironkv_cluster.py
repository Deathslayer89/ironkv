#!/usr/bin/env python3
"""
IRONKV Cluster Test Suite
Comprehensive testing for distributed key-value store with Redis protocol compatibility
"""

import socket
import time
import threading
import random
import json
import subprocess
import signal
import sys
from typing import List, Dict, Optional, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ironkv_test.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class NodeConfig:
    """Node configuration"""
    node_id: str
    redis_port: int
    cluster_port: int
    process_id: Optional[int] = None

class IRONKVClusterTester:
    """Comprehensive IRONKV cluster tester"""
    
    def __init__(self):
        self.nodes = [
            NodeConfig("node1", 6379, 6380),
            NodeConfig("node2", 6381, 6382),
            NodeConfig("node3", 6383, 6384)
        ]
        self.leader_node = None
        self.test_results = {}
        
    def send_redis_command(self, port: int, command: str) -> Tuple[bool, str]:
        """Send Redis command to specified port"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(5)
                s.connect(('localhost', port))
                s.send(f"{command}\r\n".encode())
                response = s.recv(1024).decode().strip()
                return True, response
        except Exception as e:
            return False, str(e)
    
    def find_leader(self) -> Optional[NodeConfig]:
        """Find the current leader node"""
        logger.info("🔍 Finding current leader...")
        
        for node in self.nodes:
            success, response = self.send_redis_command(node.redis_port, "SET leader_test value")
            if success and response == "+OK":
                logger.info(f"✅ Leader found: {node.node_id} (port {node.redis_port})")
                return node
        
        logger.error("❌ No leader found!")
        return None
    
    def test_basic_operations(self) -> Dict[str, bool]:
        """Test basic Redis operations"""
        logger.info("🧪 Testing basic Redis operations...")
        results = {}
        
        # Find leader
        leader = self.find_leader()
        if not leader:
            return {"basic_operations": False}
        
        # Test SET
        success, response = self.send_redis_command(leader.redis_port, "SET test_key hello")
        results["SET"] = success and response == "+OK"
        
        # Test GET
        success, response = self.send_redis_command(leader.redis_port, "GET test_key")
        results["GET"] = success and "hello" in response
        
        # Test DEL
        success, response = self.send_redis_command(leader.redis_port, "DEL test_key")
        results["DEL"] = success and response == ":1"
        
        # Test PING on all nodes
        for node in self.nodes:
            success, response = self.send_redis_command(node.redis_port, "PING")
            results[f"PING_{node.node_id}"] = success and response == "+PONG"
        
        # Test TTL
        success, response = self.send_redis_command(leader.redis_port, "SET ttl_test value EX 5")
        results["TTL_SET"] = success and response == "+OK"
        
        success, response = self.send_redis_command(leader.redis_port, "TTL ttl_test")
        results["TTL_GET"] = success and response.isdigit() and int(response) > 0
        
        all_passed = all(results.values())
        logger.info(f"Basic operations: {'✅ PASSED' if all_passed else '❌ FAILED'}")
        return results
    
    def test_leader_failover(self) -> Dict[str, bool]:
        """Test automatic leader failover"""
        logger.info("🔄 Testing leader failover...")
        results = {}
        
        # Find current leader
        original_leader = self.find_leader()
        if not original_leader:
            return {"failover": False}
        
        logger.info(f"Original leader: {original_leader.node_id}")
        
        # Kill the leader
        try:
            subprocess.run(['pkill', '-f', f'node-id {original_leader.node_id}'], 
                         check=True, capture_output=True)
            logger.info(f"Killed {original_leader.node_id}")
            results["leader_killed"] = True
        except subprocess.CalledProcessError:
            logger.error(f"Failed to kill {original_leader.node_id}")
            results["leader_killed"] = False
            return results
        
        # Wait for failover
        time.sleep(5)
        
        # Find new leader
        new_leader = self.find_leader()
        if new_leader and new_leader != original_leader:
            logger.info(f"New leader: {new_leader.node_id}")
            results["failover_success"] = True
            
            # Test write on new leader
            success, response = self.send_redis_command(new_leader.redis_port, "SET failover_test value")
            results["new_leader_write"] = success and response == "+OK"
        else:
            logger.error("Failover failed - no new leader found")
            results["failover_success"] = False
        
        # Restart the original leader
        try:
            subprocess.Popen([
                './kv_cache_server/target/release/kv_cache_server',
                '--cluster', '--node-id', original_leader.node_id,
                '--cluster-port', str(original_leader.cluster_port)
            ])
            logger.info(f"Restarted {original_leader.node_id}")
            results["leader_restart"] = True
        except Exception as e:
            logger.error(f"Failed to restart {original_leader.node_id}: {e}")
            results["leader_restart"] = False
        
        # Wait for rejoin
        time.sleep(5)
        
        # Test cluster is healthy
        all_nodes_responding = True
        for node in self.nodes:
            success, response = self.send_redis_command(node.redis_port, "PING")
            if not success or response != "+PONG":
                all_nodes_responding = False
                break
        
        results["cluster_healthy"] = all_nodes_responding
        
        all_passed = all(results.values())
        logger.info(f"Failover test: {'✅ PASSED' if all_passed else '❌ FAILED'}")
        return results
    
    def test_concurrent_operations(self, num_threads: int = 10, operations_per_thread: int = 100) -> Dict[str, bool]:
        """Test concurrent operations"""
        logger.info(f"⚡ Testing concurrent operations ({num_threads} threads, {operations_per_thread} ops each)...")
        results = {}
        
        leader = self.find_leader()
        if not leader:
            return {"concurrent_operations": False}
        
        def worker(thread_id: int):
            """Worker function for concurrent operations"""
            thread_results = []
            for i in range(operations_per_thread):
                key = f"concurrent_{thread_id}_{i}"
                value = f"value_{thread_id}_{i}"
                
                # SET operation
                success, response = self.send_redis_command(leader.redis_port, f"SET {key} {value}")
                if not success or response != "+OK":
                    thread_results.append(False)
                    continue
                
                # GET operation
                success, response = self.send_redis_command(leader.redis_port, f"GET {key}")
                if not success or value not in response:
                    thread_results.append(False)
                    continue
                
                thread_results.append(True)
            
            return thread_results
        
        # Run concurrent operations
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(worker, i) for i in range(num_threads)]
            all_results = []
            
            for future in as_completed(futures):
                all_results.extend(future.result())
        
        total_operations = len(all_results)
        successful_operations = sum(all_results)
        success_rate = successful_operations / total_operations if total_operations > 0 else 0
        
        results["concurrent_operations"] = success_rate >= 0.95  # 95% success rate threshold
        results["success_rate"] = success_rate
        
        logger.info(f"Concurrent operations: {successful_operations}/{total_operations} ({success_rate:.2%})")
        logger.info(f"Concurrent test: {'✅ PASSED' if results['concurrent_operations'] else '❌ FAILED'}")
        return results
    
    def test_data_persistence(self) -> Dict[str, bool]:
        """Test data persistence across restarts"""
        logger.info("💾 Testing data persistence...")
        results = {}
        
        leader = self.find_leader()
        if not leader:
            return {"data_persistence": False}
        
        # Set test data
        test_data = {
            "string_key": "persistent_string",
            "number_key": "42",
            "json_key": json.dumps({"test": "data", "number": 123})
        }
        
        for key, value in test_data.items():
            success, response = self.send_redis_command(leader.redis_port, f"SET {key} {value}")
            if not success or response != "+OK":
                results["data_set"] = False
                return results
        
        results["data_set"] = True
        
        # Restart the leader
        try:
            subprocess.run(['pkill', '-f', f'node-id {leader.node_id}'], 
                         check=True, capture_output=True)
            time.sleep(2)
            
            subprocess.Popen([
                './kv_cache_server/target/release/kv_cache_server',
                '--cluster', '--node-id', leader.node_id,
                '--cluster-port', str(leader.cluster_port)
            ])
            
            # Wait for restart and leader election
            time.sleep(10)
            
            # Find new leader
            new_leader = self.find_leader()
            if not new_leader:
                results["leader_restart"] = False
                return results
            
            results["leader_restart"] = True
            
            # Verify data persistence
            for key, expected_value in test_data.items():
                success, response = self.send_redis_command(new_leader.redis_port, f"GET {key}")
                if not success or expected_value not in response:
                    results["data_persistence"] = False
                    return results
            
            results["data_persistence"] = True
            
        except Exception as e:
            logger.error(f"Data persistence test failed: {e}")
            results["data_persistence"] = False
        
        all_passed = all(results.values())
        logger.info(f"Data persistence test: {'✅ PASSED' if all_passed else '❌ FAILED'}")
        return results
    
    def test_error_handling(self) -> Dict[str, bool]:
        """Test error handling and edge cases"""
        logger.info("⚠️ Testing error handling...")
        results = {}
        
        leader = self.find_leader()
        if not leader:
            return {"error_handling": False}
        
        # Test invalid commands
        invalid_commands = [
            "INVALID_COMMAND",
            "SET",
            "GET",
            "SET key",
            "GET nonexistent_key"
        ]
        
        for cmd in invalid_commands:
            success, response = self.send_redis_command(leader.redis_port, cmd)
            # Should get error response, not crash
            results[f"invalid_command_{cmd}"] = success and ("-ERR" in response or response == "$-1")
        
        # Test on follower nodes (should reject writes)
        followers = [node for node in self.nodes if node != leader]
        for follower in followers:
            success, response = self.send_redis_command(follower.redis_port, "SET follower_test value")
            results[f"follower_write_rejection_{follower.node_id}"] = success and "-ERR not leader" in response
        
        all_passed = all(results.values())
        logger.info(f"Error handling test: {'✅ PASSED' if all_passed else '❌ FAILED'}")
        return results
    
    def run_performance_benchmark(self, num_operations: int = 1000) -> Dict[str, float]:
        """Run performance benchmark"""
        logger.info(f"📊 Running performance benchmark ({num_operations} operations)...")
        
        leader = self.find_leader()
        if not leader:
            return {"benchmark": False}
        
        # SET operations benchmark
        start_time = time.time()
        successful_sets = 0
        
        for i in range(num_operations):
            success, response = self.send_redis_command(leader.redis_port, f"SET bench_key_{i} value_{i}")
            if success and response == "+OK":
                successful_sets += 1
        
        set_time = time.time() - start_time
        set_ops_per_sec = successful_sets / set_time if set_time > 0 else 0
        
        # GET operations benchmark
        start_time = time.time()
        successful_gets = 0
        
        for i in range(num_operations):
            success, response = self.send_redis_command(leader.redis_port, f"GET bench_key_{i}")
            if success and "value_" in response:
                successful_gets += 1
        
        get_time = time.time() - start_time
        get_ops_per_sec = successful_gets / get_time if get_time > 0 else 0
        
        # Cleanup
        for i in range(num_operations):
            self.send_redis_command(leader.redis_port, f"DEL bench_key_{i}")
        
        results = {
            "set_operations_per_sec": set_ops_per_sec,
            "get_operations_per_sec": get_ops_per_sec,
            "set_success_rate": successful_sets / num_operations,
            "get_success_rate": successful_gets / num_operations
        }
        
        logger.info(f"Performance benchmark:")
        logger.info(f"  SET: {set_ops_per_sec:.2f} ops/sec ({results['set_success_rate']:.2%} success)")
        logger.info(f"  GET: {get_ops_per_sec:.2f} ops/sec ({results['get_success_rate']:.2%} success)")
        
        return results
    
    def run_all_tests(self) -> Dict[str, Dict]:
        """Run all tests"""
        logger.info("🚀 Starting comprehensive IRONKV cluster test suite...")
        
        all_results = {}
        
        # Basic operations test
        all_results["basic_operations"] = self.test_basic_operations()
        
        # Concurrent operations test
        all_results["concurrent_operations"] = self.test_concurrent_operations()
        
        # Error handling test
        all_results["error_handling"] = self.test_error_handling()
        
        # Performance benchmark
        all_results["performance"] = self.run_performance_benchmark()
        
        # Leader failover test (destructive - run last)
        all_results["failover"] = self.test_leader_failover()
        
        # Data persistence test
        all_results["data_persistence"] = self.test_data_persistence()
        
        return all_results
    
    def generate_report(self, results: Dict[str, Dict]) -> str:
        """Generate test report"""
        report = []
        report.append("=" * 60)
        report.append("IRONKV CLUSTER TEST REPORT")
        report.append("=" * 60)
        report.append("")
        
        total_tests = 0
        passed_tests = 0
        
        for test_name, test_results in results.items():
            report.append(f"📋 {test_name.upper().replace('_', ' ')}")
            report.append("-" * 40)
            
            if isinstance(test_results, dict):
                for key, value in test_results.items():
                    if isinstance(value, bool):
                        status = "✅ PASS" if value else "❌ FAIL"
                        report.append(f"  {key}: {status}")
                        total_tests += 1
                        if value:
                            passed_tests += 1
                    elif isinstance(value, float):
                        report.append(f"  {key}: {value:.2f}")
                    else:
                        report.append(f"  {key}: {value}")
            else:
                report.append(f"  Result: {test_results}")
            
            report.append("")
        
        # Summary
        report.append("=" * 60)
        report.append("SUMMARY")
        report.append("=" * 60)
        report.append(f"Total Tests: {total_tests}")
        report.append(f"Passed: {passed_tests}")
        report.append(f"Failed: {total_tests - passed_tests}")
        report.append(f"Success Rate: {(passed_tests/total_tests)*100:.1f}%" if total_tests > 0 else "N/A")
        
        if passed_tests == total_tests:
            report.append("🎉 ALL TESTS PASSED! IRONKV cluster is working perfectly!")
        else:
            report.append("⚠️ Some tests failed. Check the logs for details.")
        
        return "\n".join(report)

def main():
    """Main function"""
    print("🚀 IRONKV Cluster Test Suite")
    print("=" * 40)
    
    # Check if cluster is running
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(1)
            s.connect(('localhost', 6379))
    except:
        print("❌ IRONKV cluster is not running!")
        print("Please start the cluster first:")
        print("  Terminal 1: RUST_LOG=info ./kv_cache_server/target/release/kv_cache_server --cluster --node-id node1 --cluster-port 6380")
        print("  Terminal 2: RUST_LOG=info ./kv_cache_server/target/release/kv_cache_server --cluster --node-id node2 --cluster-port 6382")
        print("  Terminal 3: RUST_LOG=info ./kv_cache_server/target/release/kv_cache_server --cluster --node-id node3 --cluster-port 6384")
        sys.exit(1)
    
    # Create tester and run tests
    tester = IRONKVClusterTester()
    
    try:
        results = tester.run_all_tests()
        report = tester.generate_report(results)
        
        # Save report to file
        with open('ironkv_test_report.txt', 'w') as f:
            f.write(report)
        
        print("\n" + report)
        print(f"\n📄 Detailed report saved to: ironkv_test_report.txt")
        print(f"📝 Logs saved to: ironkv_test.log")
        
    except KeyboardInterrupt:
        print("\n⚠️ Test interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        logger.exception("Test failed")

if __name__ == "__main__":
    main() 