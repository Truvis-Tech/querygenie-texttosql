#!/usr/bin/env python3
"""
Cloud SQL Connection Test Script
This script tests connection to Google Cloud SQL using the Cloud SQL Connector.
"""

import os
import sys
import time
from datetime import datetime
from google.cloud.sql.connector import Connector
import sqlalchemy

# Configuration - Update these values or set as environment variables
CONFIG = {
    'INSTANCE_CONNECTION_NAME': os.getenv('INSTANCE_CONNECTION_NAME', 'your-project:your-region:your-instance'),
    'DB_USER': os.getenv('DB_USER', 'your-username'),
    'DB_PASSWORD': os.getenv('DB_PASSWORD', 'your-password'),
    'DB_NAME': os.getenv('DB_NAME', 'your-database'),
    'DRIVER': os.getenv('DB_DRIVER', 'pg8000'),  # or 'psycopg2'
    'USE_IAM_AUTH': os.getenv('USE_IAM_AUTH', 'false').lower() == 'true'
}

def print_header(title):
    """Print a formatted header."""
    print("\n" + "="*60)
    print(f" {title}")
    print("="*60)

def print_status(message, status="INFO"):
    """Print a status message with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{status}] {message}")

def check_prerequisites():
    """Check if all prerequisites are met."""
    print_header("CHECKING PREREQUISITES")
    
    # Check if required libraries are installed
    try:
        from google.cloud.sql.connector import Connector
        print_status("✅ Cloud SQL Connector is installed")
    except ImportError:
        print_status("❌ Cloud SQL Connector not found. Install with: pip install cloud-sql-python-connector", "ERROR")
        return False
    
    # Check database driver
    driver = CONFIG['DRIVER']
    try:
        if driver == 'pg8000':
            import pg8000
            print_status("✅ pg8000 driver is available")
        elif driver == 'psycopg2':
            import psycopg2
            print_status("✅ psycopg2 driver is available")
        else:
            print_status(f"❌ Unknown driver: {driver}", "ERROR")
            return False
    except ImportError:
        print_status(f"❌ {driver} driver not found. Install with: pip install {driver}", "ERROR")
        return False
    
    # Check configuration
    missing_configs = []
    for key, value in CONFIG.items():
        if key == 'USE_IAM_AUTH':
            continue
        if not value or value.startswith('your-'):
            missing_configs.append(key)
    
    if missing_configs:
        print_status(f"❌ Missing configuration: {', '.join(missing_configs)}", "ERROR")
        print_status("Please update the CONFIG dictionary or set environment variables", "INFO")
        return False
    
    print_status("✅ All prerequisites met")
    return True

def test_basic_connection():
    """Test basic connection to Cloud SQL."""
    print_header("TESTING BASIC CONNECTION")
    
    connector = Connector()
    
    try:
        print_status(f"Attempting to connect to: {CONFIG['INSTANCE_CONNECTION_NAME']}")
        print_status(f"Using driver: {CONFIG['DRIVER']}")
        print_status(f"Database user: {CONFIG['DB_USER']}")
        print_status(f"Database name: {CONFIG['DB_NAME']}")
        print_status(f"IAM Authentication: {CONFIG['USE_IAM_AUTH']}")
        
        # Create connection
        conn_params = {
            "instance_connection_string": CONFIG['INSTANCE_CONNECTION_NAME'],
            "driver": CONFIG['DRIVER'],
            "user": CONFIG['DB_USER'],
            "db": CONFIG['DB_NAME']
        }
        
        if CONFIG['USE_IAM_AUTH']:
            conn_params["enable_iam_auth"] = True
            print_status("Using IAM authentication")
        else:
            conn_params["password"] = CONFIG['DB_PASSWORD']
            print_status("Using password authentication")
        
        conn = connector.connect(**conn_params)
        print_status("✅ Successfully connected to Cloud SQL!")
        
        # Test basic query
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        version = cursor.fetchone()
        print_status(f"PostgreSQL version: {version[0]}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print_status(f"❌ Connection failed: {str(e)}", "ERROR")
        return False
    finally:
        connector.close()

def test_sqlalchemy_connection():
    """Test connection using SQLAlchemy."""
    print_header("TESTING SQLALCHEMY CONNECTION")
    
    connector = Connector()
    
    try:
        def getconn():
            conn_params = {
                "instance_connection_string": CONFIG['INSTANCE_CONNECTION_NAME'],
                "driver": CONFIG['DRIVER'],
                "user": CONFIG['DB_USER'],
                "db": CONFIG['DB_NAME']
            }
            
            if CONFIG['USE_IAM_AUTH']:
                conn_params["enable_iam_auth"] = True
            else:
                conn_params["password"] = CONFIG['DB_PASSWORD']
            
            return connector.connect(**conn_params)
        
        # Create SQLAlchemy engine
        engine = sqlalchemy.create_engine(
            f"postgresql+{CONFIG['DRIVER']}://",
            creator=getconn,
        )
        
        print_status("Testing SQLAlchemy connection pool...")
        
        with engine.connect() as db_conn:
            result = db_conn.execute(sqlalchemy.text("SELECT current_database(), current_user, current_timestamp"))
            row = result.fetchone()
            print_status(f"✅ Connected to database: {row[0]}")
            print_status(f"✅ Connected as user: {row[1]}")
            print_status(f"✅ Server time: {row[2]}")
        
        return True
        
    except Exception as e:
        print_status(f"❌ SQLAlchemy connection failed: {str(e)}", "ERROR")
        return False
    finally:
        connector.close()

def test_connection_performance():
    """Test connection performance."""
    print_header("TESTING CONNECTION PERFORMANCE")
    
    connector = Connector()
    
    try:
        connection_times = []
        num_tests = 5
        
        for i in range(num_tests):
            start_time = time.time()
            
            conn_params = {
                "instance_connection_string": CONFIG['INSTANCE_CONNECTION_NAME'],
                "driver": CONFIG['DRIVER'],
                "user": CONFIG['DB_USER'],
                "db": CONFIG['DB_NAME']
            }
            
            if CONFIG['USE_IAM_AUTH']:
                conn_params["enable_iam_auth"] = True
            else:
                conn_params["password"] = CONFIG['DB_PASSWORD']
            
            conn = connector.connect(**conn_params)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            
            end_time = time.time()
            connection_time = end_time - start_time
            connection_times.append(connection_time)
            
            print_status(f"Connection {i+1}: {connection_time:.3f} seconds")
        
        avg_time = sum(connection_times) / len(connection_times)
        min_time = min(connection_times)
        max_time = max(connection_times)
        
        print_status(f"Average connection time: {avg_time:.3f} seconds")
        print_status(f"Fastest connection: {min_time:.3f} seconds")
        print_status(f"Slowest connection: {max_time:.3f} seconds")
        
        return True
        
    except Exception as e:
        print_status(f"❌ Performance test failed: {str(e)}", "ERROR")
        return False
    finally:
        connector.close()

def test_database_operations():
    """Test basic database operations."""
    print_header("TESTING DATABASE OPERATIONS")
    
    connector = Connector()
    
    try:
        conn_params = {
            "instance_connection_string": CONFIG['INSTANCE_CONNECTION_NAME'],
            "driver": CONFIG['DRIVER'],
            "user": CONFIG['DB_USER'],
            "db": CONFIG['DB_NAME']
        }
        
        if CONFIG['USE_IAM_AUTH']:
            conn_params["enable_iam_auth"] = True
        else:
            conn_params["password"] = CONFIG['DB_PASSWORD']
        
        conn = connector.connect(**conn_params)
        cursor = conn.cursor()
        
        # Test CREATE TABLE
        print_status("Testing table creation...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_connection (
                id SERIAL PRIMARY KEY,
                test_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print_status("✅ Table creation successful")
        
        # Test INSERT
        print_status("Testing data insertion...")
        cursor.execute(
            "INSERT INTO test_connection (test_data) VALUES (%s)",
            (f"Test data from {datetime.now()}",)
        )
        print_status("✅ Data insertion successful")
        
        # Test SELECT
        print_status("Testing data selection...")
        cursor.execute("SELECT COUNT(*) FROM test_connection")
        count = cursor.fetchone()[0]
        print_status(f"✅ Found {count} rows in test table")
        
        # Test UPDATE
        print_status("Testing data update...")
        cursor.execute(
            "UPDATE test_connection SET test_data = %s WHERE id = (SELECT MAX(id) FROM test_connection)",
            ("Updated test data",)
        )
        print_status("✅ Data update successful")
        
        # Test DELETE (clean up)
        print_status("Cleaning up test data...")
        cursor.execute("DELETE FROM test_connection WHERE test_data = %s", ("Updated test data",))
        print_status("✅ Data cleanup successful")
        
        # Commit changes
        conn.commit()
        cursor.close()
        conn.close()
        
        return True
        
    except Exception as e:
        print_status(f"❌ Database operations test failed: {str(e)}", "ERROR")
        return False
    finally:
        connector.close()

def print_summary(results):
    """Print test summary."""
    print_header("TEST SUMMARY")
    
    total_tests = len(results)
    passed_tests = sum(results.values())
    
    print_status(f"Total tests: {total_tests}")
    print_status(f"Passed: {passed_tests}")
    print_status(f"Failed: {total_tests - passed_tests}")
    
    for test_name, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print_status(f"{test_name}: {status}")
    
    if passed_tests == total_tests:
        print_status("🎉 ALL TESTS PASSED!", "SUCCESS")
    else:
        print_status("⚠️  SOME TESTS FAILED", "WARNING")

def main():
    """Main function to run all tests."""
    print_header("CLOUD SQL CONNECTION TEST SUITE")
    print_status("Starting Cloud SQL connection tests...")
    
    # Check prerequisites first
    if not check_prerequisites():
        print_status("Prerequisites check failed. Exiting.", "ERROR")
        sys.exit(1)
    
    # Run all tests
    results = {}
    
    results["Basic Connection"] = test_basic_connection()
    results["SQLAlchemy Connection"] = test_sqlalchemy_connection()
    results["Connection Performance"] = test_connection_performance()
    results["Database Operations"] = test_database_operations()
    
    # Print summary
    print_summary(results)
    
    # Exit with appropriate code
    if all(results.values()):
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
