import sqlite3
import os
import logging
import datetime
from main import db_file

# Global connection and cursor that will be used across functions
conn = None
cursor = None

# Setup logging 100% vibe coded
def setup_logging(log_file="db_errors.log"):

    log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(log_file)
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir)
    
    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(log_formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    
    # Remove existing handlers to avoid duplicates if setup_logging is called multiple times
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)
    
    return root_logger

# Initialize logger
logger = setup_logging(os.path.join("logs", f"db_errors_{datetime.datetime.now().strftime('%Y%m%d')}.log"))

def create_database(db_file):
    # Check if database file already exists
    if os.path.exists(db_file):
        logger.info(f"Database already exists: {db_file}")
        return True
    try:
        connection = sqlite3.connect(db_file)
        connection.close()
        logger.info(f"Database created successfully: {db_file}")
        return True
    except sqlite3.Error as e:
        logger.error(f"Error creating database: {e}")
        return False

def init_dbconnection(db_file):
    global conn, cursor
    try:
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        logger.info(f"Successfully connected to database: {db_file}")
        return True
    except sqlite3.Error as e:
        logger.error(f"Error connecting to database: {e}")
        return False

def close_dbconnection(db_file):
    global conn, cursor
    if conn:
        try:
            conn.close()
            conn = None
            cursor = None
            logger.info(f"Database connection for {db_file} closed successfully")
            return True
        except sqlite3.Error as e:
            logger.error(f"Error closing database connection: {e}")
    return False

def create_dbtable(table_name, columns):
    global conn, cursor
    if not conn or not cursor:
        logger.error("Database connection not initialized")
        logger.error(f"Error creating table {table_name}: {e}")
        logger.error(f"Attempting to initialize database connection ...")
        if not init_dbconnection(db_file):
            logger.error(f"Error initializing database connection: {e} from create_dbtable function")
            logger.error(f"Exiting program ...")
            exit()
    try:
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns})"
        cursor.execute(query)
        conn.commit()
        logger.info(f"Table created successfully: {table_name}")
        return True
    except sqlite3.Error as e:
        logger.error(f"Error creating table {table_name}: {e}")
        return False

def insert_dbrecord(table_name, column_values):
   
    columns = ', '.join(column_values.keys())
    placeholders = ', '.join(['?' for _ in column_values])
    values = tuple(column_values.values())
    
    query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    try:
        cursor.execute(query, values)
        conn.commit()
        last_row_id = cursor.lastrowid
        logger.info(f"Record inserted successfully into {table_name} with ID: {last_row_id}")
        return last_row_id
    except sqlite3.Error as e:
        logger.error(f"Error inserting into {table_name}: {e}")
        return None

"""
def update_dbrecord(table_name, column_values, condition, condition_params):
    global conn, cursor
    if not conn or not cursor:
        logger.error("Database connection not initialized")
        return None
    
    set_clause = ', '.join([f"{column} = ?" for column in column_values.keys()])
    values = tuple(column_values.values()) + condition_params
    
    query = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"
    
    try:
        cursor.execute(query, values)
        conn.commit()
        rows_affected = cursor.rowcount
        logger.info(f"Updated {rows_affected} record(s) in {table_name}")
        return rows_affected
    except sqlite3.Error as e:
        logger.error(f"Error updating {table_name}: {e}")
        return None
"""

def delet_dbrecord(table_name, condition, params):
    """
    Delete records from the specified table
    :param table_name: name of the table
    :param condition: WHERE clause for the delete
    :param params: parameters for the condition
    :return: Number of rows deleted or None if error
    """
    global conn, cursor
    if not conn or not cursor:
        logger.error("Database connection not initialized")
        return None
    
    query = f"DELETE FROM {table_name} WHERE {condition}"
    
    try:
        cursor.execute(query, params)
        conn.commit()
        rows_affected = cursor.rowcount
        logger.info(f"Deleted {rows_affected} record(s) from {table_name}")
        return rows_affected
    except sqlite3.Error as e:
        logger.error(f"Error deleting from {table_name}: {e}")
        return None

def execute_query(query, params=()):
    """
    Execute a query with optional parameters
    :param query: SQL query to execute
    :param params: Parameters for the query (tuple)
    :return: True if successful, False otherwise
    """
    global conn, cursor
    if not conn or not cursor:
        logger.error("Database connection not initialized")
        return False
    
    try:
        cursor.execute(query, params)
        conn.commit()
        logger.info("Query executed successfully")
        return True
    except sqlite3.Error as e:
        logger.error(f"Error executing query: {e}")
        return False

def fetch_all(query, params=()):
    """
    Execute a SELECT query and fetch all results
    :param query: SQL query to execute
    :param params: Parameters for the query (tuple)
    :return: List of rows or None if error
    """
    global conn, cursor
    if not conn or not cursor:
        logger.error("Database connection not initialized")
        return None
    
    try:
        cursor.execute(query, params)
        results = cursor.fetchall()
        logger.info(f"Fetched {len(results)} result(s)")
        return results
    except sqlite3.Error as e:
        logger.error(f"Error fetching data: {e}")
        return None

def fetch_one(query, params=()):
    """
    Execute a SELECT query and fetch one result
    :param query: SQL query to execute
    :param params: Parameters for the query (tuple)
    :return: A single row or None if error/no result
    """
    global conn, cursor
    if not conn or not cursor:
        logger.error("Database connection not initialized")
        return None
    
    try:
        cursor.execute(query, params)
        result = cursor.fetchone()
        if result:
            logger.info("Successfully fetched one result")
        else:
            logger.info("No result found")
        return result
    except sqlite3.Error as e:
        logger.error(f"Error fetching data: {e}")
        return None

def table_exists(table_name):
    """
    Check if a table exists in the database
    :param table_name: name of the table
    :return: True if the table exists, False otherwise
    """
    global conn, cursor
    if not conn or not cursor:
        logger.error("Database connection not initialized")
        return False
    
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
    result = fetch_one(query, (table_name,))
    if result:
        logger.info(f"Table '{table_name}' exists")
    else:
        logger.info(f"Table '{table_name}' does not exist")
    return result is not None
