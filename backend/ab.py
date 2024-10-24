import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get environment variables
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# Database configuration
db_config = {
    "host": DB_HOST,  # MySQL host
    "user": DB_USER,  # MySQL username
    "password": DB_PASSWORD,  # MySQL password
    "database": DB_NAME,  # MySQL database name
}

# Example usage with a MySQL connector (e.g., mysql-connector-python)
import mysql.connector

try:
    connection = mysql.connector.connect(**db_config)
    if connection.is_connected():
        print("Connected to MySQL database")
except mysql.connector.Error as err:
    print(f"Error: {err}")
finally:
    if connection.is_connected():
        connection.close()
        print("MySQL connection is closed")