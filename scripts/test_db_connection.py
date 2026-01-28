# scripts/test_db_connection.py
from db_config import DB_CONFIG
import mysql.connector

print("Using config:", DB_CONFIG)

conn = mysql.connector.connect(**DB_CONFIG)
print("Connected OK!")
conn.close()
