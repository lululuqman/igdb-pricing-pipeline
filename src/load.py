import psycopg2
import os
from dotenv import load_dotenv
from io import StringIO
import pandas as pd

load_dotenv()

# Database credentials
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASS = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")

TABLE_NAME = "game_data"
CSV_PATH = "data/clean_games_data.csv"

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    print("-> Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        host=DB_HOST,
        port=DB_PORT
    )
    print("-> Connection successful.")
    return conn

def create_table_if_not_exists(conn):
    """Creates the target table with the correct schema."""
    cursor = conn.cursor()
    # Define the SQL schema matching your clean_games_data.csv columns
    create_table_command = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id INT PRIMARY KEY,
        game_name VARCHAR(255) NOT NULL,
        release_date DATE,
        genres TEXT,
        avg_rating DECIMAL(4, 2),
        base_price_usd DECIMAL(6, 2),
        base_price_myr DECIMAL(6, 2),
        load_timestamp TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
    );
    """
    cursor.execute(create_table_command)
    conn.commit()
    print(f"-> Table '{TABLE_NAME}' created or already exists.")
    cursor.close()

def load_data_using_copy(conn):
    """
    Uses the highly efficient COPY command to bulk load data from the CSV file.
    This is the standard best practice for fast data ingestion in PostgreSQL.
    """
    print(f"-> Starting bulk load into {TABLE_NAME}...")
    cursor = conn.cursor()
    
    # Use Pandas to read the CSV into memory as a file-like object for copy_expert
    df = pd.read_csv(CSV_PATH)

    # Use StringIO to create a file-like object in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=True)
    csv_buffer.seek(0)
    
    # The COPY command format must match the CSV and table structure
    copy_sql = f"""
        COPY {TABLE_NAME} FROM STDIN WITH CSV HEADER DELIMITER AS ','
    """
    
    # Execute the COPY command
    cursor.copy_expert(sql=copy_sql, file=csv_buffer)
    conn.commit()
    
    print(f"✅ Successfully loaded {len(df)} rows into {TABLE_NAME}.")
    cursor.close()


def run_load_pipeline():
    """Orchestrates the connection and data loading process."""
    conn = None
    try:
        conn = get_db_connection()
        create_table_if_not_exists(conn)
        load_data_using_copy(conn)
        
    except psycopg2.Error as e:
        print(f"❌ Database Error: {e}")
        if conn:
            conn.rollback()
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")
    finally:
        if conn:
            conn.close()
            print("-> Database connection closed.")
        
if __name__ == "__main__":
    run_load_pipeline()