import os
import pandas as pd
import sqlite3
import re
from datetime import datetime
import glob
import shutil

# --- Configuration ---
DATA_LAKE_PATH = './simulated_data_lake_cust_mstr'
DATABASE_NAME = ':memory:' # Use ':memory:' for an in-memory SQLite database, or 'your_database.db' for a file-based one.

# --- Helper Functions ---

def create_dummy_cust_mstr_files(base_path):
    """
    Creates dummy CUST_MSTR CSV files in the simulated data lake for demonstration.
    """
    if os.path.exists(base_path):
        shutil.rmtree(base_path) # Clean up previous run
    os.makedirs(base_path, exist_ok=True)
    print(f"Created simulated data lake for CUST_MSTR at: {os.path.abspath(base_path)}")

    # CUST_MSTR files
    cust_mstr_data_1 = pd.DataFrame({
        'CustomerID': [1, 2, 3],
        'Name': ['Alice', 'Bob', 'Charlie'],
        'City': ['NY', 'LA', 'Chicago']
    })
    cust_mstr_data_2 = pd.DataFrame({
        'CustomerID': [4, 5],
        'Name': ['David', 'Eve'],
        'City': ['Houston', 'Phoenix']
    })
    cust_mstr_data_1.to_csv(os.path.join(base_path, 'CUST_MSTR_20230101.csv'), index=False)
    cust_mstr_data_2.to_csv(os.path.join(base_path, 'CUST_MSTR_20230102.csv'), index=False)
    print("  - Generated CUST_MSTR_20230101.csv and CUST_MSTR_20230102.csv")

def setup_cust_mstr_database(conn):
    """
    Sets up the SQLite database with the CUST_MSTR table.
    """
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS CUST_MSTR (
            CustomerID INTEGER,
            Name TEXT,
            City TEXT,
            FileDate TEXT
        )
    ''')
    conn.commit()
    print("Database table CUST_MSTR created/ensured.")

# --- Processing Function ---

def process_cust_mstr_files(data_lake_path, conn):
    """
    Processes CUST_MSTR files: extracts date, adds column, truncates, and loads.
    """
    print("\n--- Processing CUST_MSTR files ---")
    all_cust_mstr_dfs = []
    file_pattern = os.path.join(data_lake_path, 'CUST_MSTR_*.csv')
    files_found = glob.glob(file_pattern)

    if not files_found:
        print(f"No CUST_MSTR files found matching '{file_pattern}'.")
        return

    for file_path in files_found:
        try:
            # Extract date from filename (e.g., CUST_MSTR_20191112.csv -> 20191112)
            match = re.search(r'CUST_MSTR_(\d{8})\.csv', os.path.basename(file_path))
            if match:
                date_str = match.group(1)
                formatted_date = datetime.strptime(date_str, '%Y%m%d').strftime('%Y-%m-%d')

                df = pd.read_csv(file_path)
                df['FileDate'] = formatted_date
                all_cust_mstr_dfs.append(df)
                print(f"  - Processed {os.path.basename(file_path)}, added FileDate: {formatted_date}")
            else:
                print(f"  - Skipping {os.path.basename(file_path)}: Date pattern not found in filename.")
        except Exception as e:
            print(f"  - Error processing {os.path.basename(file_path)}: {e}")

    if all_cust_mstr_dfs:
        combined_df = pd.concat(all_cust_mstr_dfs, ignore_index=True)
        print(f"Combined {len(all_cust_mstr_dfs)} CUST_MSTR files. Total rows: {len(combined_df)}")

        # Truncate and Load
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM CUST_MSTR;") # Truncate equivalent in SQLite
            conn.commit()
            print("  - Truncated CUST_MSTR table.")

            combined_df.to_sql('CUST_MSTR', conn, if_exists='append', index=False)
            conn.commit()
            print(f"  - Loaded {len(combined_df)} rows into CUST_MSTR table.")
        except Exception as e:
            print(f"  - Error during CUST_MSTR database load: {e}")
    else:
        print("No valid CUST_MSTR data to load.")

# --- Main Execution ---

def run_cust_mstr_processor():
    """
    Orchestrates the CUST_MSTR data loading process.
    """
    print("--- Starting CUST_MSTR Data Load Process ---")

    # 1. Create dummy files for simulation
    create_dummy_cust_mstr_files(DATA_LAKE_PATH)

    # 2. Connect to database
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        print(f"Connected to database: {DATABASE_NAME}")
        setup_cust_mstr_database(conn)

        # 3. Process CUST_MSTR files
        process_cust_mstr_files(DATA_LAKE_PATH, conn)

        print("\n--- CUST_MSTR Data Load Process Completed Successfully ---")

        # --- Verification (Optional) ---
        print("\n--- Verifying Loaded Data in CUST_MSTR Table ---")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM CUST_MSTR;")
        for row in cursor.fetchall():
            print(row)

    except Exception as e:
        print(f"\n--- An error occurred during the CUST_MSTR data load: {e} ---")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")
        # Clean up the simulated data lake after execution
        if os.path.exists(DATA_LAKE_PATH):
            shutil.rmtree(DATA_LAKE_PATH)
            print(f"Cleaned up simulated data lake at: {os.path.abspath(DATA_LAKE_PATH)}")


if __name__ == "__main__":
    run_cust_mstr_processor()