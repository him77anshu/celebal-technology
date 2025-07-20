import os
import pandas as pd
import sqlite3
import re
from datetime import datetime
import glob
import shutil

# --- Configuration ---
DATA_LAKE_PATH = './simulated_data_lake_master_child'
DATABASE_NAME = ':memory:' # Use ':memory:' for an in-memory SQLite database, or 'your_database.db' for a file-based one.

# --- Helper Functions ---

def create_dummy_master_child_files(base_path):
    """
    Creates dummy master_child_export CSV files in the simulated data lake for demonstration.
    """
    if os.path.exists(base_path):
        shutil.rmtree(base_path) # Clean up previous run
    os.makedirs(base_path, exist_ok=True)
    print(f"Created simulated data lake for master_child_export at: {os.path.abspath(base_path)}")

    # master_child_export files
    master_child_data_1 = pd.DataFrame({
        'MasterID': [101, 102],
        'ChildName': ['ChildA', 'ChildB'],
        'Value': [100, 200]
    })
    master_child_data_2 = pd.DataFrame({
        'MasterID': [103],
        'ChildName': ['ChildC'],
        'Value': [300]
    })
    master_child_data_1.to_csv(os.path.join(base_path, 'master_child_export-20230101.csv'), index=False)
    master_child_data_2.to_csv(os.path.join(base_path, 'master_child_export-20230102.csv'), index=False)
    print("  - Generated master_child_export-20230101.csv and master_child_export-20230102.csv")

def setup_master_child_database(conn):
    """
    Sets up the SQLite database with the master_child table.
    """
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS master_child (
            MasterID INTEGER,
            ChildName TEXT,
            Value REAL,
            FileDate TEXT,
            FileDateKey TEXT
        )
    ''')
    conn.commit()
    print("Database table master_child created/ensured.")

# --- Processing Function ---

def process_master_child_export_files(data_lake_path, conn):
    """
    Processes master_child_export files: extracts date/datekey, adds columns, truncates, and loads.
    """
    print("\n--- Processing master_child_export files ---")
    all_master_child_dfs = []
    file_pattern = os.path.join(data_lake_path, 'master_child_export-*.csv')
    files_found = glob.glob(file_pattern)

    if not files_found:
        print(f"No master_child_export files found matching '{file_pattern}'.")
        return

    for file_path in files_found:
        try:
            # Extract date from filename (e.g., master_child_export-20191112.csv -> 20191112)
            match = re.search(r'master_child_export-(\d{8})\.csv', os.path.basename(file_path))
            if match:
                date_key_str = match.group(1)
                formatted_date = datetime.strptime(date_key_str, '%Y%m%d').strftime('%Y-%m-%d')

                df = pd.read_csv(file_path)
                df['FileDate'] = formatted_date
                df['FileDateKey'] = date_key_str
                all_master_child_dfs.append(df)
                print(f"  - Processed {os.path.basename(file_path)}, added FileDate: {formatted_date}, FileDateKey: {date_key_str}")
            else:
                print(f"  - Skipping {os.path.basename(file_path)}: Date pattern not found in filename.")
        except Exception as e:
            print(f"  - Error processing {os.path.basename(file_path)}: {e}")

    if all_master_child_dfs:
        combined_df = pd.concat(all_master_child_dfs, ignore_index=True)
        print(f"Combined {len(all_master_child_dfs)} master_child_export files. Total rows: {len(combined_df)}")

        # Truncate and Load
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM master_child;") # Truncate equivalent
            conn.commit()
            print("  - Truncated master_child table.")

            combined_df.to_sql('master_child', conn, if_exists='append', index=False)
            conn.commit()
            print(f"  - Loaded {len(combined_df)} rows into master_child table.")
        except Exception as e:
            print(f"  - Error during master_child database load: {e}")
    else:
        print("No valid master_child_export data to load.")

# --- Main Execution ---

def run_master_child_processor():
    """
    Orchestrates the master_child_export data loading process.
    """
    print("--- Starting master_child_export Data Load Process ---")

    # 1. Create dummy files for simulation
    create_dummy_master_child_files(DATA_LAKE_PATH)

    # 2. Connect to database
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        print(f"Connected to database: {DATABASE_NAME}")
        setup_master_child_database(conn)

        # 3. Process master_child_export files
        process_master_child_export_files(DATA_LAKE_PATH, conn)

        print("\n--- master_child_export Data Load Process Completed Successfully ---")

        # --- Verification (Optional) ---
        print("\n--- Verifying Loaded Data in master_child Table ---")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM master_child;")
        for row in cursor.fetchall():
            print(row)

    except Exception as e:
        print(f"\n--- An error occurred during the master_child_export data load: {e} ---")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")
        # Clean up the simulated data lake after execution
        if os.path.exists(DATA_LAKE_PATH):
            shutil.rmtree(DATA_LAKE_PATH)
            print(f"Cleaned up simulated data lake at: {os.path.abspath(DATA_LAKE_PATH)}")


if __name__ == "__main__":
    run_master_child_processor()