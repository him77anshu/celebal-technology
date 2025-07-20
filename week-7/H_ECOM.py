import os
import pandas as pd
import sqlite3
import glob
import shutil

# --- Configuration ---
DATA_LAKE_PATH = './simulated_data_lake_h_ecom_order'
DATABASE_NAME = ':memory:' # Use ':memory:' for an in-memory SQLite database, or 'your_database.db' for a file-based one.

# --- Helper Functions ---

def create_dummy_h_ecom_order_files(base_path):
    """
    Creates dummy H_ECOM_ORDER CSV files in the simulated data lake for demonstration.
    """
    if os.path.exists(base_path):
        shutil.rmtree(base_path) # Clean up previous run
    os.makedirs(base_path, exist_ok=True)
    print(f"Created simulated data lake for H_ECOM_ORDER at: {os.path.abspath(base_path)}")

    # H_ECOM_ORDER files
    ecom_order_data = pd.DataFrame({
        'OrderID': ['ORD001', 'ORD002'],
        'Product': ['Laptop', 'Mouse'],
        'Quantity': [1, 2],
        'Price': [1200.00, 25.00]
    })
    ecom_order_data.to_csv(os.path.join(base_path, 'H_ECOM_ORDER.csv'), index=False)
    print("  - Generated H_ECOM_ORDER.csv")

def setup_h_ecom_order_database(conn):
    """
    Sets up the SQLite database with the H_ECOM_Orders table.
    """
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS H_ECOM_Orders (
            OrderID TEXT,
            Product TEXT,
            Quantity INTEGER,
            Price REAL
        )
    ''')
    conn.commit()
    print("Database table H_ECOM_Orders created/ensured.")

# --- Processing Function ---

def process_h_ecom_order_files(data_lake_path, conn):
    """
    Processes H_ECOM_ORDER files: loads as is, truncates, and loads.
    """
    print("\n--- Processing H_ECOM_ORDER files ---")
    all_ecom_order_dfs = []
    file_pattern = os.path.join(data_lake_path, 'H_ECOM_ORDER.csv')
    files_found = glob.glob(file_pattern)

    if not files_found:
        print(f"No H_ECOM_ORDER files found matching '{file_pattern}'.")
        return

    for file_path in files_found:
        try:
            df = pd.read_csv(file_path)
            all_ecom_order_dfs.append(df)
            print(f"  - Processed {os.path.basename(file_path)}")
        except Exception as e:
            print(f"  - Error processing {os.path.basename(file_path)}: {e}")

    if all_ecom_order_dfs:
        combined_df = pd.concat(all_ecom_order_dfs, ignore_index=True)
        print(f"Combined {len(all_ecom_order_dfs)} H_ECOM_ORDER files. Total rows: {len(combined_df)}")

        # Truncate and Load
        try:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM H_ECOM_Orders;") # Truncate equivalent
            conn.commit()
            print("  - Truncated H_ECOM_Orders table.")

            combined_df.to_sql('H_ECOM_Orders', conn, if_exists='append', index=False)
            conn.commit()
            print(f"  - Loaded {len(combined_df)} rows into H_ECOM_Orders table.")
        except Exception as e:
            print(f"  - Error during H_ECOM_Orders database load: {e}")
    else:
        print("No valid H_ECOM_ORDER data to load.")

# --- Main Execution ---

def run_h_ecom_order_processor():
    """
    Orchestrates the H_ECOM_ORDER data loading process.
    """
    print("--- Starting H_ECOM_ORDER Data Load Process ---")

    # 1. Create dummy files for simulation
    create_dummy_h_ecom_order_files(DATA_LAKE_PATH)

    # 2. Connect to database
    conn = None
    try:
        conn = sqlite3.connect(DATABASE_NAME)
        print(f"Connected to database: {DATABASE_NAME}")
        setup_h_ecom_order_database(conn)

        # 3. Process H_ECOM_ORDER files
        process_h_ecom_order_files(DATA_LAKE_PATH, conn)

        print("\n--- H_ECOM_ORDER Data Load Process Completed Successfully ---")

        # --- Verification (Optional) ---
        print("\n--- Verifying Loaded Data in H_ECOM_Orders Table ---")
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM H_ECOM_Orders;")
        for row in cursor.fetchall():
            print(row)

    except Exception as e:
        print(f"\n--- An error occurred during the H_ECOM_ORDER data load: {e} ---")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")
        # Clean up the simulated data lake after execution
        if os.path.exists(DATA_LAKE_PATH):
            shutil.rmtree(DATA_LAKE_PATH)
            print(f"Cleaned up simulated data lake at: {os.path.abspath(DATA_LAKE_PATH)}")


if __name__ == "__main__":
    run_h_ecom_order_processor()