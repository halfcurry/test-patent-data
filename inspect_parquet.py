# inspect_parquet.py
import pandas as pd
import os

def inspect_parquet_file(filepath):
    """
    Loads a Parquet file and prints its basic information.

    Args:
        filepath (str): The path to the Parquet file.
    """
    if not os.path.exists(filepath):
        print(f"Error: Parquet file not found at '{filepath}'")
        return

    print(f"Loading Parquet file from: {filepath}")
    try:
        df = pd.read_parquet(filepath)

        print("\n--- Parquet File Information ---")
        print(f"Number of rows: {df.shape[0]}")
        print(f"Number of columns: {df.shape[1]}")

        print("\n--- First 5 Rows of Data ---")
        # Display the first 5 rows to show the content
        print(df.head())

        print("\n--- Column Information (Data Types and Non-Null Counts) ---")
        # Display column names, non-null counts, and data types
        df.info()

        print("\n--- Unique 'decision' statuses (after filtering) ---")
        # Check unique values in the 'decision' column to confirm filtering
        if 'decision' in df.columns:
            print(df['decision'].unique())
        else:
            print("'decision' column not found in DataFrame.")

    except Exception as e:
        print(f"An error occurred while reading or inspecting the Parquet file: {e}")

if __name__ == "__main__":
    # Assuming the Parquet file is saved in the root of your workspace
    parquet_file_path = "hupd_2018_processed.parquet"
    inspect_parquet_file(parquet_file_path)