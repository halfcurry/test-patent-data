# inspect_parquet.py
import pandas as pd
import os

def inspect_parquet_file(filepath, unique_id_column='application_number'):
    """
    Loads a Parquet file and prints its basic information, including a detailed
    analysis of duplicate rows based on a specified unique ID column.

    Args:
        filepath (str): The path to the Parquet file.
        unique_id_column (str): The column to use for identifying duplicate records.
                                 Defaults to 'application_number'.
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
        print(df.head())

        print("\n--- Column Information (Data Types and Non-Null Counts) ---")
        df.info()

        print("\n--- Unique 'decision' statuses (after filtering) ---")
        if 'decision' in df.columns:
            print(df['decision'].unique())
        else:
            print("'decision' column not found in DataFrame.")

        # --- Detailed Duplicate Check ---
        print(f"\n--- Detailed Duplicate Check based on '{unique_id_column}' ---")
        if unique_id_column in df.columns:
            # Calculate the frequency of each unique_id_column value
            id_counts = df[unique_id_column].value_counts()

            # Identify IDs that appear more than once (i.e., are duplicates)
            duplicate_ids = id_counts[id_counts > 1].index.tolist()

            if duplicate_ids:
                total_duplicate_entries = id_counts[id_counts > 1].sum()
                num_unique_duplicates = len(duplicate_ids)

                print(f"WARNING: Found {total_duplicate_entries} rows participating in duplicate sets.")
                print(f"There are {num_unique_duplicates} unique '{unique_id_column}' values with duplicates.")

                print("\nFrequency of each duplicate ID (ID: Count):")
                for _id in duplicate_ids:
                    print(f"- {_id}: {id_counts[_id]}")

                print("\nDisplaying all rows for the first 5 duplicate IDs (if any):")
                # Iterate through duplicate IDs and show all rows for each
                for i, dup_id in enumerate(duplicate_ids):
                    if i >= 5: # Limit to first 5 duplicate IDs for display
                        print(f"...and {num_unique_duplicates - 5} more unique duplicate IDs not shown.")
                        break
                    print(f"\n--- Rows for '{unique_id_column}': {dup_id} ---")
                    # Select all rows where the unique_id_column matches the duplicate ID
                    print(df[df[unique_id_column] == dup_id])
            else:
                print(f"No duplicate records found based on '{unique_id_column}'.")
        else:
            print(f"'{unique_id_column}' column not found in DataFrame. Cannot check for duplicates.")

    except Exception as e:
        print(f"An error occurred while reading or inspecting the Parquet file: {e}")

if __name__ == "__main__":
    parquet_file_path = "hupd_2018_processed.parquet"
    inspect_parquet_file(parquet_file_path)