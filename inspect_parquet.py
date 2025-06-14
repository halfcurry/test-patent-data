import pandas as pd
import os
import argparse
import dask.dataframe as dd # Import dask.dataframe
from dask.distributed import Client, LocalCluster, progress # For Dask client and progress bar

def inspect_parquet_file(filepath: str, unique_id_column: str = 'application_number', client: Client = None):
    """
    Loads a Parquet file using Dask and prints its basic information, including a detailed
    analysis of duplicate rows based on a specified unique ID column.
    Designed to handle files larger than memory.

    Args:
        filepath (str): The path to the Parquet file. This can be a directory containing
                        multiple Parquet parts (as Dask writes them).
        unique_id_column (str): The column to use for identifying duplicate records.
                                Defaults to 'application_number'.
        client (dask.distributed.Client, optional): An already initialized Dask client.
                                                    If None, a local client will be created.
    """
    if not os.path.exists(filepath):
        print(f"Error: Parquet file or directory not found at '{filepath}'")
        return

    print(f"Loading Parquet file(s) from: {filepath}")
    try:
        # Use dask.dataframe.read_parquet. Dask automatically handles multi-part Parquet files
        # written by ddf.to_parquet(..., compression='snappy')
        ddf = dd.read_parquet(filepath)

        print("\n--- Parquet File Information (computed lazily) ---")
        print(f"Number of Dask DataFrame partitions: {ddf.npartitions}")
        print(f"Approximate number of rows (calling .compute() for accuracy): ", end='')
        # .compute() here will trigger computation, but for just shape[0], it's often efficient
        num_rows = ddf.shape[0].compute()
        print(num_rows)
        print(f"Number of columns: {len(ddf.columns)}")

        print("\n--- First 5 Rows of Data (calling .head() triggers computation) ---")
        print(ddf.head()) # .head() brings a small portion to local memory

        print("\n--- Column Information (Data Types and Non-Null Counts) ---")
        # Dask's dtypes are similar to pandas, but info() might not be available directly.
        # We can simulate info() with .dtypes and .isnull().sum()
        print("Dask DataFrame dtypes:")
        print(ddf.dtypes)
        print("\nNon-null counts (computed lazily, will trigger computation):")
        # Computing isn't necessary for isnull().sum(), but calling it displays it.
        # Use persist() and progress() to monitor this if it's large.
        non_null_counts = (ddf.count().compute()) # .count() returns non-nulls for each col
        print(non_null_counts)


        print("\n--- Counts of 'decision' statuses ---")
        if 'decision' in ddf.columns:
            decision_counts = ddf['decision'].value_counts().compute()
            print(decision_counts)
        else:
            print("'decision' column not found in DataFrame.")

        # --- Detailed Duplicate Check using Dask ---
        print(f"\n--- Detailed Duplicate Check based on '{unique_id_column}' ---")
        if unique_id_column in ddf.columns:
            print(f"Computing value counts for '{unique_id_column}' (this will trigger a distributed computation)...")
            # Calculate the frequency of each unique_id_column value
            # This is a key operation that benefits from Dask's distributed nature.
            id_counts_dask = ddf[unique_id_column].value_counts()
            
            # Use progress to monitor the value_counts computation
            print("Monitoring 'value_counts' computation:")
            progress(id_counts_dask)
            id_counts = id_counts_dask.compute() # Bring the result to local Pandas Series

            # Identify IDs that appear more than once (i.e., are duplicates)
            duplicate_ids = id_counts[id_counts > 1].index.tolist()

            if duplicate_ids:
                total_duplicate_entries = id_counts[id_counts > 1].sum()
                num_unique_duplicates = len(duplicate_ids)

                print(f"WARNING: Found {total_duplicate_entries} rows participating in duplicate sets.")
                print(f"There are {num_unique_duplicates} unique '{unique_id_column}' values with duplicates.")

                print("\nFrequency of each duplicate ID (ID: Count):")
                # Sort for consistent output and easier readability
                for _id in sorted(duplicate_ids)[:10]: # Limit printing to first 10 duplicate IDs
                    print(f"- {_id}: {id_counts[_id]}")
                if len(duplicate_ids) > 10:
                    print(f"... showing top 10 of {num_unique_duplicates} unique duplicate IDs.")

                print("\nDisplaying first few rows for the first 5 unique duplicate IDs (if any):")
                # Iterate through duplicate IDs and show a sample of rows for each
                # WARNING: ddf.loc or ddf[...] on a specific index/value can be slow with Dask
                # if the index is not sorted/partitioned by that column.
                # For demonstration, we'll collect a few, but for many, this part can be slow.
                
                # To avoid loading all duplicate rows to memory, we'll only fetch a few
                # or rely on the previous statistics.
                
                # For showing *all* rows of duplicates, you would need to filter and then compute,
                # which could still be memory-intensive if there are many duplicate rows.
                # df[df[unique_id_column] == dup_id].compute()
                
                # Instead, we'll just advise if there are many, and show a few for example
                print("\nNote: Displaying all rows for duplicate IDs can be memory intensive for many duplicates.")
                print("Consider viewing specific duplicate IDs using a Dask query or reducing the number shown.")

                # Example of fetching just a few samples for duplicates
                for i, dup_id in enumerate(sorted(duplicate_ids)):
                    if i >= 5: # Limit to first 5 unique duplicate IDs for detailed display
                        print(f"...and {num_unique_duplicates - 5} more unique duplicate IDs not shown in detail.")
                        break
                    print(f"\n--- Sample Rows for '{unique_id_column}': {dup_id} ---")
                    # Fetch first 2 rows for this duplicate ID
                    # This is still an expensive operation for each duplicate ID if data is not indexed well
                    print(ddf[ddf[unique_id_column] == dup_id].head(2))
                    
            else:
                print(f"No duplicate records found based on '{unique_id_column}'.")
        else:
            print(f"'{unique_id_column}' column not found in DataFrame. Cannot check for duplicates.")

    except Exception as e:
        print(f"An error occurred while reading or inspecting the Parquet file: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load and inspect a Parquet file using Dask, including detailed duplicate analysis."
    )
    parser.add_argument(
        "filepath",
        type=str,
        help="The path to the Parquet file or directory (e.g., 'hupd_processed/2018.parquet')"
    )
    parser.add_argument(
        "--id_column",
        type=str,
        default="application_number",
        help="The column name to use for identifying duplicate records. Defaults to 'application_number'."
    )
    
    args = parser.parse_args()

    # --- Dask Client Setup ---
    # This part is crucial for distributed processing
    num_cores = os.cpu_count() or 4
    total_memory_gb = 16 
    # For inspection, we can afford slightly more memory per worker if needed,
    # or keep it tight to prevent OOM if the analysis itself is memory heavy.
    # Let's give a bit more buffer for read operations.
    memory_per_worker = f"{int(total_memory_gb * 0.8 / num_cores)}GB" # Use 80% of total RAM for workers

    client = None
    cluster = None
    try:
        cluster = LocalCluster(
            n_workers=num_cores,
            processes=True,
            memory_limit=memory_per_worker,
            # If you were spilling to disk during creation, you might want to reuse that dir
            # local_directory="dask_tmp", # Ensure this path is valid and has free space
            dashboard_address=":8787" # Ensure dashboard is accessible
        )
        client = Client(cluster)
        print(f"Dask client dashboard link: {client.dashboard_link}")
        print(f"Dask client configured with {len(client.scheduler_info()['workers'])} workers.")
        print(f"Each worker has a memory limit of {memory_per_worker}.")

    except Exception as e:
        print(f"Warning: Could not start Dask client: {e}.")
        print("Proceeding without Dask client; this may fail for large files.")
        
    # Pass the client object to the inspection function
    inspect_parquet_file(args.filepath, args.id_column, client=client)

    if client:
        client.close()
    if cluster:
        cluster.close()
        print("Dask client and cluster closed.")