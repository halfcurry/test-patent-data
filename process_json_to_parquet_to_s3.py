import os
import json
import dask.dataframe as dd
import dask.bag as db
from dask.distributed import Client, LocalCluster, progress
import pandas as pd
import pyarrow as pa
import time
import argparse
import s3fs # Essential for S3 integration

# --- CPU OPTIMIZATION: Choose a faster JSON parser if available ---
json_parser_name = "default_json"
try:
    import orjson as json_parser_module
    json_parser_name = "orjson"
    print("Using orjson for faster JSON parsing.")
except ImportError:
    try:
        import ujson as json_parser_module
        json_parser_name = "ujson"
        print("Using ujson for faster JSON parsing.")
    except ImportError:
        import json as json_parser_module # Fallback to standard json
        print("Using default json library for parsing (orjson/ujson not found).")


# --- Helper function to load JSON based on the chosen parser ---
def safe_json_load(f):
    """
    Loads JSON from a file-like object using the selected faster parser,
    or falls back to the default `json` module.
    """
    if json_parser_name in ["orjson", "ujson"]:
        return json_parser_module.loads(f.read())
    else:
        return json_parser_module.load(f)


def process_hupd_data_dask(input_dir: str, output_target_path: str, fields_to_drop: list, client_obj: Client = None):
    """
    Loads JSON files from a directory using Dask, filters based on decision status,
    drops specified fields, and saves to Parquet in parallel directly to a local
    path or an S3 bucket.

    Args:
        input_dir (str): Directory containing the extracted JSON files.
        output_target_path (str): The destination path for the processed Parquet files.
                                  This can be a local directory (e.g., 'hupd_processed/2018.parquet')
                                  or an S3 URI (e.g., 's3://your-bucket/processed_data/2018.parquet').
        fields_to_drop (list): A list of field names to drop from each JSON record.
        client_obj (dask.distributed.Client): The Dask client object, if already initialized.
    """
    ALLOWED_DECISION_STATUSES = {"ACCEPTED", "REJECTED"}

    # Determine if the output is S3 or local
    is_s3_output = output_target_path.lower().startswith("s3://")

    if not is_s3_output:
        # For local output, ensure the parent directory exists
        output_parent_dir = os.path.dirname(output_target_path)
        if output_parent_dir and not os.path.exists(output_parent_dir):
            os.makedirs(output_parent_dir, exist_ok=True)
            print(f"Created local output directory: {output_parent_dir}")
    else:
        print(f"Output path is an S3 URI: {output_target_path}. Ensuring s3fs is correctly configured for access.")

    print(f"Scanning for JSON files in {input_dir}...")
    json_filepaths = []
    for root, _, files in os.walk(input_dir):
        for f in files:
            if f.endswith('.json'):
                json_filepaths.append(os.path.join(root, f))
    
    total_files_scanned = len(json_filepaths)
    if not json_filepaths:
        print(f"No JSON files found in {input_dir}. Exiting.")
        return

    print(f"Found {total_files_scanned} JSON files. Starting Dask processing...")

    def load_and_filter_json(filepath):
        """
        Loads a single JSON file, filters it based on decision status,
        and drops specified fields. Returns None if the record is invalid or filtered out.
        """
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                record = safe_json_load(f) 
                decision_status = record.get("decision")

                if decision_status is None or \
                   decision_status.startswith("CONT-") or \
                   decision_status == "PENDING" or \
                   decision_status not in ALLOWED_DECISION_STATUSES:
                    return None

                for field in fields_to_drop:
                    record.pop(field, None)
                return record
        except (json_parser_module.JSONDecodeError, KeyError, Exception) as e: 
            # You might want to log these errors for debugging, but for silent skipping,
            # returning None is effective.
            return None

    # Get the number of workers from the client, or use CPU count as a fallback
    num_workers = len(client_obj.scheduler_info()['workers']) if client_obj else (os.cpu_count() or 4)

    # Heuristic for partitioning: aim for a certain number of files per partition
    # This value might need tuning based on average file size and memory limits.
    files_per_partition_target = 250 # Adjusted to be more aggressive with partitioning
    
    # Calculate desired number of partitions, ensuring at least `num_workers * 2` partitions
    # and capping to prevent excessive scheduler overhead for very small files.
    desired_npartitions = max(num_workers * 2, total_files_scanned // files_per_partition_target)
    max_reasonable_partitions = 10000 # Increased cap for very large datasets/many files
    num_bag_partitions = min(desired_npartitions, max_reasonable_partitions)

    print(f"Dividing {total_files_scanned} files into {num_bag_partitions} Dask Bag partitions (avg {total_files_scanned / num_bag_partitions:.0f} files/partition).")
    dask_bag = db.from_sequence(json_filepaths, npartitions=num_bag_partitions)
    
    # Map the load_and_filter_json function over the Dask Bag, then filter out None values
    processed_records_bag = dask_bag.map(load_and_filter_json).filter(lambda x: x is not None)

    # --- Schema Inference (using a sample record) ---
    sample_record = None
    print("Attempting to find a valid sample record for schema inference (checking up to 10 files)...")
    # Take a few samples to infer the schema. `take` is computed on the cluster.
    sample_records_list = processed_records_bag.take(10, npartitions=min(10, processed_records_bag.npartitions))
    
    if sample_records_list and len(sample_records_list) > 0:
        sample_record = sample_records_list[0] 
        print(f"Found valid sample record.")
    else:
        print(f"ERROR: Could not find any valid sample records. Exiting.")
        return

    # Create a Pandas DataFrame from the sample record to infer the initial Dask DataFrame meta
    meta_df = pd.DataFrame([sample_record])
    ddf = processed_records_bag.to_dataframe(meta=meta_df)

    print(f"Saving filtered data to Parquet format to: {output_target_path}...")

    # --- EXPLICIT PYARROW SCHEMA DEFINITION ---
    # This ensures consistent data types and handles complex nested types.
    parquet_schema = pa.schema([
        pa.field('application_number', pa.string()),
        pa.field('publication_number', pa.string()),
        pa.field('title', pa.string()),
        pa.field('decision', pa.string()),
        pa.field('date_produced', pa.string()),
        pa.field('date_published', pa.string()),
        pa.field('main_cpc_label', pa.string()),
        pa.field('cpc_labels', pa.list_(pa.string())),
        pa.field('main_ipcr_label', pa.string()),
        pa.field('ipcr_labels', pa.list_(pa.string())),
        pa.field('patent_number', pa.string()),
        pa.field('filing_date', pa.string()),
        pa.field('patent_issue_date', pa.string()),
        pa.field('abandon_date', pa.string()),
        pa.field('uspc_class', pa.string()),
        pa.field('uspc_subclass', pa.string()),
        pa.field('examiner_id', pa.string()),
        pa.field('examiner_name_last', pa.string()),
        pa.field('examiner_name_first', pa.string()),
        pa.field('examiner_name_middle', pa.string()),
        pa.field('inventor_list', pa.list_(pa.struct([
            pa.field('inventor_city', pa.string()),
            pa.field('inventor_country', pa.string()),
            pa.field('inventor_name_first', pa.string()),
            pa.field('inventor_name_last', pa.string()),
            pa.field('inventor_state', pa.string())
        ]))),
        pa.field('abstract', pa.string()),
        pa.field('summary', pa.string()),
        pa.field('full_description', pa.string()),
    ])

    print("Initiating Dask computation and displaying progress...")
    # Persist the Dask DataFrame to trigger the computation and make it available in memory/disk cache
    ddf_persisted = ddf.persist()
    # Display the progress bar for the computation
    progress(ddf_persisted)

    # Dask's to_parquet automatically handles writing to S3 if the path starts with 's3://'
    ddf_persisted.to_parquet(output_target_path, write_index=False, schema=parquet_schema, compression='snappy')
    
    print(f"Data saved to Parquet directory: {output_target_path}")
    print("Note: Dask writes multiple part files into the specified path, treating it as a directory.")


    print("\n--- Processing Summary ---")
    print(f"Total JSON files scanned: {total_files_scanned}")
    print("Processing complete. Data saved to Parquet.")
    print(f"Number of Dask DataFrame partitions used: {ddf.npartitions}")


if __name__ == "__main__":
    start_time = time.time()

    parser = argparse.ArgumentParser(
        description="Process HUPd JSON data using Dask and save to Parquet (local or S3)."
    )
    parser.add_argument(
        "year",
        type=str,
        nargs='?', 
        default="2018", 
        help="The year of the HUPd dataset to process (e.g., '2018', '2019'). Defaults to 2018."
    )
    parser.add_argument(
        "--s3-bucket",
        type=str,
        help="Optional: S3 bucket name (e.g., 'your-patent-data-bucket'). If provided, data will be saved to S3."
    )
    parser.add_argument(
        "--s3-prefix",
        type=str,
        default="hupd_processed", # Default prefix within S3
        help="Optional: S3 key prefix for the output (e.g., 'processed_data/'). Only used if --s3-bucket is specified."
    )
    
    args = parser.parse_args()

    num_cores = os.cpu_count() or 4
    total_memory_gb = 16 

    # Recommended memory limits for 16GB RAM with a 13GB dataset.
    # This encourages Dask to spill to disk, preventing OOM errors.
    num_workers_config = num_cores # Use all available cores as workers
    memory_per_worker_config = "2.5GB" # Each worker capped at 2.5GB

    # Crucial: Specify a local_directory for Dask to spill temporary data to disk.
    # Ensure this directory has ample free space.
    dask_tmp_dir = os.path.join(os.getcwd(), "dask_tmp") 
    os.makedirs(dask_tmp_dir, exist_ok=True)
    print(f"Dask will use {dask_tmp_dir} for temporary spill files.")


    client = None
    cluster = None # Define cluster outside try block for wider scope
    try:
        cluster = LocalCluster(
            n_workers=num_workers_config,
            processes=True, # Use separate processes for workers (safer for memory isolation)
            memory_limit=memory_per_worker_config, 
            local_directory=dask_tmp_dir, # Enables spilling to disk
            dashboard_address=":8787" # Makes the Dask dashboard accessible for monitoring
        )
        client = Client(cluster)
        print(f"Dask client dashboard link: {client.dashboard_link}")
        print(f"Dask client configured with {len(client.scheduler_info()['workers'])} workers.")
        print(f"Each worker has a memory limit of {memory_per_worker_config}.")

    except Exception as e:
        print(f"Warning: Could not start Dask client for optimal performance: {e}.")
        print("Falling back to default scheduler (might be less efficient).")

    input_directory = os.path.join("hupd_extracted", args.year)
    
    # Determine the final output path (local or S3 URI)
    if args.s3_bucket:
        # Construct the S3 URI: s3://bucket-name/prefix/year.parquet/
        s3_path_prefix = args.s3_prefix
        if s3_path_prefix and not s3_path_prefix.endswith('/'):
            s3_path_prefix += '/' # Ensure the prefix ends with a slash for a proper path
        
        output_target_path = f"s3://{args.s3_bucket}/{s3_path_prefix}{args.year}.parquet"
    else:
        # Local path if no S3 bucket specified
        output_target_path = os.path.join("hupd_processed", f"{args.year}.parquet")
    
    fields_to_remove = ["claims", "background"]

    # Pass the client object and the determined output path to the processing function
    process_hupd_data_dask(input_directory, output_target_path, fields_to_remove, client_obj=client)

    # Clean up Dask client and cluster resources
    if client:
        client.close()
    if cluster: 
        cluster.close() 
        print("Dask client and cluster closed.")
    
    end_time = time.time()
    total_time = end_time - start_time
    print(f"\nTotal script execution time: {total_time:.2f} seconds")