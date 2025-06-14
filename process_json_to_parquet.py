import os
import json
import dask.dataframe as dd
import dask.bag as db
from dask.diagnostics import ProgressBar
import pandas as pd
import pyarrow as pa # Make sure pyarrow is imported
from dask.distributed import Client, LocalCluster
import time
import argparse

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
    if json_parser_name in ["orjson", "ujson"]:
        return json_parser_module.loads(f.read())
    else:
        return json_parser_module.load(f)


def process_hupd_data_dask(input_dir, output_parquet_file, fields_to_drop):
    """
    Loads JSON files from a directory using Dask, filters based on decision status,
    drops specified fields, and saves to Parquet in parallel.

    Args:
        input_dir (str): Directory containing the extracted JSON files.
        output_parquet_file (str): Path where the processed Parquet file will be saved.
        fields_to_drop (list): A list of field names to drop from each JSON record.
    """
    ALLOWED_DECISION_STATUSES = {"ACCEPTED", "REJECTED"}

    # Ensure output directory exists
    output_parent_dir = os.path.dirname(output_parquet_file)
    if output_parent_dir and not os.path.exists(output_parent_dir):
        os.makedirs(output_parent_dir, exist_ok=True)
        print(f"Created output directory: {output_parent_dir}")


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
            return None

    num_bag_partitions = os.cpu_count() * 2 if os.cpu_count() else 8 
    dask_bag = db.from_sequence(json_filepaths, npartitions=num_bag_partitions)
    processed_records_bag = dask_bag.map(load_and_filter_json).filter(lambda x: x is not None)

    # --- META INFERENCE for Dask DataFrame ---
    # We still need a `meta` DataFrame for Dask's internal operations (like knowing column names).
    # However, the types will be overridden by the explicit `schema` in `to_parquet`.
    sample_record = None
    print("Attempting to find a valid sample record for schema inference (checking up to 10 files)...")
    
    # Use .take() to get a few samples efficiently from the *processed* bag
    sample_records_list = processed_records_bag.take(10, npartitions=min(10, processed_records_bag.npartitions))
    
    if sample_records_list and len(sample_records_list) > 0:
        sample_record = sample_records_list[0] # Take the first valid record
        print(f"Found valid sample record.")
    else:
        print(f"ERROR: Could not find any valid sample records. This might indicate an issue with input data or filtering logic. Exiting.")
        return

    # Create a Pandas DataFrame from the sample record to use as `meta`
    # The actual types in this meta_df don't need to perfectly match PyArrow's
    # complex types as we will provide the explicit PyArrow schema.
    meta_df = pd.DataFrame([sample_record])
    ddf = processed_records_bag.to_dataframe(meta=meta_df)

    print(f"Saving filtered data to Parquet format: {output_parquet_file}...")

    # --- EXPLICIT PYARROW SCHEMA DEFINITION ---
    # This is the fix! Use this precise schema to ensure Dask/PyArrow writes correctly.
    parquet_schema = pa.schema([
        pa.field('application_number', pa.string()),
        pa.field('publication_number', pa.string()),
        pa.field('title', pa.string()),
        pa.field('decision', pa.string()),
        pa.field('date_produced', pa.string()),
        pa.field('date_published', pa.string()),
        pa.field('main_cpc_label', pa.string()),
        pa.field('cpc_labels', pa.list_(pa.string())), # Corrected: list of strings
        pa.field('main_ipcr_label', pa.string()),
        pa.field('ipcr_labels', pa.list_(pa.string())), # Corrected: list of strings
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
        # Corrected: list of structs for inventor_list
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

    with ProgressBar():
        ddf.to_parquet(output_parquet_file, write_index=False, schema=parquet_schema, compression='snappy')
        print(f"Data saved to Parquet directory: {output_parquet_file}")
        print("Note: Dask writes multiple part files into the specified path, treating it as a directory.")


    print("\n--- Processing Summary ---")
    print(f"Total JSON files scanned: {total_files_scanned}")
    
    print("Processing complete. Data saved to Parquet.")
    print(f"Number of Dask DataFrame partitions used: {ddf.npartitions}")

if __name__ == "__main__":
    start_time = time.time()

    parser = argparse.ArgumentParser(
        description="Process HUPd JSON data using Dask and save to Parquet."
    )
    parser.add_argument(
        "year",
        type=str,
        nargs='?', 
        default="2018", 
        help="The year of the HUPd dataset to process (e.g., '2018', '2019'). Defaults to 2018."
    )
    args = parser.parse_args()

    num_cores = os.cpu_count() or 4
    total_memory_gb = 16 
    memory_per_worker = f"{int(total_memory_gb / num_cores)}GB" 

    client = None
    try:
        cluster = LocalCluster(
            n_workers=num_cores,
            processes=True, 
            memory_limit=memory_per_worker, 
        )
        client = Client(cluster)
        print(f"Dask client dashboard link: {client.dashboard_link}")
        print(f"Dask client configured with {len(client.scheduler_info()['workers'])} workers.")
        print(f"Each worker has a memory limit of {memory_per_worker}.")

    except Exception as e:
        print(f"Warning: Could not start Dask client for optimal performance: {e}.")
        print("Falling back to default scheduler (might be less efficient).")

    input_directory = os.path.join("hupd_extracted", args.year)
    output_parquet = os.path.join("hupd_processed", f"{args.year}.parquet")
    
    fields_to_remove = ["claims", "background"]

    process_hupd_data_dask(input_directory, output_parquet, fields_to_remove)

    if client:
        client.close()
        cluster.close() 
        print("Dask client and cluster closed.")
    
    end_time = time.time()
    total_time = end_time - start_time
    print(f"\nTotal script execution time: {total_time:.2f} seconds")