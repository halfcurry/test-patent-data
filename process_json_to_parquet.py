import os
import json
import dask.dataframe as dd
import dask.bag as db
from dask.diagnostics import ProgressBar
import pandas as pd
import pyarrow as pa
from dask.distributed import Client, LocalCluster
import time

# --- CPU OPTIMIZATION: Choose a faster JSON parser if available ---
# We'll also keep track of which parser was successfully imported to adjust loading logic
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
        # orjson/ujson require the entire content as string/bytes
        return json_parser_module.loads(f.read())
    else:
        # Standard json.load works directly with file objects
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
                record = safe_json_load(f) # <--- Using helper function
                decision_status = record.get("decision")

                if decision_status is None or \
                   decision_status.startswith("CONT-") or \
                   decision_status == "PENDING" or \
                   decision_status not in ALLOWED_DECISION_STATUSES:
                    return None

                for field in fields_to_drop:
                    record.pop(field, None)
                return record
        except (json_parser_module.JSONDecodeError, KeyError, Exception) as e: # Use module-specific error
            print(f"Warning: Could not process {filepath} in Dask Bag map: {e}")
            return None

    dask_bag = db.from_sequence(json_filepaths, npartitions=os.cpu_count() or 4)
    processed_records_bag = dask_bag.map(load_and_filter_json).filter(lambda x: x is not None)

    # --- META INFERENCE for Dask DataFrame ---
    sample_record = None
    print("Attempting to find a valid sample record for schema inference...")
    num_checked_for_sample = 0
    for filepath in json_filepaths[:min(100, len(json_filepaths))]:
        num_checked_for_sample += 1
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                temp_record = safe_json_load(f) # <--- Using helper function
                
                if isinstance(temp_record, dict):
                    for field in fields_to_drop:
                        temp_record.pop(field, None)
                    sample_record = temp_record
                    print(f"Found valid sample record from: {filepath}")
                    break
                else:
                    print(f"DEBUG: File '{filepath}' did not contain a top-level dictionary. Type found: {type(temp_record)}")

        except json_parser_module.JSONDecodeError as e: # Use module-specific error
            print(f"DEBUG: JSONDecodeError in sample inference for '{filepath}': {e}")
        except Exception as e:
            print(f"DEBUG: Other error in sample inference for '{filepath}': {e}")

    if sample_record is None:
        print(f"ERROR: Could not find a valid sample record after checking {num_checked_for_sample} files. Exiting.")
        return

    meta_df = pd.DataFrame([sample_record])
    ddf = processed_records_bag.to_dataframe(meta=meta_df)

    print(f"Saving filtered data to Parquet format: {output_parquet_file}...")

    # --- EXPLICIT PYARROW SCHEMA DEFINITION ---
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

    with ProgressBar():
        ddf.to_parquet(output_parquet_file, write_index=False, schema=parquet_schema)

    print("\n--- Processing Summary ---")
    print(f"Total JSON files scanned: {total_files_scanned}")
    
    # try:
    #     num_records_in_parquet = ddf.shape[0].compute()
    #     print(f"Total records sent to Parquet (after filtering): {num_records_in_parquet}")
    #     print(f"Number of records filtered out: {total_files_scanned - num_records_in_parquet}")
    # except Exception as e:
    #     print(f"Could not compute final record count: {e}")
    #     print("Note: Computing `ddf.shape[0]` can re-trigger computations on large datasets.")

    print("Processing complete. Data saved to Parquet.")
    print(f"Number of Dask DataFrame partitions used: {ddf.npartitions}")

if __name__ == "__main__":
    start_time = time.time()

    client = None
    try:
        cluster = LocalCluster(
            n_workers=os.cpu_count() or 4,
            processes=True,
            memory_limit='4GB',
        )
        client = Client(cluster)
        print(f"Dask client dashboard link: {client.dashboard_link}")
        print(f"Dask client configured with {len(client.scheduler_info()['workers'])} workers.")
    except Exception as e:
        print(f"Warning: Could not start Dask client for optimal performance: {e}. Falling back to default scheduler.")
        print("Consider checking Dask installation or resource availability.")

    input_directory = "hupd_2018_extracted/2018"
    output_parquet = "hupd_2018_processed.parquet"
    fields_to_remove = ["claims", "background"]

    process_hupd_data_dask(input_directory, output_parquet, fields_to_remove)

    if client:
        client.close()
        print("Dask client and cluster closed.")
    
    end_time = time.time()
    total_time = end_time - start_time
    print(f"\nTotal script execution time: {total_time:.2f} seconds")