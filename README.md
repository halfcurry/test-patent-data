# test-patent-data

```bash
python download_and_extract_hupd.py 2018

# If using go:
go mod init go_process_json_to_parquet
go mod tidy
go run go_process_json_to_parquet -year 2018

#If using python dask:
python process_json_to_parquet.py 2018

#Inspecting the resultant parquet:
python inspect_parquet.py hupd_processed/2018.parquet
```

```bash
#Using python dask:
--- Processing Summary ---
Total JSON files scanned: 31968
Processing complete. Data saved to Parquet.
Number of Dask DataFrame partitions used: 8
Dask client and cluster closed.

Total script execution time: 11.27 seconds

#Using go:
-- Processing Summary ---
Total JSON files scanned: 31968
Total records processed by workers (before deduplication): 201
Total records filtered out by worker logic (decision status, parsing errors): 31767
Total records written to Parquet (after deduplication): 201
Processing complete. Data saved to Parquet.

Total script execution time: 10.33 seconds
```