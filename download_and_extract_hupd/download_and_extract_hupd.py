# download_and_extract_hupd.py
import os
import tarfile
from huggingface_hub import hf_hub_download

def download_and_extract_hupd(repo_id, filename, download_dir, extract_dir):
    """
    Downloads a file from Hugging Face Hub and extracts it if it's a tar.gz.

    Args:
        repo_id (str): The Hugging Face repository ID (e.g., "HUPD/hupd").
        filename (str): The specific file to download (e.g., "data/2018.tar.gz").
        download_dir (str): Directory to save the downloaded file.
        extract_dir (str): Directory to extract the contents.
    """
    os.makedirs(download_dir, exist_ok=True)
    os.makedirs(extract_dir, exist_ok=True) # Ensure the base extraction directory exists

    print(f"Downloading {filename} from {repo_id} using huggingface_hub...")
    try:
        downloaded_filepath = hf_hub_download(
            repo_id=repo_id,
            filename=filename,
            cache_dir=download_dir,
            repo_type="dataset"
        )
        print(f"File downloaded to: {downloaded_filepath}")
    except Exception as e:
        print(f"Error downloading file from Hugging Face Hub: {e}")
        return

    print(f"Extracting {downloaded_filepath} to {extract_dir}...")
    try:
        with tarfile.open(downloaded_filepath, "r:gz") as tar:
            tar.extractall(path=extract_dir) # Extract into the base directory
        print("Extraction complete.")
        # The actual JSON files will now be in extract_dir/{year}/
        print(f"JSON files should now be in: {os.path.join(extract_dir, filename.split('/')[1].split('.')[0])}")
        # Example verification: print(os.listdir(os.path.join(extract_dir, filename.split('/')[1].split('.')[0]))[:5])
    except tarfile.ReadError as e:
        print(f"Error: Could not read tar.gz file. It might be corrupted: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during extraction: {e}")


if __name__ == "__main__":
    HUGGINGFACE_REPO_ID = "HUPD/hupd"

    year_to_download = os.getenv("HUPD_DATA_YEAR", "2018")
    TAR_FILENAME = f"data/{year_to_download}.tar.gz"

    DOWNLOAD_CACHE_DIR = "hf_cache"
    # *** KEY CHANGE HERE: Extract directly into the base hupd_extracted folder ***
    # The tarball is expected to contain a top-level directory like "2018/"
    EXTRACT_DIR = "hupd_extracted" 

    download_and_extract_hupd(HUGGINGFACE_REPO_ID, TAR_FILENAME, DOWNLOAD_CACHE_DIR, EXTRACT_DIR)