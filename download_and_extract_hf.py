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
    os.makedirs(extract_dir, exist_ok=True)

    print(f"Downloading {filename} from {repo_id} using huggingface_hub...")
    try:
        # hf_hub_download automatically handles caching and finds the raw URL
        downloaded_filepath = hf_hub_download(
            repo_id=repo_id,
            filename=filename,
            cache_dir=download_dir # You can specify a custom cache directory
        )
        print(f"File downloaded to: {downloaded_filepath}")
    except Exception as e:
        print(f"Error downloading file from Hugging Face Hub: {e}")
        return

    print(f"Extracting {downloaded_filepath} to {extract_dir}...")
    try:
        with tarfile.open(downloaded_filepath, "r:gz") as tar:
            tar.extractall(path=extract_dir)
        print("Extraction complete.")
        print(f"JSON files should now be in: {extract_dir}")
        print("You can verify by listing contents (e.g., os.listdir(extract_dir)[:5])")
    except tarfile.ReadError as e:
        print(f"Error: Could not read tar.gz file. It might be corrupted: {e}")
    except Exception as e:
        print(f"An unexpected error occurred during extraction: {e}")


if __name__ == "__main__":
    HUGGINGFACE_REPO_ID = "HUPD/hupd"
    TAR_FILENAME = "data/2018.tar.gz"
    DOWNLOAD_CACHE_DIR = "hf_cache" # Hugging Face will cache here
    EXTRACT_DIR = "hupd_2018_extracted"

    download_and_extract_hupd(HUGGINGFACE_REPO_ID, TAR_FILENAME, DOWNLOAD_CACHE_DIR, EXTRACT_DIR)