# Dockerfile (in your project root, e.g., next to download_and_extract_hupd.py)
FROM python:3.9-slim-buster

# Set the working directory in the container
WORKDIR /app

# Copy the Python script and requirements file
COPY download_and_extract_hupd.py .

# Install Python dependencies
# You'll need a requirements.txt if you have more dependencies.
# For now, we'll just install huggingface_hub directly.
RUN pip install huggingface_hub

# Command to run the script. It will read env variables automatically.
CMD ["python", "download_and_extract_hupd.py"]