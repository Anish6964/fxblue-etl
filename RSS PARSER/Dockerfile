# Use official Python image as a parent
FROM python:3.9-slim

WORKDIR /app

# Copy the requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire content to the container
COPY . .

# Run gcp_rss.py as the main script
CMD ["python", "gcp_rss.py"]
