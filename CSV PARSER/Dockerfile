# Use official Python image as a parent
FROM python:3.9-slim

# Install dependencies and Cloud SQL Proxy
RUN apt-get update && apt-get install -y \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install the Cloud SQL Proxy
RUN curl -sSL https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -o /usr/local/bin/cloud_sql_proxy && \
    chmod +x /usr/local/bin/cloud_sql_proxy

WORKDIR /app

# Copy the requirements.txt and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire content to the container
COPY . .

# Run gcp_csv.py as the main script
CMD ["sh", "-c", "/usr/local/bin/cloud_sql_proxy -dir=/cloudsql -credential_file=/secrets/cloudsql/credentials.json & python gcp_csv.py"]

