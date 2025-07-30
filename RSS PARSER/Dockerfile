# RSS_PARSER/Dockerfile
FROM python:3.9-slim

# Install Postgres client libraries & curl
RUN apt-get update && apt-get install -y \
    libpq-dev \
    curl \
  && rm -rf /var/lib/apt/lists/*

# Install the Cloud SQL Proxy binary
RUN curl -sSL https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 \
     -o /usr/local/bin/cloud_sql_proxy \
  && chmod +x /usr/local/bin/cloud_sql_proxy

# Create the socket directory
RUN mkdir /cloudsql

WORKDIR /app

# Copy & install Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the RSS script
COPY gcp_rss.py .

# On startup: launch Cloud SQL Proxy, then run your script
CMD ["sh","-c","/usr/local/bin/cloud_sql_proxy -dir=/cloudsql -instances=$CLOUD_SQL_CONNECTION_NAME & python gcp_rss.py"]
