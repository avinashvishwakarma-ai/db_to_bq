# Use a stable Python 3.11 slim image
FROM python:3.11-slim

 
RUN apt-get update && apt-get install -y \
    ca-certificates \
    openssl \
    libssl-dev \
    libsasl2-dev \
    gcc \
    g++ \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

#  Set environment variables
# This ensures Python uses the OS-level certificates updated above
# and prevents .pyc files from being written to the container
# Standard Python environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Forces Python/Requests to use the system CA bundle you updated in step 1
ENV REQUESTS_CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

WORKDIR /app

# 3. Install Python Dependencies
# We copy this first to leverage Docker's cache layer
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# 4. Copy Application Files
# Includes your .py script and the .sql query file
COPY . .

# 5. Run the Script
# Replace 'your_script_name.py' with your actual filename
CMD ["python", "final_subsea_data_pipeline.py"]