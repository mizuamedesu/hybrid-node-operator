FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy operator code
COPY operator/ ./operator/

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Run the operator
CMD ["kopf", "run", "--standalone", "--liveness=http://0.0.0.0:8080/healthz", "operator/main.py"]
