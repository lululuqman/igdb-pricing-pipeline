# Dockerfile

FROM apache/airflow:2.8.1-python3.11

# Install necessary system packages for common Python libraries.
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gcc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# (*** REMOVE THE CHOWN COMMANDS HERE ***)

# Switch back to the low-privilege 'airflow' user (best practice)
USER airflow