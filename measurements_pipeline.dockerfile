# Use Python 3.9 as the base image
FROM python:3.9

# Set working directory inside the container
WORKDIR /app

# Copy all project files into the container
COPY measurements_pipeline.py .
COPY pipeline_unit_tests.py .
COPY requirements.txt .
COPY measurements_coding_challenge.csv .  # Ensure the dataset is copied

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Create a log file
RUN touch pipeline.log

# Expose logs as a volume
VOLUME /app/logs

# Set the entry point command
CMD ["python", "measurements_pipeline.py"]
