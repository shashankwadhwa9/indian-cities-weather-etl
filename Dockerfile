# Use an official Python runtime as a parent image
FROM python:3.11

# Set the working directory in the container
WORKDIR /app

# Copy the code into the container
COPY . /app/

# Install Python dependencies
RUN pip install -r requirements.txt

# Run the application
CMD ["python", "pipeline.py"]
