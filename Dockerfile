# Use an official Python runtime as a parent image
FROM python:3.13

# Set the working directory in the container
WORKDIR /runner

# Copy the current directory contents into the container
COPY . .

# Install graphviz
RUN apt-get update && apt-get install -y graphviz libgraphviz-dev pkg-config

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt
# Run the application
CMD ["python", "-u", "main.py"]
