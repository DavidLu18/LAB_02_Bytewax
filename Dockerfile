FROM python:3.10.12

# Set the working directory
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install sqlalchemy==2.0.41

# Copy the rest of the application code into the container
COPY . .
