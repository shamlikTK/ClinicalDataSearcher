FROM python:3.12-slim

WORKDIR /app

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy and install Python dependencies first (better caching)
COPY requirements.txt .
RUN uv pip install --system --no-cache -r requirements.txt

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    PYTHONDONTWRITEBYTECODE=1

# Create data directory and copy application code
RUN mkdir -p /app/data
COPY . .

# Command to run the application
CMD ["python", "etl_pipeline.py"] 
