FROM python:3.10-slim

WORKDIR /app

# Upgrade pip and install requirements
RUN python -m pip install --upgrade pip
COPY requirements.txt .
# Add gunicorn to your requirements installation
RUN pip install --no-cache-dir -r requirements.txt gunicorn

# Copy the rest of your application code
COPY . .

# Expose port 8000 (standard for web apps, preferred by Azure App Service)
EXPOSE 8001

# --- CRITICAL CHANGE ---
# Replace the Uvicorn command with the Gunicorn command to run multiple workers
CMD ["gunicorn", "mda_2:app", "--workers", "4", "--worker-class", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8001"]
