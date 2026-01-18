#!/bin/bash

# --- NGINX SECURITY FIX ---
# Stops Nginx from sending its version number in the 'Server' header
echo "Modifying Nginx config to hide server version..."
sed -i 's/http {/http { \n    server_tokens off;/' /etc/nginx/nginx.conf
echo "Nginx config modified."


# --- APPLICATION STARTUP (MDA) ---
# Starts Gunicorn for the 'mda:app'
# The -k flag points to the custom worker class inside your mda.py file
echo "Starting Gunicorn for mda:app..."
gunicorn --bind=0.0.0.0:8000 --workers=4 -k mda.CustomUvicornWorker mda:app