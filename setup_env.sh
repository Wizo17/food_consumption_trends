#!/bin/bash
echo "Creating the virtual environment."
python3 -m venv venv

echo "Environment activation."
source venv/bin/activate

echo "Installing dependencies."
pip install -r requirements.txt

echo "Copying .env."
cp .env_prod .env

echo "Environment successfully configured!"
