#!/bin/bash

gsutil cp gs://food_consumption_trends/source_code/requirements.txt /tmp/requirements.txt

pip install -r /tmp/requirements.txt

