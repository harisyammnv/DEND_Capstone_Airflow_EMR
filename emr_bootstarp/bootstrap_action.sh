#!/bin/bash
mkdir -p $HOME/python_apps
aws s3 cp s3://dend-capstone-data/python_apps  $HOME/python_apps --recursive --exclude "*" --include "*.py"