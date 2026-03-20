#!/bin/bash

java -version
uv run --env-file .env python src/main.py
