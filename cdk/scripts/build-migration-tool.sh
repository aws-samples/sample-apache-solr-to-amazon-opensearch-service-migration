#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CDK_DIR="$(dirname "$SCRIPT_DIR")"
PROJECT_ROOT="$(dirname "$CDK_DIR")"

echo "Building migration tool package..."

# Create assets directory and remove old package
mkdir -p "$CDK_DIR/assets"
rm -f "$CDK_DIR/assets/migration-tool.zip"

# Create fresh zip excluding unnecessary files
cd "$PROJECT_ROOT"
zip -r "$CDK_DIR/assets/migration-tool.zip" . \
  -x "cdk/*" "myenv/*" "venv/*" "__pycache__/*" "*.pyc" ".git/*" "*.log" \
     "*.zip" ".DS_Store" "*/.DS_Store"

echo "Migration tool packaged at: $CDK_DIR/assets/migration-tool.zip"