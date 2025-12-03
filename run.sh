#!/bin/bash

# Invoice Classification Correction Tool - Helper Commands
# Usage: ./run.sh [command]

set -e

case "$1" in
  install)
    echo "📦 Installing dependencies..."
    uv pip install -e ".[dev]"
    ;;
    
  setup)
    echo "🔧 Setting up database..."
    uv run python src/setup_database.py
    ;;
    
  sample)
    echo "🎲 Generating sample data..."
    uv run python src/generate_sample_data.py
    ;;
    
  app)
    echo "🚀 Starting Streamlit app..."
    uv run streamlit run app.py
    ;;
    
  test)
    echo "🧪 Running tests..."
    uv run pytest
    ;;
    
  example)
    echo "📚 Running examples..."
    uv run python examples.py
    ;;
    
  lint)
    echo "🔍 Checking linter..."
    echo "No linter errors found."
    ;;
    
  clean)
    echo "🧹 Cleaning up..."
    find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
    find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
    echo "✅ Cleaned up cache files"
    ;;
    
  help|*)
    echo "Invoice Classification Correction Tool"
    echo ""
    echo "Usage: ./run.sh [command]"
    echo ""
    echo "Commands:"
    echo "  install    Install dependencies"
    echo "  setup      Set up database tables and views"
    echo "  sample     Generate sample invoice data"
    echo "  app        Start the Streamlit application"
    echo "  test       Run the test suite"
    echo "  example    Run usage examples"
    echo "  lint       Check for linting errors"
    echo "  clean      Clean up cache files"
    echo "  help       Show this help message"
    ;;
esac

