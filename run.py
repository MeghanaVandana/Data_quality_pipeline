import sys
from src.pipeline.orchestrator import run_pipeline

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python run.py <csv-file-path>")
        sys.exit(1)

    run_pipeline(sys.argv[1])
