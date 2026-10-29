"""Main module entry point for running via `python -m src.log_pipeline`."""

import sys
from .cli import main

if __name__ == "__main__":
    sys.exit(main())
