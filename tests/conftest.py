import os
import sys
from pathlib import Path

# Ensure the repo root (where main.py/score.py live) is importable regardless
# of the directory pytest is invoked from.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# main.py reads these at import time (e.g. it raises if no Gemini key is
# configured, and constructs a genai.Client per key). Set harmless dummy
# values before any test imports main, so the import doesn't hit the network
# or fail outright in a CI/test environment with no real .env.
os.environ.setdefault("GEMINI_API_KEY_1", "test-key")
os.environ.setdefault("MONGODB_URI", "mongodb://localhost:27017")
os.environ.setdefault("DB_NAME", "test_jobless")
os.environ.setdefault("TELEGRAM_API_ID", "0")
