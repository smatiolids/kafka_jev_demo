"""Copies questions.json into connector.json as the "jev.questions" string.

Usage: python config/set_questions.py
"""

import json
from pathlib import Path

CONFIG_DIR = Path(__file__).parent
QUESTIONS_FILE = CONFIG_DIR / "questions.json"
CONNECTOR_FILE = CONFIG_DIR / "connector.json"

questions = json.loads(QUESTIONS_FILE.read_text())
connector = json.loads(CONNECTOR_FILE.read_text())

connector["jev.questions"] = json.dumps(questions, separators=(",", ":"), ensure_ascii=False)

CONNECTOR_FILE.write_text(json.dumps(connector, indent=2, ensure_ascii=False) + "\n")
print(f"Set jev.questions in {CONNECTOR_FILE.name} from {QUESTIONS_FILE.name}")
