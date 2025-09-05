# app/tests/try_build_event.py
from pathlib import Path
import os, csv
from dotenv import load_dotenv
from app.processing.schema import event_from_row

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parents[2]
env_dir = os.getenv("DATA_DIR", "data")
DATA_DIR = Path(env_dir) if Path(env_dir).is_absolute() else PROJECT_ROOT / env_dir
tsv_path = DATA_DIR / "corpus" / "validated.tsv"

print("TSV =", tsv_path)
with open(tsv_path, encoding="utf-8-sig", newline="") as f:
    r = csv.DictReader(f, delimiter="\t")
    for i, row in enumerate(r, start=1):
        ev = event_from_row(row)
        print(f"{i}. clip_id={ev.clip_id} | path={ev.path} | locale={ev.locale} | sentence[:40]={ (ev.sentence or '')[:40] }")
        if i >= 3:
            break
