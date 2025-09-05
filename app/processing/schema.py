# app/processing/schema.py
from dataclasses import dataclass
from typing import Optional, Dict
from pathlib import Path

@dataclass
class ClipEvent:
    clip_id: str               # מזהה ייחודי לכל קליפ
    path: str                  # יחסי ל- clips/
    sentence: Optional[str] = None
    locale: Optional[str] = None
    meta: Dict[str, str] = None

    def to_message(self) -> dict:
        return {
            "clip_id": self.clip_id,
            "path": self.path.replace("\\", "/"),
            "sentence": self.sentence,
            "locale": self.locale,
            "meta": self.meta or {},
            "schema_version": 1,
        }

def make_clip_id(row: Dict[str, str]) -> str:
    """מנסה sentence_id, אחרת שם קובץ בלי סיומת"""
    sid = (row.get("sentence_id") or "").strip()
    if sid:
        return sid
    path = (row.get("path") or "").strip()
    return Path(path).stem

def event_from_row(row: Dict[str, str]) -> ClipEvent:
    """בונה אירוע מסכימת ה-TSV שלנו"""
    return ClipEvent(
        clip_id=make_clip_id(row),
        path=(row.get("path") or "").strip().replace("\\", "/"),
        sentence=(row.get("sentence") or "").strip(),
        locale=(row.get("locale") or "").strip(),
        meta={
            "age": (row.get("age") or "").strip(),
            "gender": (row.get("gender") or "").strip(),
            "accents": (row.get("accents") or "").strip(),
            "up_votes": (row.get("up_votes") or "").strip(),
            "down_votes": (row.get("down_votes") or "").strip(),
        },
    )
