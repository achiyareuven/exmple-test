# app/utils/logger.py
import logging
import os
from pathlib import Path
from logging.handlers import RotatingFileHandler

# שורש הפרויקט: שני הורים מעל app/utils
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# אפשר להגדיר LOG_DIR ב-.env; אחרת <root>/logs
LOG_DIR = Path(os.getenv("LOG_DIR", PROJECT_ROOT / "logs"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "app.log"

# נשמור את הלוגר האב כ-singleton
_APP_LOGGER = None

def _init_app_logger() -> logging.Logger:
    logger = logging.getLogger("app")       # לוגר אב יחיד לכל המערכת
    logger.setLevel(logging.DEBUG)

    # ננקה handlers קיימים (אם טעון מחדש)
    if logger.handlers:
        logger.handlers.clear()

    fmt = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # למסך
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    # לקובץ עם רוטציה (5MB, עד 3 גיבויים)
    fh = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # לא להפיץ ל-root (כדי למנוע לוג כפול אם מישהו הגדיר basicConfig)
    logger.propagate = False

    logger.debug(f"Logging to file: {LOG_FILE}")
    return logger

def get_logger(name: str) -> logging.Logger:
    global _APP_LOGGER
    if _APP_LOGGER is None:
        _APP_LOGGER = _init_app_logger()
    # החזר child-logger (אין לו handlers; הוא יורש מה"אב")
    return _APP_LOGGER.getChild(name) if name and name != "app" else _APP_LOGGER
