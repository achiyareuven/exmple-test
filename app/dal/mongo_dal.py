# import os, csv, json

import os
from pymongo import MongoClient
from pymongo.errors import PyMongoError
from app.utils.logger import get_logger
from dotenv import load_dotenv
load_dotenv()


class MongoDAL:
    def __init__(
        self,
        uri: str = None,
        db_name: str = None,
        collection_name: str = None,
    ):
        self.logger = get_logger(__name__)

        # קריאה מהסביבה אם לא הועבר פרמטר
        self.uri = uri or os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.db_name = db_name or os.getenv("MONGO_DB", "appdb")
        self.collection_name = collection_name or os.getenv("MONGO_COLLECTION", "texts")

        try:
            self.client = MongoClient(self.uri)
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            self.logger.info(
                f"Connected to MongoDB: {self.uri}, DB: {self.db_name}, Collection: {self.collection_name}"
            )
        except PyMongoError as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    def create(self, doc: dict) -> str:
        try:
            result = self.collection.insert_one(doc)
            self.logger.info(f"Inserted document with _id={result.inserted_id}")
            return str(result.inserted_id)
        except PyMongoError as e:
            self.logger.error(f"Create failed: {e}")
            raise

    def read(self, query: dict) -> dict:
        try:
            doc = self.collection.find_one(query)
            self.logger.info(f"Read query={query}, found={bool(doc)}")
            return doc
        except PyMongoError as e:
            self.logger.error(f"Read failed: {e}")
            raise

    def list_all(self) -> list:
        try:
            docs = list(self.collection.find())
            self.logger.info(f"Listed {len(docs)} documents")
            return docs
        except PyMongoError as e:
            self.logger.error(f"List all failed: {e}")
            raise

    def update(self, query: dict, new_values: dict) -> int:
        try:
            result = self.collection.update_many(query, {"$set": new_values})
            self.logger.info(
                f"Updated {result.modified_count} documents, query={query}, new_values={new_values}"
            )
            return result.modified_count
        except PyMongoError as e:
            self.logger.error(f"Update failed: {e}")
            raise

    def delete(self, query: dict) -> int:
        try:
            result = self.collection.delete_many(query)
            self.logger.info(f"Deleted {result.deleted_count} documents, query={query}")
            return result.deleted_count
        except PyMongoError as e:
            self.logger.error(f"Delete failed: {e}")
            raise


dal =MongoDAL()
doc_id = dal.create({"name": "Achiya", "role": "Tester"})
print(f"Inserted ID: {doc_id}")


































# from pathlib import Path
# from typing import Callable, Iterator, Dict, Any, Optional
# from dotenv import load_dotenv
# from kafka import KafkaProducer
# from app.processing.schema import event_from_row  # ברירת מחדל לדומיין האודיו
# from app.utils.logger import get_logger
#
# logger = get_logger(__name__)
#
# class IngestProducer:
#     """
#     Producer גנרי:
#     - קורא רשומות ממקור טבלאי (TSV/CSV) או מכל איטרטור שתיתנו.
#     - ממיר כל רשומה ל'הודעה' באמצעות פונקציה ניתנת להחלפה (row_to_message).
#     - מפיק מפתח (key) באמצעות key_fn (ברירת מחדל: 'clip_id' או 'id').
#     - שולח ל- Kafka topic לפי קונפיג בסביבה.
#     """
#
#     def __init__(
#         self,
#         source_path: Optional[Path] = None,
#         *,
#         row_to_message: Optional[Callable[[Dict[str, Any]], Dict[str, Any]]] = None,
#         key_fn: Optional[Callable[[Dict[str, Any]], str]] = None,
#         delimiter: Optional[str] = None,
#         encoding: str = "utf-8-sig",
#     ):
#         load_dotenv()
#
#         # --- קביעת מקור הנתונים ---
#         self.project_root = Path(__file__).resolve().parents[2]
#         env_dir = os.getenv("DATA_DIR", "data")
#         data_dir = Path(env_dir) if Path(env_dir).is_absolute() else (self.project_root / env_dir)
#         # אם לא נמסר קובץ – נשתמש בברירת המחדל של הפרויקט הנוכחי (validated.tsv)
#         self.source_path = source_path or (data_dir / "corpus" / "validated.tsv")
#
#         if not self.source_path.exists():
#             raise FileNotFoundError(f"Source file not found: {self.source_path}")
#
#         # --- פונקציות הניתנות להחלפה ---
#         # ברירת מחדל (דומיין אודיו): המרת שורת TSV לאירוע ע"י schema.event_from_row().to_message()
#         self.row_to_message = row_to_message or (lambda row: event_from_row(row).to_message())
#         # ברירת מחדל למפתח: clip_id או id
#         self.key_fn = key_fn or (lambda msg: msg.get("clip_id") or msg.get("id") or "")
#
#         # --- תצורת קובץ טבלאי ---
#         # delimiter: אם לא נמסר – ננחש לפי סיומת (.tsv -> \t, אחרת ',')
#         if delimiter is None:
#             self.delimiter = "\t" if self.source_path.suffix.lower() == ".tsv" else ","
#         else:
#             self.delimiter = delimiter
#         self.encoding = encoding
#
#         # --- Kafka ---
#         self.topic = os.getenv("KAFKA_TOPIC_INGEST", "ingest.events")
#         broker = os.getenv("KAFKA_BROKER", "localhost:9092")
#         acks = os.getenv("KAFKA_ACKS", "all")
#
#         self.producer = KafkaProducer(
#             bootstrap_servers=[broker],
#             acks=acks,
#             linger_ms=50,
#             compression_type="gzip",
#             key_serializer=lambda k: k.encode("utf-8"),
#             value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
#         )
#
#         logger.info(f"Kafka producer connected to {broker}, topic='{self.topic}'")
#         logger.info(f"Using source: {self.source_path} (delimiter={'TAB' if self.delimiter=='\\t' else self.delimiter})")
#
#     # --- מקורות נתונים גמישים ---
#     def iter_rows(self) -> Iterator[Dict[str, Any]]:
#         """קריאת שורות מהקובץ הטבלאי (TSV/CSV) כדיקטים."""
#         with open(self.source_path, encoding=self.encoding, newline="") as f:
#             reader = csv.DictReader(f, delimiter=self.delimiter)
#             for row in reader:
#                 yield row
#
#     def iter_messages(self, limit: Optional[int] = None) -> Iterator[Dict[str, Any]]:
#         """המרת שורות להודעות באמצעות row_to_message."""
#         for i, row in enumerate(self.iter_rows(), start=1):
#             msg = self.row_to_message(row)
#             yield msg
#             if limit and i >= limit:
#                 break
#
#     # --- שליחה ---
#     def send(self, limit: Optional[int] = None) -> int:
#         sent = 0
#         for msg in self.iter_messages(limit=limit):
#             key = self.key_fn(msg)
#             if not key:
#                 logger.warning("Skipping message without key")
#                 continue
#             self.producer.send(self.topic, key=key, value=msg)
#             sent += 1
#             if sent % 500 == 0:
#                 logger.info(f"Progress: sent {sent} messages")
#
#         self.producer.flush()
#         logger.info(f"Done. Sent={sent} messages to topic='{self.topic}'")
#         return sent
#
#     def close(self):
#         try:
#             self.producer.flush()
#         finally:
#             self.producer.close()
#
#
# if __name__ == "__main__":
#     prod = IngestProducer()
#     try:
#         limit = int(os.getenv("INGEST_LIMIT", "0")) or None
#         prod.send(limit=limit)
#     finally:
#         prod.close()
