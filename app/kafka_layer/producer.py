import os, csv, json
from pathlib import Path
from dotenv import load_dotenv
from kafka import KafkaProducer
from app.processing.schema import event_from_row
from app.utils.logger import get_logger


class IngestProducer:
    """
    Producer מינימלי:
    - קורא validated.tsv מתוך DATA_DIR/corpus
    - ממיר כל שורה לאירוע (Schema)
    - שולח ל- Kafka topic (key = clip_id)
    """

    def __init__(self):
        load_dotenv()
        self.logger = get_logger(__name__)

        # נתיב נתונים (יחסי או מוחלט) + קובץ ה-TSV
        project_root = Path(__file__).resolve().parents[2]
        env_dir = os.getenv("DATA_DIR", "data")
        data_dir = Path(env_dir) if Path(env_dir).is_absolute() else (project_root / env_dir)
        self.validated_tsv = data_dir / "corpus" / "validated.tsv"

        # הגדרות Kafka
        self.topic = os.getenv("KAFKA_TOPIC_INGEST", "audio.ingested")
        broker = os.getenv("KAFKA_BROKER", "localhost:9092")
        acks = os.getenv("KAFKA_ACKS", "all")

        if not self.validated_tsv.exists():
            raise FileNotFoundError(f"validated.tsv not found at: {self.validated_tsv}")

        # יצירת producer
        self.producer = KafkaProducer(
            bootstrap_servers=[broker],
            acks=acks,
            linger_ms=50,
            compression_type="gzip",
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        )

        self.logger.info(f"Kafka producer connected to {broker}, topic='{self.topic}'")
        self.logger.info(f"Using TSV: {self.validated_tsv}")

    def iter_events(self, limit: int | None = None):
        """מייצר אירועים מה-TSV (עד limit אם הוגדר)."""
        with open(self.validated_tsv, encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for i, row in enumerate(reader, start=1):
                event = event_from_row(row).to_message()
                yield event
                if limit and i >= limit:
                    break

    def send(self, limit: int | None = None) -> int:
        """שולח את האירועים לטופיק; מחזיר כמה נשלחו."""
        sent = 0
        for ev in self.iter_events(limit=limit):
            self.producer.send(self.topic, key=ev["clip_id"], value=ev)
            sent += 1
            if sent % 500 == 0:
                self.logger.info(f"Progress: sent {sent} messages")
        self.producer.flush()
        self.logger.info(f"Done. Sent={sent} messages to topic='{self.topic}'")
        return sent

    def close(self):
        try:
            self.producer.flush()
        finally:
            self.producer.close()


prod = IngestProducer()
try:
    prod.send(limit=int(os.getenv("INGEST_LIMIT", "0")) or None)
finally:
    prod.close()
