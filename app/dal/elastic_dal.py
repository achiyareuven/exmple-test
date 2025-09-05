import os
from elasticsearch import Elasticsearch, exceptions
from elasticsearch.helpers import bulk, streaming_bulk
from app.utils.logger import get_logger


class ElasticDAL:
    def __init__(self, uri: str = None, index_name: str = None):
        self.logger = get_logger(__name__)

        # קריאה מ־.env או ברירת מחדל
        self.uri = uri or os.getenv("ES_URI", "http://localhost:9200")
        self.index_name = index_name or os.getenv("ES_INDEX", "texts")

        try:
            self.es = Elasticsearch(self.uri)
            if not self.es.ping():
                raise exceptions.ConnectionError("Elasticsearch is not responding")
            self.logger.info(f"Connected to Elasticsearch: {self.uri}, Index: {self.index_name}")

            # יצירת אינדקס אם לא קיים
            if not self.es.indices.exists(index=self.index_name):
                self.es.indices.create(index=self.index_name)
                self.logger.info(f"Created index: {self.index_name}")
        except Exception as e:
            self.logger.error(f"Failed to connect to Elasticsearch: {e}")
            raise

    def index_doc(self, doc_id: str, doc: dict):
        """אינדוקס מסמך לפי id"""
        try:
            result = self.es.index(index=self.index_name, id=doc_id, document=doc)
            self.logger.info(f"Indexed doc id={doc_id}")
            return result
        except Exception as e:
            self.logger.error(f"Indexing failed: {e}")
            raise

    def get_doc(self, doc_id: str) -> dict:
        """שליפת מסמך לפי id"""
        try:
            result = self.es.get(index=self.index_name, id=doc_id)
            self.logger.info(f"Got doc id={doc_id}")
            return result["_source"]
        except exceptions.NotFoundError:
            self.logger.warning(f"Doc id={doc_id} not found")
            return None
        except Exception as e:
            self.logger.error(f"Get failed: {e}")
            raise

    def search(self, query: str, field: str = "text") -> list:
        """חיפוש טקסט חופשי בשדה"""
        try:
            body = {
                "query": {
                    "match": {field: query}
                }
            }
            result = self.es.search(index=self.index_name, body=body)
            hits = [hit["_source"] for hit in result["hits"]["hits"]]
            self.logger.info(f"Searched for '{query}' -> {len(hits)} hits")
            return hits
        except Exception as e:
            self.logger.error(f"Search failed: {e}")
            raise

    def update_doc(self, doc_id: str, new_values: dict):
        """עדכון מסמך קיים"""
        try:
            result = self.es.update(index=self.index_name, id=doc_id, body={"doc": new_values})
            self.logger.info(f"Updated doc id={doc_id}")
            return result
        except Exception as e:
            self.logger.error(f"Update failed: {e}")
            raise

    def delete_doc(self, doc_id: str):
        """מחיקת מסמך לפי id"""
        try:
            result = self.es.delete(index=self.index_name, id=doc_id)
            self.logger.info(f"Deleted doc id={doc_id}")
            return result
        except exceptions.NotFoundError:
            self.logger.warning(f"Tried to delete non-existent doc id={doc_id}")
            return None
        except Exception as e:
            self.logger.error(f"Delete failed: {e}")
            raise



    # -------- BULK OPS --------
    def bulk_index(self, docs: list, id_field: str | None = None, refresh: bool = False) -> tuple[int, list]:
        """
        אינדוקס מהיר של רשימת מסמכים.
        docs: רשימת דיקטים. אם יש id_field – ישלוף ממנו את ה-id.
        מחזיר (num_success, errors)
        """
        actions = []
        for d in docs:
            _id = (d.get(id_field) if id_field else d.get("_id")) if isinstance(d, dict) else None
            if "_id" in d:
                d = {k: v for k, v in d.items() if k != "_id"}
            actions.append({
                "_op_type": "index",
                "_index": self.index_name,
                "_id": _id,
                "_source": d,
            })

        success, errors = bulk(self.es, actions, refresh=refresh, raise_on_error=False)
        self.logger.info(f"Bulk index -> success={success}, errors={len(errors)}")
        return success, errors

    def bulk_delete(self, ids: list[str], refresh: bool = False) -> tuple[int, list]:
        """
        מחיקת הרבה מסמכים לפי IDs
        """
        actions = [{"_op_type": "delete", "_index": self.index_name, "_id": _id} for _id in ids]
        success, errors = bulk(self.es, actions, refresh=refresh, raise_on_error=False)
        self.logger.info(f"Bulk delete -> success={success}, errors={len(errors)}")
        return success, errors

    def bulk_update(self, updates: list[dict], id_field: str = "_id", refresh: bool = False) -> tuple[int, list]:
        """
        עדכון חלקי (partial) בכמות.
        updates: רשימת פריטים בפורמט {"_id": "...", "doc": {...}}
                 אפשר לשנות id_field אם ה-id יושב בשם שדה אחר.
        """
        actions = []
        for u in updates:
            _id = u[id_field]
            doc = u["doc"]
            actions.append({
                "_op_type": "update",
                "_index": self.index_name,
                "_id": _id,
                "doc": doc
            })
        success, errors = bulk(self.es, actions, refresh=refresh, raise_on_error=False)
        self.logger.info(f"Bulk update -> success={success}, errors={len(errors)}")
        return success, errors

    def bulk_upsert(self, docs: list[dict], id_field: str, refresh: bool = False) -> tuple[int, list]:
        """
        UPSERT בכמות: אם קיים – יעדכן; אם לא – ייצור.
        docs: כל פריט חייב להכיל את שדה ה-id_field + תוכן המסמך
        """
        actions = []
        for d in docs:
            _id = d[id_field]
            src = {k: v for k, v in d.items() if k != id_field}
            actions.append({
                "_op_type": "update",
                "_index": self.index_name,
                "_id": _id,
                "doc": src,
                "doc_as_upsert": True
            })
        success, errors = bulk(self.es, actions, refresh=refresh, raise_on_error=False)
        self.logger.info(f"Bulk upsert -> success={success}, errors={len(errors)}")
        return success, errors

    def streaming_bulk_index(self, docs_iter, id_field: str | None = None, chunk_size: int = 500,
                             refresh: bool = False) -> tuple[int, int]:
        """
        זרימה (streaming) לכמויות ענק – מחזיר (ok, fail) מונים.
        docs_iter: איטרטור שמחזיר מסמכים
        """

        def _gen():
            for d in docs_iter:
                _id = (d.get(id_field) if id_field else d.get("_id")) if isinstance(d, dict) else None
                if "_id" in d:
                    d = {k: v for k, v in d.items() if k != "_id"}
                yield {
                    "_op_type": "index",
                    "_index": self.index_name,
                    "_id": _id,
                    "_source": d,
                }

        ok, fail = 0, 0
        for ok_flag, _ in streaming_bulk(self.es, _gen(), chunk_size=chunk_size, refresh=refresh, raise_on_error=False):
            ok += int(ok_flag)
            fail += int(not ok_flag)
        self.logger.info(f"Streaming bulk index -> ok={ok}, fail={fail}")
        return ok, fail
