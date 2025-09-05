import os
import uuid
import pytest
from elasticsearch import exceptions as es_exceptions

# נבדוק שהמודול נטען
from app.dal.elastic_dal import ElasticDAL


@pytest.fixture(scope="session")
def es_uri():
    return os.getenv("ES_URI", "http://localhost:9200")


@pytest.fixture(scope="session")
def temp_index_name():
    # אינדקס ייחודי לסשן הטסט
    return f"test_texts_{uuid.uuid4().hex[:8]}"


@pytest.fixture(scope="session")
def dal(es_uri, temp_index_name):
    """
    יוצר ElasticDAL לאינדקס זמני.
    אם אין חיבור ל-ES – מדלגים על כל הסשן.
    """
    try:
        instance = ElasticDAL(uri=es_uri, index_name=temp_index_name)
        return instance
    except Exception as e:
        pytest.skip(f"Skipping ES tests: {e}")


@pytest.fixture(autouse=True)
def _clean_between_tests(dal):
    """
    לפני כל טסט נוודא שהאינדקס קיים (נוצר ב-__init__), וננקה מסמכים קיימים (delete by query).
    """
    try:
        dal.es.delete_by_query(
            index=dal.index_name,
            body={"query": {"match_all": {}}},
            ignore_unavailable=True
        )
        dal.es.indices.refresh(index=dal.index_name)
    except es_exceptions.NotFoundError:
        pass
    yield


def test_connection_and_index_created(dal):
    assert dal.es.ping() is True
    assert dal.es.indices.exists(index=dal.index_name) is True


def test_index_and_get_doc(dal):
    doc_id = "a1"
    src = {"text": "hello world", "lang": "en"}
    dal.index_doc(doc_id, src)
    dal.es.indices.refresh(index=dal.index_name)

    got = dal.get_doc(doc_id)
    assert got is not None
    assert got["text"] == "hello world"


def test_search(dal):
    dal.index_doc("s1", {"text": "shalom olam", "lang": "he"})
    dal.index_doc("s2", {"text": "hello world", "lang": "en"})
    dal.es.indices.refresh(index=dal.index_name)

    hits = dal.search("shalom", field="text")
    assert len(hits) == 1
    assert hits[0]["lang"] == "he"


def test_update_doc(dal):
    dal.index_doc("u1", {"text": "old text", "status": "new"})
    dal.es.indices.refresh(index=dal.index_name)

    dal.update_doc("u1", {"status": "updated"})
    dal.es.indices.refresh(index=dal.index_name)

    got = dal.get_doc("u1")
    assert got["status"] == "updated"


def test_delete_doc(dal):
    dal.index_doc("d1", {"text": "to delete"})
    dal.es.indices.refresh(index=dal.index_name)

    dal.delete_doc("d1")
    dal.es.indices.refresh(index=dal.index_name)

    assert dal.get_doc("d1") is None


def test_bulk_index(dal):
    docs = [
        {"_id": "b1", "text": "doc one", "tag": "x"},
        {"_id": "b2", "text": "doc two", "tag": "x"},
        {"_id": "b3", "text": "doc three", "tag": "y"},
    ]
    ok, errors = dal.bulk_index(docs)
    assert ok >= 3
    assert len(errors) == 0

    dal.es.indices.refresh(index=dal.index_name)
    hits_x = dal.search("doc", field="text")
    assert len(hits_x) == 3


def test_bulk_update(dal):
    dal.bulk_index([
        {"_id": "u2", "text": "doc u2", "status": "new"},
        {"_id": "u3", "text": "doc u3", "status": "new"},
    ])
    dal.es.indices.refresh(index=dal.index_name)

    ok, errors = dal.bulk_update([
        {"_id": "u2", "doc": {"status": "done"}},
        {"_id": "u3", "doc": {"status": "done"}},
    ])
    assert ok >= 2 and len(errors) == 0

    dal.es.indices.refresh(index=dal.index_name)
    assert dal.get_doc("u2")["status"] == "done"
    assert dal.get_doc("u3")["status"] == "done"


def test_bulk_upsert(dal):
    # upsert למסמך חדש
    ok, errors = dal.bulk_upsert([
        {"mongo_id": "m1", "text": "first m1", "v": 1},
    ], id_field="mongo_id")
    assert ok >= 1 and len(errors) == 0

    # upsert לעדכון קיים
    ok, errors = dal.bulk_upsert([
        {"mongo_id": "m1", "text": "updated m1", "v": 2},
    ], id_field="mongo_id")
    assert ok >= 1 and len(errors) == 0

    dal.es.indices.refresh(index=dal.index_name)

    # שליפה לפי id ה"אלסטי" תהיה mongo_id שקבענו
    got = dal.get_doc("m1")
    assert got["text"] == "updated m1"
    assert got["v"] == 2


def test_bulk_delete(dal):
    dal.bulk_index([
        {"_id": "del1", "text": "to delete 1"},
        {"_id": "del2", "text": "to delete 2"},
    ])
    dal.es.indices.refresh(index=dal.index_name)

    ok, errors = dal.bulk_delete(["del1", "del2"])
    assert ok >= 2 and len(errors) == 0

    dal.es.indices.refresh(index=dal.index_name)
    assert dal.get_doc("del1") is None
    assert dal.get_doc("del2") is None


def test_streaming_bulk_index(dal):
    def gen():
        for i in range(150):
            yield {"_id": f"s{i}", "text": f"stream {i}", "kind": "stream"}

    ok, fail = dal.streaming_bulk_index(gen(), chunk_size=50)
    assert ok >= 150 and fail == 0

    dal.es.indices.refresh(index=dal.index_name)
    hits = dal.search("stream", field="text")
    assert len(hits) >= 150


@pytest.fixture(scope="session", autouse=True)
def _teardown_index(request, es_uri, temp_index_name):
    """
    בסוף כל הסשן – מחיקת האינדקס הזמני (אם נוצר).
    """
    dal = None
    try:
        dal = ElasticDAL(uri=es_uri, index_name=temp_index_name)
    except Exception:
        # אם לא הצלחנו להתחבר בסשן – אין מה לנקות
        return

    def fin():
        try:
            if dal.es.indices.exists(index=temp_index_name):
                dal.es.indices.delete(index=temp_index_name, ignore=[404])
        except Exception:
            pass

    request.addfinalizer(fin)
