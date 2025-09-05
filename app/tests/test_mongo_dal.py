from app.dal.mongo_dal import MongoDAL

def run_tests():
    # יוצרים אובייקט DAL (משתמש בברירות מחדל: localhost:27017, db=appdb, col=texts)
    dal = MongoDAL()

    # בדיקת יצירה
    doc_id = dal.create({"name": "Achiya", "role": "Tester"})
    print(f"Inserted ID: {doc_id}")

    # בדיקת קריאה
    doc = dal.read({"name": "Achiya"})
    print("Read result:", doc)

    # בדיקת עדכון
    updated_count = dal.update({"name": "Achiya"}, {"role": "Developer"})
    print(f"Updated {updated_count} documents")

    # בדיקת שליפת הכל
    docs = dal.list_all()
    print(f"Total documents in collection: {len(docs)}")

    # בדיקת מחיקה
    deleted_count = dal.delete({"name": "Achiya"})
    print(f"Deleted {deleted_count} documents")


if __name__ == "__main__":
    run_tests()