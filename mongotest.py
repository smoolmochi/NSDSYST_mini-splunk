import pymongo
import hashlib

# pymongo.MongoClient("mongodb://MONGODB_VM_IP:27017")
client = pymongo.MongoClient("mongodb://127.0.0.1:27017")
db = client["mini_splunk"]
logs = db["logs"]

sample_line = "Feb  7 16:03:34 SYSSVR1 sshd[1032662]: Accepted password for user27"
doc = {
    "_id": hashlib.sha256(sample_line.encode()).hexdigest(),
    "timestamp": "Feb  7 16:03:34",
    "host": "SYSSVR1",
    "daemon": "sshd[1032662]",
    "severity": "INFO",
    "message": "Accepted password for user27"
}

logs.insert_one(doc)
print(">> Inserted. Total docs now:", logs.count_documents({}))

# Try inserting the exact same line again — should be rejected as a duplicate, not crash
try:
    logs.insert_one(doc)
except pymongo.errors.DuplicateKeyError:
    print(">> Correctly rejected as duplicate")