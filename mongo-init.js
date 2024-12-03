db = db.getSiblingDB('bank_impacta');

db.createCollection('bank');

json_file = fetch("./data/json/bank.json")

db.sample_collection.insertMany([json_file]);