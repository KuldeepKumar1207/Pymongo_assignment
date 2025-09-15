import pymongo
from pymongo import MongoClient,errors
from bson.objectid import ObjectId
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
import re
from datetime import datetime

client = MongoClient("mongodb://localhost:27017/") # Connect to mongo db
db=client['training_db'] #Create a database called "training_db"
collection = db['employees'] #Create a collection called "employees"

# Insert 5 sample employee records with fields: name, email, department, salary, join_date
collection.delete_many({}) # Clear existing records 
collection.insert_many([{'Name':'Kuldeep11',"Email":"Kul11@gmail.com","Department":"AI/ML","Salary":600000, "join_Date":"09/09/2025"},
                       {'Name':'Advait',"Email":"Kul22@gmail.com","Department":"AI","Salary":700000, "join_Date":"09/09/2025"},
                       {'Name':'Gautami',"Email":"Kul33@gmail.com","Department":"ML","Salary":800000, "join_Date":"09/09/2025"},
                       {'Name':'Deepti',"Email":"Kul44@gmail.com","Department":"AI/ML","Salary":600000, "join_Date":"09/09/2025"},
                       {'Name':'Abhijeet',"Email":"Kul55@gmail.com","Department":"AI","Salary":400000, "join_Date":"09/09/2025"}])

# function to List All Records
def fun():
    for i in collection.find():
        print(i)

fun()

# to search employees in specific ("AI") department
for i in collection.find({"Department":"AI"}):
    print(i)

# to find employees within salary range ( greater than 400000 and less than 800000 )
for i in collection.find({"Salary":{"$gt":400000,"$lt":800000}}):
    print(i)

# to find employees whose names contain specific substring ("e")
for i in collection.find({"Name":{"$regex":"e"}}):
    print(i)

# to combine multiple search criteria (Name contains "e" and Salary equals 600000)
for i in collection.find({"Name":{"$regex":"e"},"Salary":{"$eq":600000}}):
    print(i)

# to sort employees by salary (in ascending order)
for i in collection.find({}).sort({"Salary":1}):
    print(i)

# to sort employees by salary (in descending order)
for i in collection.find({}).sort({"Salary":-1}):
    print(i)

# to implement pagination (limit)
for i in collection.find({}).sort({"Salary":-1}).limit(1):
    print(i)

# to implement pagination (skip)
for i in collection.find({}).sort({"Salary":-1}).skip(2):
    print(i)


collection.create_index([("Email", ASCENDING)], unique=True) #  reating index on email to prevent duplicates
EMAIL_RE = re.compile(r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I) # email regex to filter valid emails
ALLOWED_DEPARTMENTS = {"AI", "AI/ML", "ML"} # allowed departments

try: # Single Record Insert with validations
    name, email, dept, jd = input("Enter name,email,department,join_date: ").split(",")
    salary = int(input("Enter salary: "))

    # validations for email 
    if not EMAIL_RE.match(email.strip()):
        raise ValueError("Invalid Email format")
    # validations for department
    if dept.strip() not in ALLOWED_DEPARTMENTS:
        raise ValueError(f"Department must be one of {ALLOWED_DEPARTMENTS}")
    doc = {
        "name": name.strip(),
        "email": email.strip().lower(),   # normalize for duplicate prevention
        "department": dept.strip(),
        "salary": salary,
        "join_date": datetime.strptime(jd.strip(), "%Y-%m-%d"),
        "created_at": datetime.utcnow()
    }

    collection.insert_one(doc)
    print("Single Record Inserted")

except errors.DuplicateKeyError:
    print("Duplicate email already exists")
except Exception as e:
    print("Error:", e)

for i in collection.find({}):
    print(i)

# Bulk Insert with validations
bulk_docs = [
    {"name": "Kuldeep", "email": "kuldeep@example.com", "department": "AI", "salary": 50000, "join_date": "2025-09-01"},
    {"name": "Asha", "email": "asha@example.com", "department": "ML", "salary": 60000, "join_date": "2025-08-15"},
    {"name": "InvalidDept", "email": "bad@example.com", "department": "Finance", "salary": 45000, "join_date": "2025-07-10"},
    {"name": "Dup", "email": "asha@example.com", "department": "AI/ML", "salary": 70000, "join_date": "2025-09-01"}
]
 # Validate and prepare documents for bulk insert
valid_docs = [] # to hold valid documents
for rec in bulk_docs:
    try:
        if not EMAIL_RE.match(rec["email"]):
            raise ValueError(f"Invalid email: {rec['email']}")
        if rec["department"] not in ALLOWED_DEPARTMENTS:
            raise ValueError(f"Invalid department: {rec['department']}")
        rec["email"] = rec["email"].lower()
        rec["join_date"] = datetime.strptime(rec["join_date"], "%Y-%m-%d")
        rec["created_at"] = datetime.utcnow()
        valid_docs.append(rec)
    except Exception as e:
        print("Skipping record:", rec, "| Reason:", e)
# Perform bulk insert of valid documents
if valid_docs:
    try:
        result = collection.insert_many(valid_docs, ordered=False)
        print("Bulk insert done. Inserted IDs:", result.inserted_ids)
    except errors.BulkWriteError as bwe:
        print("Bulk Insert Error:", bwe.details)

# Update Single Field: Update one field of an employee
collection.update_one({"Name":"Kuldeep11"},{"$set":{"Salary":9800000}})
for i in collection.find():
    print(i)

# Update Multiple Fields: Update several fields at once
collection.update_one({"Name":"Kuldeep11"},{"$set":{"Salary":800000,"Email":"Kul1v1@gmail.com",'Name':"Kuldeep_Kumar"}})
for i in collection.find():
    print(i)

# Update by ID: Update employee using ObjectId
collection.update_one({"_id":ObjectId('68c3b5a74c4841c1507d905e')},{"$set":{"Salary":800000,}})
for i in collection.find():
    print(i)

# Update by Criteria: Update multiple employees matching criteria
collection.update_many({"Department":"AI"},{"$inc":{"Salary":200000}})
for i in collection.find():
    print(i)

# Conditional Updates: Update only if certain conditions are met
collection.update_many({"Name":{"$regex":"i"}},[{"$set":{"Salary":{"$cond":{"if":{"$eq":["$Department","AI/ML"]},"then":0,"else":"$Salary"}}}}])
for i in collection.find():
    print(i)

# Add Modification Timestamp: Track when record was last modified
collection.update_many({"Name":{"$regex":"i"}},[{"$set":{"Salary":{"$cond":{"if":{"$eq":["$Department","AI/ML"]},"then":0,"else":"$Salary"}},"last_modified": datetime.utcnow()}}])
for i in collection.find():
    print(i)