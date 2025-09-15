import pymongo
from pymongo import MongoClient,errors
from bson.objectid import ObjectId
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
import re
from datetime import datetime
from pprint import pprint

# Task – 1: Create a Python script to establish connection with MongoDB and set up a sample database.
# Connect to MongoDB (assume default localhost)

client = MongoClient("mongodb://localhost:27017/")

# Task – 1: Create a Python script to establish connection with MongoDB and set up a sample database.
# Create a database called "training_db"

db=client['training_db']

# Task – 1: Create a Python script to establish connection with MongoDB and set up a sample database.
# Create a collection called "employees"

collection = db['employees']

# Task – 1: Create a Python script to establish connection with MongoDB and set up a sample database.
# Insert 5 sample employee records with fields: name, email, department, salary, join_date

collection.insert_many
(
    [
        {
            'Name':'Kuldeep11',
            "Email":"Kul11@gmail.com",
            "Department":"AI/ML",
            "Salary":600000,
            "join_Date":"09/09/2025"
        },
        {
            'Name':'Advait',
            "Email":"Kul22@gmail.com",
            "Department":"AI",
            "Salary":700000, 
            "join_Date":"09/09/2025"
        },
        {
            'Name':'Gautami',
            "Email":"Kul33@gmail.com",
            "Department":"ML",
            "Salary":800000, 
            "join_Date":"09/09/2025"
        },
        {
            'Name':'Deepti',
            "Email":"Kul44@gmail.com",
            "Department":"AI/ML",
            "Salary":600000, 
            "join_Date":"09/09/2025"
        },
        {
            'Name':'Abhijeet',
            "Email":"Kul55@gmail.com",
            "Department":"AI",
            "Salary":400000, 
            "join_Date":"09/09/2025"
        }
    ]
)


# Task – 2: Implement various search and listing functionalities.
# List All Records: Function to display all employees
for i in collection.find():
    pprint(i)


# Task – 2: Implement various search and listing functionalities.
# Search by Department: Find employees in specific department
for i in collection.find( { "Department" : "AI" } ):
    pprint(i)


# Task – 2: Implement various search and listing functionalities.
# Search by Salary Range: Find employees within salary range
for i in collection.find( { "Salary" : { "$gt" : 400000 , "$lt" : 800000 } } ):
    pprint(i)


# Task – 2: Implement various search and listing functionalities.
# Search by Name Pattern: Find employees whose names contain specific substring
for i in collection.find ( { "Name" : { "$regex" : "e" } } ):
    pprint(i)


# Task – 2: Implement various search and listing functionalities.
# Advanced Search: Combine multiple search criteria
for i in collection.find ( { "Name" : { "$regex" : "e" } , "Salary" : { "$eq" : 600000 } } ):
    pprint(i)


# Task – 2: Implement various search and listing functionalities.
# Sort Results: Sort employees by salary (ascending)
for i in collection.find( { } ).sort( { "Salary" : 1 } ):
    pprint(i)


# Task – 2: Implement various search and listing functionalities.
# Sort Results: Sort employees by salary (descending)
for i in collection.find( { } ).sort( { "Salary" : -1 } ):
    pprint(i)


# Task – 2: Implement various search and listing functionalities.
# Limit Results: Implement pagination (limit and skip)
for i in collection.find( { } ).sort( { "Salary" : -1 } ).limit( 1 ): # using limit
    pprint(i)

for i in collection.find( { } ).sort ( { "Salary" : -1 } ).skip( 2 ): # using skip
    pprint(i)


# Task 3:Implement functions to add new records with validation.
# Duplicate Prevention: Prevent duplicate email addresses

collection.create_index( [ ( "Email" , ASCENDING ) ], unique=True ) #  creating index on email to prevent duplicates
email_re = re.compile( r"^[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}$", re.I ) # email regex to filter valid emails
all_Dept = { "AI" , "AI/ML" , "ML" } # allowed departments


# Task 3:Implement functions to add new records with validation.
# Single Insert: Add one employee record

try: # Single Record Insert with validations
    name, email, dept, jd = input("Enter name,email,department,join_date: ").split(",")
    salary = int(input("Enter salary: "))


    # Task 3:Implement functions to add new records with validation.
    # Input Validation: Validate data before insertion

    # validations for email
    if not email_re.match(email.strip()):
        raise ValueError("Invalid Email format")
    # validations for department
    if dept.strip() not in all_Dept:
        raise ValueError(f"Department must be one of {all_Dept}")
    

    doc = {
        "Name": name.strip(),
        "Email": email.strip(),
        "Department": dept.strip(),
        "Salary": salary,
        "join_Date": jd.strip(),
        "created_at": datetime.utcnow()
    }

    collection.insert_one(doc)
    pprint("Single Record Inserted")

except errors.DuplicateKeyError:
    pprint("Duplicate email already exists")
except Exception as e:
    pprint("Error:", e)

for i in collection.find( { } ):
    pprint(i)


# Task 3:Implement functions to add new records with validation.
# Bulk Insert: Add multiple employee records

bulk_docs = [
    {
        "Name": "Kuldeep",
        "Email": "kuldeep@example.com", 
        "Department": "AI", 
        "Salary": 50000, 
        "join_Date": "09/01/2025"
        },
    {
        "Name": "Asha",
        "Email": "asha@example.com", 
        "Department": "ML", 
        "Salary": 60000, 
        "join_Date": "08/15/2025"
        },
    {
        "Name": "InvalidDept", 
        "Email": "bad@example.com", 
        "Department": "Finance", 
        "Salary": 45000, 
        "join_Date": "07/10/2025"
        },
    {
        "Name": "Dup", 
        "Email": "asha@example.com", 
        "Department": "AI/ML", 
        "Salary": 70000, 
        "join_Date": "09/01/2025"
        }
]


 # Validate and prepare documents for bulk insert
valid_docs = [] # to hold valid documents
for rec in bulk_docs:
    try:
        if not email_re.match( rec ["Email"] ):
            raise ValueError(f"Invalid email: { rec [ 'Email' ] } ")
        if rec["Department"] not in all_Dept:
            raise ValueError(f"Invalid department: { rec [ 'Department' ] } ")
        rec["Email"] = rec["Email"].lower()
        rec["join_Date"] = rec["join_Date"]
        rec["created_at"] = datetime.utcnow()
        valid_docs.append(rec)
    except Exception as e:
        print("Skipping record:", rec, "| Reason:", e)
# Perform bulk insert of valid documents
if valid_docs:
    try:
        result = collection.insert_many(valid_docs, ordered=False)
        pprint("Bulk insert done. Inserted IDs:", result.inserted_ids)
    except errors.BulkWriteError as bwe:
        pprint("Bulk Insert Error:", bwe.details)


# Task -4: Implement various update operations with different scenarios.
# Update Single Field: Update one field of an employee

collection.update_one(
    {
        "Name":"Kuldeep11"
        },
    {
        "$set":{"Salary":9800000}
        }
)
for i in collection.find():
    pprint(i)


# Task -4: Implement various update operations with different scenarios.
# Update Multiple Fields: Update several fields at once

collection.update_one(
    {
        "Name":"Kuldeep11"
    },
    {
        "$set":
        {
            "Salary":800000,
            "Email":"Kul1v1@gmail.com",
            'Name':"Kuldeep_Kumar"
        }
    }
)

for i in collection.find():
    pprint(i)


# Task -4: Implement various update operations with different scenarios.
# Update by ID: Update employee using ObjectId

collection.update_one(
    {
        "_id":ObjectId('68c3b5a74c4841c1507d905e')
    },
        {
            "$set":
                {
                    "Salary":800000
                }
        }
)
for i in collection.find():
    pprint(i)


# Task -4: Implement various update operations with different scenarios.
# Update by Criteria: Update multiple employees matching criteria

collection.update_many( { "Department":"AI" } , { "$inc" : { "Salary" : 200000 } } )
for i in collection.find():
    pprint(i)


# Task -4: Implement various update operations with different scenarios.
# Conditional Updates: Update only if certain conditions are met

collection.update_many(
    {
        "Name":
            {
                "$regex":"i"
            }
    },
    [
        {
            "$set":
            {
                "Salary":
                {
                    "$cond":
                    {
                        "if":
                        {
                            "$eq":
                            [
                                "$Department","AI/ML"
                            ]
                        },
                        "then":0,"else":"$Salary"
                    }
                }
            }
        }
    ]
)

for i in collection.find():
    pprint(i)


# Task -4: Implement various update operations with different scenarios.
# Add Modification Timestamp: Track when record was last modified
collection.update_many(
    {
        "Name":
        {
            "$regex":"i"
        }
    },
    [
        {
            "$set":
            {
                "Salary":
                {
                    "$cond":
                    {
                        "if":
                        {
                            "$eq":
                            [
                                "$Department","AI/ML"
                            ]
                        },
                        "then":0,"else":"$Salary"
                    }
                },
                "last_modified": datetime.utcnow()
            }
        }
    ]
)


for i in collection.find():
    pprint(i)