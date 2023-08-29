from pymongo import MongoClient

# Establish a connection to the MongoDB instance
client = MongoClient('localhost', 27017)

# Select the 'whois' database and 'whois' collection
db = client['whois']
collection = db['whois']

# Currently no need to update the fields because it is already handled by the pipeline
#
## Define the pipeline for the update operation
#pipeline = [
#    {
#        "$set": {
#            "split_domain": { "$split": ["$domain_name", "."] }
#        }
#    },
#    {
#        "$set": {
#            "domain_word": { "$arrayElemAt": ["$split_domain", 0] },
#            "domain_tld": { "$arrayElemAt": ["$split_domain", 1] }
#        }
#    },
#    {
#        "$unset": "split_domain"
#    }
#]
#
## Use the update_many method with an aggregation pipeline to update the documents
#collection.update_many({}, pipeline)
#
#print("Update fields completed.")

collection.create_index("domain_name")

print("Create index domain_name completed.")

collection.create_index("domain_word")

print("Create index domain_word completed.")

dates = ['query_time', 'create_date', 'update_date', 'expiry_date']
for d in dates:
    collection.create_index(d)
    print("Create index " + d + " completed.")
