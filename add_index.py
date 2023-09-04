from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor

# Establish a connection to the MongoDB instance
client = MongoClient('localhost', 27017)

# Select the 'whois' database and 'whois' collection
db = client['whois']
collection = db['whois']

def create_index(field):
    collection.create_index([(field, 1)])

fields_to_index = ['domain_name', 'domain_word', 'query_time', 'create_date', 'update_date', 'expiry_date']

print('Start create indexes...')
print(fields_to_index)
with ThreadPoolExecutor() as executor:
    executor.map(create_index, fields_to_index)

print('Finished create indexes')
