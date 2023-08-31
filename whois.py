#!/usr/bin/python3
from flask import Flask, render_template, request, jsonify
from pymongo import MongoClient, ASCENDING, DESCENDING
from datetime import datetime

app = Flask(__name__)

client = MongoClient('localhost', 27017)  # Connecting to the local MongoDB instance
db = client['whois']  # The database
collection = db['whois']  # The collection

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

def format_date(record):
    date_fields = ['query_time', 'create_date', 'update_date', 'expiry_date']
    for field in date_fields:
        if record[field] and isinstance(record[field], datetime):
            record[field] = record[field].strftime("%Y-%m-%d %H:%M:%S")

def search_domain(domain_query):
    results = {}

    # Searching for exact matches in domain_name field
    for record in collection.find({"domain_name": domain_query}).sort("query_time", ASCENDING):
        domain_name = record['domain_name']
        if domain_name not in results:
            results[domain_name] = []

        record.pop('_id')
        format_date(record)
        results[domain_name].append(record)

    # Searching for exact matches in domain_word field
    for record in collection.find({"domain_word": domain_query}).sort("query_time", ASCENDING):
        domain_name = record['domain_name']
        if domain_name not in results:
            results[domain_name] = []

        record.pop('_id')
        format_date(record)
        results[domain_name].append(record)

    return results

@app.route('/get_domain_history', methods=['POST'])
def get_domain_history():
    query = request.form.get('domain')
    if not query:
        return jsonify({"error": "Domain query not provided"}), 400
    results = search_domain(query)
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)
