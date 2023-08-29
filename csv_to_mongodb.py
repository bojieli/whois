#!/usr/bin/python3

"""
DESCRIPTION
    Simple python script to import a csv into ElasticSearch. It can also update existing Elastic data if
    only parameter --id-column is provided

HOW IT WORKS
    The script creates an ElasticSearch API PUT request for
    each row in your CSV. It is similar to running an bulk insert by:

    $ curl -XPUT localhost:9200/_bulk -d '{index: "", type: ""}
                                         { data }'

    In both `json-struct` and `elastic-index` path, the script will
    insert your csv data by replacing the column name wrapped in '%'
    with the data for the given row. For example, `%id%` will be
    replaced with data from the `id` column of your CSV.

NOTES
    - CSV must have headers
    - insert elastic address (with port) as argument, it defaults to localhost:9200

EXAMPLES
    1. CREATE example:

    $ python csv_to_elastic.py \
        --elastic-address 'localhost:9200' \
        --csv-file input.csv \
        --elastic-index 'index' \
        --datetime-field=dateField

    CSV:

|  name  |      major       |
|--------|------------------|
|  Mike  |   Engineering    |
|  Erin  | Computer Science |


    2. CREATE/UPDATE example:

    $ python csv_to_elastic.py \
        --elastic-address 'localhost:9200' \
        --csv-file input.csv \
        --elastic-index 'index' \
        --datetime-field=dateField \
        --id-column id
CSV:

|  id  |  name  |      major       |
|------|--------|------------------|
|   1  |  Mike  |   Engineering    |
|   2  |  Erin  | Computer Science |

"""

import argparse
import http.client
import os
import csv
import json
import dateutil.parser
from datetime import datetime
from base64 import b64encode
from pymongo import MongoClient


def main(file_path, delimiter, max_rows, elastic_index, datetime_field, elastic_type, elastic_address, ssl, username, password, id_column):
    endpoint = '/_bulk'
    if max_rows is None:
      max_rows_disp = "all"
    else:
      max_rows_disp = max_rows

    print("")
    print(" ----- CSV to ElasticSearch ----- ")
    print("Importing %s rows into `%s` from '%s'" % (max_rows_disp, elastic_index, file_path))
    print("")

    count = 0
    headers = []
    headers_position = {}
    to_mongo_docs = []
    with open(file_path, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter=delimiter, quotechar='"')
        for row in reader:
            if count == 0:
                for iterator, col in enumerate(row):
                    headers.append(col)
                    headers_position[col] = iterator
            elif max_rows is not None and count >= max_rows:
                print('Max rows imported - exit')
                break
            #elif len(row[0]) == 0:    # Empty rows on the end of document
            #    print("Found empty rows at the end of document")
            #    break
            else:
                doc = {}
                pos = 0
                time_fields = ['query_time']
                date_fields = ['create_date', 'update_date', 'expiry_date']
                for header in headers:
                    doc[header] = row[pos]
                    if doc[header] == '':
                        doc[header] = None
                    if header == 'domain_name':
                        try:
                            doc['domain_word'], doc['domain_tld'] = doc[header].split('.', 1)
                        except:
                            print('Invalid domain ' + doc[header])
                    if header in time_fields and doc[header]:
                        try:
                            doc[header] = datetime.strptime(doc[header], "%Y-%m-%d %H:%M:%S")
                        except:  # date not parse-able, ignore
                            pass
                    if header in date_fields and doc[header]:
                        try:
                            doc[header] = datetime.strptime(doc[header], "%Y-%m-%d")
                        except:  # date not parse-able, ignore
                            pass
                    pos += 1
                to_mongo_docs.append(doc)

            count += 1
            if count % 10000 == 0:
                send_to_elastic(to_mongo_docs, count)
                to_mongo_docs = []
                print(count)

    print('Reached end of CSV - sending to Elastic')
    send_to_elastic(to_mongo_docs, count)
    print(count)

    print("Done.")


client = MongoClient('localhost', 27017)
db = client['whois']
collection = db['whois']

def send_to_elastic(to_mongo_docs, block=0):
    collection.insert_many(to_mongo_docs)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='CSV to ElasticSearch.')

    parser.add_argument('--elastic-address',
                        required=False,
                        type=str,
                        default='localhost:9200',
                        help='Your elasticsearch endpoint address')
    parser.add_argument('--ssl',
                        dest='ssl',
                        action='store_true',
                        required=False,
                        help='Use SSL connection')
    parser.add_argument('--username',
                        required=False,
                        type=str,
                        help='Username for basic auth (for example with elastic cloud)')
    parser.add_argument('--password',
                        required=False,
                        type=str,
                        help='Password')
    parser.add_argument('--csv-file',
                        required=True,
                        type=str,
                        help='path to csv to import')
    parser.add_argument('--elastic-index',
                        required=False,
                        type=str,
                        help='elastic index you want to put data in')
    parser.add_argument('--elastic-type',
                        required=False,
                        type=str,
                        default='text',
                        help='Your entry type for elastic')
    parser.add_argument('--max-rows',
                        type=int,
                        default=None,
                        help='max rows to import')
    parser.add_argument('--datetime-field',
                        type=str,
                        help='datetime field for elastic')
    parser.add_argument('--id-column',
                        type=str,
                        default=None,
                        help='If you want to have index and you have it in csv, this the argument to point to it')
    parser.add_argument('--delimiter',
                        type=str,
                        default=",",
                        help='If you want to have a different delimiter than ,')

    parsed_args = parser.parse_args()

    main(file_path=parsed_args.csv_file, delimiter = parsed_args.delimiter,
         elastic_index=parsed_args.elastic_index, elastic_type=parsed_args.elastic_type,
         datetime_field=parsed_args.datetime_field, max_rows=parsed_args.max_rows,
         elastic_address=parsed_args.elastic_address, ssl=parsed_args.ssl, username=parsed_args.username, password=parsed_args.password, id_column=parsed_args.id_column)
