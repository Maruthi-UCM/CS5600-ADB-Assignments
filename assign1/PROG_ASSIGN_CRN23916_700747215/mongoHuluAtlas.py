from flask import Flask, Response, jsonify,request, redirect, render_template
from flask.helpers import url_for
from pymongo import MongoClient
from json2html import *

app = Flask(__name__)

# Connection details to MongoDB

client = MongoClient("mongodb+srv://imsurya19:imsurya19@cluster.fxkk1jm.mongodb.net/?retryWrites=true&w=majority")
db = client.DB
collection = db.Hulu
#print(client.server_info())
'''
# connection details for local host

mongo = MongoClient(
    host = 'localhost',
    port = 27017,
    serverSelectionTimeoutMS = 1000
    )
db = mongo.practice_db 
collection = db.practice_db
mongo.server_info() 
'''

# Method for GET api call to get all the records from the collection
@app.route('/api',methods=['GET'])
def retrive_records():
    try:
        # Fetching data from MongoDB collection
        cursor = collection.find()
        data = []
        for doc in cursor:
            doc['_id'] = str(doc['_id'])
            data.append(doc)
        #print(data)
        return jsonify(data)
    except Exception as ex:
        response = Response("Search Records Error!!",status=500,mimetype='application/json')
        return response

# Method for GET api call to get all the records from the collection where title is same as in the url
@app.route('/api/<fname>',methods=['GET'])
def retrive_record(fname):
    try:
        # Fetching data from MongoDB collection where title is same as fname
        cursor = collection.find({"title" : fname})
        data = []
        for doc in cursor:
            doc['_id'] = str(doc['_id'])
            data.append(doc)
        #print(data)
        return jsonify(data)
    except Exception as ex:
        response = Response("Search Records Error!!",status=500,mimetype='application/json')
        return response

# Method for POST api call to insert a new record in to the collection
@app.route('/api', methods=['POST'])
def new_record():
    try:
        record = request.get_json()
        #print(record)
        record_inserted = collection.insert_one(record)
        return {"ID_inserted" : str(record_inserted.inserted_id), "record" : str(record)}
    except:
        response = Response("Error Inserting Records!!",status=500,mimetype='application/json')
        return response


# Method for PATCH api call to Update an existing record in the collection with the title given in the url
@app.route('/api/<fname>', methods=['PATCH'])
def update_record(fname):
    try:
        record = request.get_json()
        #print(record)
        record_updated = collection.update_one({"title" : fname}, {"$set" : record})  # u[pdating the record where title is equal to fname
        #print("updated")
        return {"updated_record" : str(record)}
    except:
        response = Response("Error Updating Records!!",status=500,mimetype='application/json')
        return response


# Method for DELETE api call to delete a record from the collection with the title given in the url
@app.route('/api/<fname>', methods=['DELETE'])
def delete_record(fname):
    try:
        collection.delete_one({"title": fname})         # deleting the record where title is equal to fname
        return redirect(url_for('retrive_records'))
    except:
        response = Response("Error Deleting Records!!",status=500,mimetype='application/json')
        return response

@app.route('/api/chart', methods=['GET'])
def unique_record():
    try:
        records = collection.distinct("title")         # getting unique records
        unique = {'Alphabets':'No:of movies','A':0,'B':0,'C':0,'D':0,'E':0,'F':0,'G':0,'H':0,'I':0,'J':0,'K':0,'L':0,'M':0,'N':0,'O':0,'P':0,'Q':0,'R':0,'S':0,'T':0,'U':0,'V':0,'W':0,'X':0,'Y':0,'Z':0,'others':0}
        for i in records:
            if i[0].isalpha():
                unique[i[0]]+=1
            else:
                unique['others']+=1
        
        return json2html.convert(json=unique)
    except:
        response = Response("Error!!",status=500,mimetype='application/json')
        return response

if __name__ == '__main__':
    app.run(debug=True)