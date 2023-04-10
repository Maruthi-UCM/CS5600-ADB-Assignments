from flask import Flask, Response, request, render_template, jsonify,redirect
from flask.helpers import url_for
from py2neo import Graph, Node, Relationship


app =Flask(__name__)


url = "neo4j+s://49080325.databases.neo4j.io"               #"neo4j+s://<Bolt url for Neo4j Aura instance>"
user = "neo4j"                                              #"<Username for Neo4j Aura instance>"
password = "qAjlip_uzWU0dEYEV5f2MZSEkcomhFG_zfajNs4_v6w"    #"<Password for Neo4j Aura instance>"
try:
    graph = Graph(url, auth=(user, password))
except Exception as ex:
    print('Error in connection to database server', ex, '\n')

'''

#local host
url = "bolt://localhost:7687"               #"neo4j+s://<Bolt url for Neo4j Aura instance>"
user = "neo4j"                              #"<Username for Neo4j Aura instance>"
password = "Admin1234"                     #"<Password for Neo4j Aura instance>"
'''


# print(graph.query('match(n) return n limit 10'))
def generateQuery(json):
    query = """
                WITH $json as data
                UNWIND data as record
                MERGE (m:Movie {Title:record.Title}) ON CREATE
                SET m.ids = record.ids, m.year = record.year, m.runtime = record.runtime, m.description = record.description, m.rating = record.rating, m.votes = record.votes, m.revenue = record.revenue                       
                """
    if "Actors" in json:
        query = query + """
                                WITH $json as data
                                UNWIND data as record
                                UNWIND record.Title AS t
                                UNWIND SPLIT(record.actors,',') AS a
                                WITH *, trim(a) AS a2
                                MERGE (:Person{name:a2})
                                WITH * 
                                MATCH(m:Movie{Title:t}),(person:Person{name:a2})
                                MERGE (person)-[:ACTED_IN]->(m)
                        """
    if "Director" in json:
        query = query + """
                                WITH $json as data
                                UNWIND data as record
                                UNWIND record.Title AS t
                                UNWIND SPLIT(record.director,',') AS d
                                WITH *, trim(d) AS d2
                                MERGE (:Person{name:d2})
                                WITH * 
                                MATCH(m:Movie{Title:t}),(person:Person{name:d2})
                                MERGE (person)-[:DIRECTED]->(m)
                        """
    if "Genre" in json:
        query = query + """
                                WITH $json as data
                                UNWIND data as record
                                UNWIND record.Title AS t
                                UNWIND SPLIT(record.genre,',') AS c
                                WITH *, trim(c) AS c2
                                MERGE (:Genres{type:c2})
                                WITH * 
                                MATCH(m:Movie{Title:t}),(genre:Genres{type:c2})
                                MERGE (genre)<-[:DISTRIBUTED_IN]-(m)
                        """
    query = query + """
                RETURN collect(distinct m.Title) AS Title"""
    return query

# Method for GET api call to get all the records from the collection
@app.route('/imdb',methods=['GET'])
def retrive_records():
    # Fetching data from NEO4j
    try:
        query = """
        MATCH (m:Movie)
        OPTIONAL MATCH (m:Movie)-[:IN]->(g:Genre)
        OPTIONAL MATCH (p:Person)-[:ACTED_IN]->(m)
        OPTIONAL MATCH (d:Person)-[:DIRECTED]->(m)
        RETURN m.Ids AS id, m.Title AS title, m.Description AS description, m.Year AS year, m.Runtime AS runtime, m.Rating AS rating, m.Votes AS votes, m.Revenue AS revenue, split(g.Genre,',') AS genre, collect(DISTINCT d.Director) AS director, split(p.Actors,',') AS actors
        """

        result = graph.run(query).data()

        if len(result) == 0:
            return jsonify({"message": "No movies found, please add a movie first"})

        return jsonify(result)
        
    except Exception as ex:
        response = Response("Search Records Error!!",status=500,mimetype='application/json')
        return response

# Method for GET api call to get all the records from the collection where title is same as in the url
@app.route('/imdb/<fname>',methods=['GET'])
def retrive_record(fname):
    try:
        # Fetching data from NEO4j
        query = """  
                        MATCH (m:Movie)
                        WHERE toLower(m.Title) CONTAINS toLower('{}')
                        OPTIONAL MATCH (m:Movie)<-[:ACTED_IN]-(actors:Person)
                        OPTIONAL MATCH (m:Movie)<-[:DIRECTED]-(director:Person) 
                        OPTIONAL MATCH (genre:Genres)<-[IN]-(m:Movie)
                        RETURN m.Ids AS id, m.Title AS title, m.Description AS description, m.Year AS year, m.Runtime AS runtime, m.Rating AS rating, m.Votes AS votes, m.Revenue AS revenue, split(g.Genre,',') AS genre, collect(DISTINCT d.Director) AS director, split(p.Actors,',') AS actors""".format(fname)

        result = graph.run(query, fname=fname).data()

        if len(result) == 0:
            return jsonify({"message": "No movie found"})

        return jsonify(result)
    except Exception as ex:
        response = Response("Search Records Error!!",status=500,mimetype='application/json')
        return response

# Method for POST api call to insert a new record in to the collection
@app.route('/imdb', methods=['POST'])
def new_record():
    try:
        request_data = request.get_json()
        title_list = []
        if request_data != {} and request_data != []:
            if type(request_data) == list:
                for each in request_data:
                    query = generateQuery(each)
                    nodes = graph.run(query, json=each)
                    nodes_data = list(nodes.data())
                    title_list.append(nodes_data)
            elif type(request_data) == dict:
                query = generateQuery(request_data)
                nodes = graph.run(query, json=request_data)
                nodes_data = list(nodes.data())
                title_list.append(nodes_data)

            if title_list != []:
                response = Response(
                    f"Movies are Succesfully Created", status=200, mimetype='application/json')
                return response
            else:
                response = Response("Error in Creation",
                                    status=200, mimetype='application/json')
                return response
        else:
            response = Response("Error: Please Check JSON Data",
                                status=500, mimetype='application/json')
            return response

    except Exception as ex:
        response = Response(
            f"Error in Create Operation: {ex}", status=500, mimetype="application/json")
        return response


# Method for PATCH api call to Update an existing record in the collection with the title given in the url
@app.route('/imdb/<fname>', methods=['PATCH'])
def update_record(fname):
    try:
        request_data = request.get_json()
        if request_data != {}:
            query = """
                                MATCH (m:Movie {Title: $fname})
                                Return m as ShowInfo
                                """
            movie_node = graph.run(query, fname=fname)
            movie_data = list(movie_node.data())
            if (movie_data != {} and movie_data != '' and movie_data != []):
                for each in movie_data:
                    movie_title = request_data['Title'] if 'Title' in request_data else each['ShowInfo']['Title']
                    movie_description = request_data['Description'] if 'Description' in request_data else each['ShowInfo']['Description']
                    movie_rating = request_data['Rating'] if 'Rating' in request_data else each['ShowInfo']['Rating']
                    updateQuery = """
                                                        MATCH (m:Movie {Title: $fname})
                                                        SET m.title = $movie_Title, m.Description = $movie_description, m.Rating = $movie_rating
                                                        RETURN m as ShowInfo
                                                        """
                    updated_node = graph.run(updateQuery, fname=fname, movie_title=movie_title,
                                               movie_description=movie_description, movie_rating=movie_rating)
                    updated_data = list(updated_node.data())
                    return jsonify(updated_data)

            else:
                response = Response(
                    f"No Movie Found with title: {fname}", status=400, mimetype="application/json")
                return response
        else:
            response = Response("No Raw JSON Found in Body to Update, please provide only title, description, rating to update",
                                status=400, mimetype="application/json")
            return response
    except Exception as ex:
        response = Response(
            f"Error in Update Operation: {ex}", status=500, mimetype="application/json")
        return response


# Method for DELETE api call to delete a record from the collection with the title given in the url
@app.route('/imdb/<fname>', methods=['DELETE'])
def delete_record(fname):
    try:
        # deleting the record where title is equal to fname
        query = """
        MATCH (m:Movie) WHERE toLower(m.Title) CONTAINS toLower({}) RETURN m.show_id as show_id
        """.format(fname)
        movie = graph.run(query, fname=fname).data()

        if len(movie) == 0:
            return jsonify({"message": "No movie found"})

        deleteQuery = """
        MATCH (m:Movie) 
        WHERE m.Title = {}
        OPTIONAL MATCH (m)-[r]-() 
        DELETE r,m
        """.format(fname)

        graph.run(deleteQuery, fname=movie[0]['Title'])

        return redirect(url_for('retrive_records'))
    except:
        response = Response("Error Deleting Records!!",status=500,mimetype='application/json')
        return response

if __name__ == '__main__':
    app.run(debug=True)