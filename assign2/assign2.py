from flask import Flask, Response, request, render_template, jsonify
from py2neo import Graph, Node, Relationship
import pandas as pd


uri = "neo4j+s://997c5533.databases.neo4j.io"               #"neo4j+s://<Bolt url for Neo4j Aura instance>"
user = "neo4j"                                              #"<Username for Neo4j Aura instance>"
password = "eLNM-k2h9MQuXqYqgHfKNKF8Hgs2fy0X1xsLBZ-SinM"    #"<Password for Neo4j Aura instance>"

graph = Graph(uri, auth=(user, password))
#print(graph.)