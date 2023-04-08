from flask import Flask, Response, request, render_template, jsonify
from py2neo import Graph, Node, Relationship
import pandas as pd


url = "neo4j+s://49080325.databases.neo4j.io"               #"neo4j+s://<Bolt url for Neo4j Aura instance>"
user = "neo4j"                                              #"<Username for Neo4j Aura instance>"
password = "qAjlip_uzWU0dEYEV5f2MZSEkcomhFG_zfajNs4_v6w"    #"<Password for Neo4j Aura instance>"

graph = Graph(url, auth=(user, password))

#print(graph.query('match(n) return n limit 10'))