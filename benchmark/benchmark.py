import pyhdb;
import random;
import os;
import time;
import sys;
from multiprocessing import Pool

# path to the folder with the queries
query_path = "queries"
query_file_names = []
queries = []
num_processes = 5
minutes = 1
random.seed("asd");

# HANA connection configurations
host = "192.168.31.57"
port = 30015
user = "SYSTEM"
password = "manager"

def openConnection():
    return pyhdb.connect(host=host, port=port, user=user, password=password)

def getQueryFrom(filename):
    fd = open(query_path +"/"+filename, 'r')
    query = fd.read()
    fd.close()
    return query

# loads all queries from the files in query_path into an array
def loadQueries():
    query_file_names = os.listdir(query_path)
    for query_file in query_file_names:
        queries.append(getQueryFrom(query_file))

# executes a random query from the queries array
def executeRandomQuery(conn):
    query = random.choice(queries)
    conn.cursor().execute(query)

# Executes a random selection of queries until a time threshold is reached
def runRandomQueriesFor(minutes):
    try:
        connection = openConnection()
        start_time = time.time()
        number_queries = 0

        while(time.time()-start_time <= minutes*60):
            executeRandomQuery(connection)
            number_queries += 1
        return number_queries
    except:
        print( "Unexpected error:", sys.exc_info()[0])


loadQueries()
with Pool(processes=num_processes) as p:
        print("Total Number of executed Queries", sum(p.map(runRandomQueriesFor, [minutes]*num_processes)))
