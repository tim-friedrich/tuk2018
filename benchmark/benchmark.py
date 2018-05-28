import pyhdb;
import random;
import os;
import time;
import sys;
import timeit;
from multiprocessing import Pool;
import argparse;
import pdb;


# path to the folder with the queries
query_path = "queries"
query_file_names = os.listdir(query_path)
queries = []
num_processes = 10
minutes = 1
num_repetitions = 10
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
    # query_file_names = os.listdir(query_path)
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

def benchmarkRandomQueries():
    query_count = 0
    for i in range(0, num_repetitions):
        with Pool(processes=num_processes) as p:
            query_count += sum(p.map(runRandomQueriesFor, [minutes]*num_processes))

    print("Total Number of executed Queries in average ", query_count/num_repetitions)

def benchmarkAllQueries():
    connection = openConnection()
    cursor = connection.cursor()
    for index, query in enumerate(queries):
        t1 = time.time()
        for i in range(0, num_repetitions):
            cursor.execute(query)
        t2 = time.time()
        avg_time = (t2 - t1) / num_repetitions
        print("Query: " + query_file_names[index] + " performed at an average of: " + str(avg_time) + " seconds")

loadQueries()

parser = argparse.ArgumentParser()
parser.add_argument('mode', choices=['all_random', 'single'])
args = parser.parse_args()
if(args.mode == 'single'):
    print("starting to benchmark all tpch queries...")
    benchmarkAllQueries()

if(args.mode == 'all_random'):
    print("starting to benchmark all tpch queries in a random order within: "+ str(minutes) +" minutes...")
    benchmarkRandomQueries()
