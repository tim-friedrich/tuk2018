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
duration = 1
num_repetitions = 10
random.seed("asd");
parser = argparse.ArgumentParser()


# HANA connection configurations
# host = "192.168.31.57"
# port = 30015
# user = "SYSTEM"
# password = "manager"

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

        while(time.time() - start_time <= minutes*60):
            executeRandomQuery(connection)
            number_queries += 1
        return number_queries
    except:
        print( "Unexpected error:", sys.exc_info()[0])

def benchmarkRandomQueries():
    query_count = 0
    for i in range(0, num_repetitions):
        with Pool(processes=num_processes) as p:
            query_count += sum(p.map(runRandomQueriesFor, [duration]*num_processes))

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

def addCommandLineArguments():
    parser.add_argument('mode', choices=['all_random', 'single'])
    ### General script config ###
    parser.add_argument('--query-folder', default="queries", help="The folder where the .sql file are stored used for the benchmark.")
    parser.add_argument('--seed', default="testseed", help="The seed used for random selections")
    parser.add_argument('--repetitions', type=int, default=10, help="The number of times the benchmark is repeated")
    parser.add_argument('--processes', type=int, default=15, help="The number of processes used for parallel query execution")
    parser.add_argument('--duration', help="The duration in minutes until the test is stopped. Currently used for random execution.")
    ### connection configuration ###
    parser.add_argument('--host', help="The database host as url or ip address")
    parser.add_argument('--port', type=int, help="The database port")
    parser.add_argument('--user', help="The database user")
    parser.add_argument('--password', help="The database password")

if __name__ == '__main__':
    loadQueries()
    addCommandLineArguments()
    args = parser.parse_args()
    query_path = args.query_folder
    random.seed(args.seed);
    num_repetitions = args.repetitions
    duration = args.duration
    num_processes = args.processes

    host = args.host
    port = args.port
    user = args.user
    password = args.password

    query_file_names = os.listdir(query_path)
    if(args.mode == 'single'):
        print("Starting to benchmark all tpch queries...")
        benchmarkAllQueries()

    if(args.mode == 'all_random'):
        print("Starting to benchmark all tpch queries in a random order within: " + str(duration) + " minutes...")
        benchmarkRandomQueries()
