import pyhdb;
import os;
import time;
import sys;
import timeit;
from multiprocessing import Pool;
import argparse;
import pdb;
from connectionManager import ConnectionManager
from queryManager import QueryManager

parser = argparse.ArgumentParser()
query_manager = None

# executes a random query from the queries array
def executeRandomQuery(connection):
    connection.executeQuery(
        query_manager.getRandomQuery()
    )

# Executes a random selection of queries until a time threshold is reached
def runRandomQueriesFor(minutes):
    try:
        connection = ConnectionManager(host, port, user, password)
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
    connection = ConnectionManager(host, port, user, password)
    for query in query_manager.queries:
        t1 = time.time()
        for i in range(0, num_repetitions):
            connection.executeQuery(query)
        t2 = time.time()
        avg_time = (t2 - t1) / num_repetitions
        print("Query: " + query_manager.getNameFor(query) + " performed at an average of: " + str(avg_time) + " seconds")

def addCommandLineArguments():
    parser.add_argument('mode', choices=['all_random', 'single'])
    ### General script config ###
    parser.add_argument(
        '--query-folder',
        default="queries",
        help="The folder where the .sql file are stored used for the benchmark.")
    parser.add_argument(
        '--seed',
        default="testseed", help="The seed used for random selections")
    parser.add_argument(
        '--repetitions',
        type=int, default=10,
        help="The number of times the benchmark is repeated")
    parser.add_argument(
        '--processes',
        type=int, default=15,
        help="The number of processes used for parallel query execution")
    parser.add_argument(
        '--duration',
        type = int,
        default = 1,
        help="The duration in minutes until the test is stopped. Currently used for random execution.")

    ### connection configuration ###
    parser.add_argument(
        '--host',
        help="The database host as url or ip address")
    parser.add_argument(
        '--port',
        type=int, help="The database port")
    parser.add_argument(
        '--user',
        help="The database user")
    parser.add_argument(
        '--password',
        help="The database password")

if __name__ == '__main__':
    # loadQueries()
    addCommandLineArguments()
    args = parser.parse_args()
    num_repetitions = args.repetitions
    duration = args.duration
    num_processes = args.processes
    
    host = args.host
    port = args.port
    user = args.user
    password = args.password
    query_manager = QueryManager(args.query_folder)
    query_manager.setSeed(args.seed)
    if(args.mode == 'single'):
        print("Starting to benchmark all tpch queries...")
        benchmarkAllQueries()

    if(args.mode == 'all_random'):
        print("Starting to benchmark all tpch queries in a random order within: " + str(duration) + " minutes...")
        benchmarkRandomQueries()
