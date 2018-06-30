import time
import sys
from multiprocessing import Pool
import argparse
import pdb
import re
from connectionManager import ConnectionManager
from queryManager import QueryManager

parser = argparse.ArgumentParser()
query_manager = None

# executes a random query from the queries array
def executeRandomQuery(connection):
    return connection.runQuery(query_manager.getRandomQuery())

# Executes a random selection of queries until a time threshold is reached
def runRandomQueriesFor(minutes):
    try:
        connection = ConnectionManager(True)
        start_time = time.time()
        number_queries = 0
        # print("Start queries at", start_time, " for ", minutes*60)
        while(time.time() - start_time <= minutes*60):
            number_queries += executeRandomQuery(connection)
            
        # print("Stop queries after", time.time() - start_time)
        return number_queries
    except:
        print( "Unexpected error:", sys.exc_info())

def benchmarkRandomQueries():
    query_count = 0
    started_at = time.time()
    for i in range(0, num_repetitions):
        with Pool(processes=num_processes) as p:
            query_count += sum(p.map(runRandomQueriesFor, [duration]*num_processes))

    total_duration = time.time() - started_at
    print("Ran for", total_duration, "in total and", total_duration/num_repetitions, "in average.")
    print("Total Number of executed Queries in average ", query_count/num_repetitions)

def benchmarkAllQueries():
    connection = ConnectionManager(False)
    for query_num in range(1, 23):
        t1 = time.time()
        for i in range(0, num_repetitions):
            connection.runQuery(query_num)

        t2 = time.time()
        avg_time = (t2 - t1) / num_repetitions
        print("Query: " + str(query_num) + " performed at an average of: " + str(avg_time) + " seconds")


def addCommandLineArguments():
    parser.add_argument('mode', choices=['all_random', 'single'])
    ### General script config ###
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


def printConfig():
    print('\n')
    print('#'*30)
    print('## Starting benchmark with:')
    print('## mode:', args.mode)
    print('## seed:', args.seed)
    print('## repetitions:', args.repetitions)
    print('## processes:', args.processes)
    print('#'*30)
    print('\n')

if __name__ == '__main__':
    # loadQueries()
    addCommandLineArguments()
    args = parser.parse_args()
    num_repetitions = args.repetitions
    duration = args.duration
    num_processes = args.processes

    printConfig()
    query_manager = QueryManager()
    query_manager.setSeed(args.seed)
    if(args.mode == 'single'):
        print("Starting to benchmark all tpch queries...")
        benchmarkAllQueries()

    if(args.mode == 'all_random'):
        print("Starting to benchmark all tpch queries in a random order within: " + str(duration) + " minutes...")
        benchmarkRandomQueries()
