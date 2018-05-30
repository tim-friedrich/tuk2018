import random;
import os

class QueryManager:
    queries = []

    def __init__(self, query_path):
        self.query_path = query_path
        self.query_file_names = os.listdir(query_path)
        self.loadQueries()

    # loads all queries from the files in query_path into an array
    def loadQueries(self):
        for query_file in self.query_file_names:
            self.queries.append(self.getQueryFrom(query_file))

    def getQueryFrom(self, filename):
        fd = open(self.query_path +"/"+filename, 'r')
        query = fd.read()
        fd.close()
        return query

    def getRandomQuery(self):
        return random.choice(self.queries)

    def setSeed(self, seed):
        random.seed(seed);

    def getNameFor(self, query):
        return self.query_file_names[self.queries.index(query)]
