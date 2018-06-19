import random;
import os
import subprocess

class QueryManager:

    def parameterizedQuery(self, query_name):
        result = subprocess.run(["qgen", query_name], stdout=subprocess.PIPE)
        return result.stdout

    def getRandomQuery(self):
    	return random.randint(1, 22)

    def setSeed(self, seed):
        random.seed(seed);
