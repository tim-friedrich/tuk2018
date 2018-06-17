import random;
import os
import subprocess

class QueryManager:

    def parameterizedQuery(self, query_name):
        result = subprocess.run(["qgen", query_name], stdout=subprocess.PIPE)
        return result.stdout

    def getRandomQuery(self):
        return self.parameterizedQuery(str(random.randint(1, 22))).decode('utf-8')

    def setSeed(self, seed):
        random.seed(seed);
