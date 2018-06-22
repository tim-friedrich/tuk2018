import random
import os
import subprocess
import re
import string

class QueryManager:

    def parameterizedQuery(self, query_name):
        result = subprocess.run(["qgen", query_name], stdout=subprocess.PIPE)
        query = result.stdout.decode("utf-8")
        if query_name == "15":
            # prevent duplicate views
            query = re.sub("revenue0", ''.join(random.choices(string.ascii_uppercase, k=12)), query)
        return query

    def getRandomQuery(self):
    	return random.randint(1, 22)

    def setSeed(self, seed):
        random.seed(seed)
