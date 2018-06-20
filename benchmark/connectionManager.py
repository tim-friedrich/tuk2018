import pyhdb;
import json;
import re;
import pdb;
import random;
from queryManager import QueryManager

class ConnectionManager:

    def __init__(self, use_cluster):
        self.config = self.loadConfig()
        self.use_cluster = use_cluster
        self.query_manager = QueryManager()
        self.openConnections()
        self.queryToServerMap = self.initServerQueryMap()

    def initServerQueryMap(self):
        map = {}
        for query_num in range(1, 23):
            map[query_num] = set()
            for idx, serverConf in enumerate(self.config['connections']):
                if query_num in serverConf['queries']:
                    map[query_num].add(self.connections[idx])
        return map

    def openConnections(self):
        self.connections = []
        for con in self.config['connections']:
            self.connections.append(
                pyhdb.connect(
                    host = con['host'],
                    port = con['port'],
                    user = con['user'],
                    password = con['password']
                )
            )

    def selectConnectionFor(self, query_num):
        if not self.use_cluster:
            return self.connections[0]

        return random.sample(self.queryToServerMap[query_num], 1)[0]

    def loadConfig(self):
        with open('config.json', 'r') as f:
            return json.load(f)

    def executeQuery(self, query, connection):
        subqueries = re.sub(';\\s+', ';', query).split(';')
        for q in subqueries:
            if len(q) > 0:
                connection.cursor().execute(q)

    def runQuery(self, query_num):
        self.executeQuery(
            self.query_manager.parameterizedQuery(str(query_num)).decode("utf-8"),
            self.selectConnectionFor(query_num)
        )

