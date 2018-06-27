import pyhdb
import json
import re
import pdb
import random
import sys
from queryManager import QueryManager

class Connection:
    
    def __init__(self, host, port, user, password, queries):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.queries = queries


class ConnectionManager:

    def __init__(self, use_cluster):
        self.config = self.loadConfig()
        self.connections = self.loadConnections()
        self.use_cluster = use_cluster
        self.query_manager = QueryManager()
        self.queryToServerMap = self.initServerQueryMap()

    def initServerQueryMap(self):
        map = {}
        for query_num in range(1, 23):
            map[query_num] = set()
            for idx, serverConf in enumerate(self.config['connections']):
                if query_num in serverConf['queries']:
                    map[query_num].add(self.connections[idx])
        return map

    def selectConnectionFor(self, query_num):
        if not self.use_cluster:
            return self.connections[0]

        return random.sample(self.queryToServerMap[query_num], 1)[0]

    def loadConfig(self):
        with open('config.json', 'r') as f:
            return json.load(f)
    
    def loadConnections(self):
        connections = {}
        for idx, serverConf in enumerate(self.config['connections']):
            connections[idx] = Connection(
                host = serverConf['host'],
                port = serverConf['port'],
                user = serverConf['user'],
                password = serverConf['password'],
                queries = serverConf['queries']
            )
        return connections

    def executeQuery(self, query, connection, query_num):
        subqueries = re.sub(';\\s+', ';', query).split(';')
        try:
            con = pyhdb.connect(
                host = connection.host,
                port = connection.port,
                user = connection.user,
                password = connection.password
            )
            for q in subqueries:
                if len(q) > 0:
                    con.cursor().execute(q)
            con.close()
            return 1
        except:
            print("Unexpected error on " + connection.host + " with query " + str(query_num) + ":", sys.exc_info()[1])
            return 0

    def runQuery(self, query_num):
        connection = self.selectConnectionFor(query_num)
        return self.executeQuery(
            self.query_manager.parameterizedQuery(str(query_num)),
            connection,
            query_num
        )

