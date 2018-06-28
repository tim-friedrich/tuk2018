import pyhdb
import json
import re
import pdb
import random
import sys
from fractions import Fraction
from queryManager import QueryManager

class Connection:
    
    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password


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
            map[query_num] = []
            for idx, serverConf in enumerate(self.config['connections']):
                if query_num in serverConf['queries']:
                    if str(query_num) in serverConf['fractions'].keys():
                        numerator = serverConf['fractions'][str(query_num)][0]
                        denominator = serverConf['fractions'][str(query_num)][1]
                        fraction = Fraction(numerator, denominator)
                    else:
                        fraction = Fraction(1, 1)
                    map[query_num].append((self.connections[idx], fraction))
            map[query_num].sort(key=getFraction)
        return map

    def selectConnectionFor(self, query_num):
        if not self.use_cluster:
            return self.connections[0][0]

        rnd = Fraction(random.uniform(0, 1))
        fraction = Fraction(0)
        for tpl in self.queryToServerMap[query_num]:
            fraction += tpl[1]
            if rnd <= fraction:
                return tpl[0]

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
                password = serverConf['password']
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

def getFraction(tpl):
    return tpl[1]