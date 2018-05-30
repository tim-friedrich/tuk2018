import pyhdb;

class ConnectionManager:
    # # HANA connection configurations
    # host = "192.168.31.57"
    # port = 30015
    # user = "SYSTEM"
    # password = "manager"

    def __init__(self, host, port, user, password):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.openConnection()

    def openConnection(self):
        self.connection = pyhdb.connect(
            host = self.host,
            port = self.port,
            user = self.user,
            password = self.password)

    def executeQuery(self, query):
        self.connection.cursor().execute(query)
