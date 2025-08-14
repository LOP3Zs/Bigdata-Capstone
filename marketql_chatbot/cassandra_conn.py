import os
from dotenv import load_dotenv
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

load_dotenv()

class CassandraConnection:
    def __init__(self):
        self.hosts = os.getenv('CASSANDRA_HOSTS', '127.0.0.1').split(',')
        self.port = int(os.getenv('CASSANDRA_PORT', 9042))
        self.keyspace = os.getenv('CASSANDRA_KEYSPACE', 'market_data')
        self.username = os.getenv('CASSANDRA_USERNAME', 'cassandra')
        self.password = os.getenv('CASSANDRA_PASSWORD', 'cassandra')
        self.session = None

    def connect(self):
        auth = None
        if self.username and self.password:
            auth = PlainTextAuthProvider(username=self.username, password=self.password)
        cluster = Cluster(self.hosts, port=self.port, auth_provider=auth) if auth else Cluster(self.hosts, port=self.port)
        self.session = cluster.connect()
        if self.keyspace:
            self.session.set_keyspace(self.keyspace)
        return self.session

    def get_session(self):
        return self.session or self.connect()


def test_connection():
    try:
        s = CassandraConnection().connect()
        v = list(s.execute("SELECT release_version FROM system.local"))[0].release_version
        print(f"✅ Cassandra OK, version: {v}")
        return True
    except Exception as e:
        print(f"❌ Cassandra connection failed: {e}")
        return False