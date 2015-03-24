CREATE KEYSPACE IF NOT EXISTS crawler WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

