CREATE KEYSPACE IF NOT EXISTS clickstream
    WITH REPLICATION = {
        'class': 'NetworkTopologyStrategy',
        'datacenter1': 2
    };