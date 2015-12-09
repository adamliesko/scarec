import redis


class RedisClient(object):
    def __init__(self, **kwargs):
        self.connection_settings = kwargs or {'host': 'localhost',
                'port': 6379, 'db': 0}

    def redis(self):
        return redis.Redis(**self.connection_settings)

    def update(self, d):
        self.connection_settings.update(d)

    def connection_setup(self, **kwargs):
        global connection, client
        if client:
            client.update(kwargs)
        else:
            client = RedisClient(**kwargs)
        connection = RedisClient.redis()

    def get_client(self):
        global connection
        return connection

client = RedisClient()
connection = client.redis()

__all__ = ['connection_setup', 'get_client']