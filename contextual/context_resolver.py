import urllib.request
import json

from rediser import redis

class ContextResolver:
    _RESOLVER_URL = 'http://orp.plista.com/api/vector_resolution.php'
    _RESOLVABLE_KEYS = [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 28, 33]

    def __init__(self):
        self.cache = ContextResolver.init_cache()

    def resolve(self, key, value):
        if (str(key) + ':' + str(value)) in self.cache:  # cache hit
            return self.cache[str(key) + ':' + str(value)]
        elif self.resolvable(key):  # cache miss
            url = self._RESOLVER_URL + '?vid=' + str(key) + '&aid=' + str(value)
            response = urllib.request.urlopen(url).read().decode('utf8')
            data = json.loads(response)
            value_resolved = data['value_resolved']
            self.store_resolved_context(key, value, value_resolved)
            return value_resolved
        else:
            return None  # cannot be resolved

    def store_resolved_context(self, vector_key, value, value_resolved):
        self.cache[str(vector_key) + ':' + str(value)] = value_resolved
        return redis.set('resolved_context:' + str(vector_key) + ':' + str(value), value_resolved)

    @classmethod
    def init_cache(cls):
        cache = {}
        keys = redis.keys('resolved_context:')
        for key in keys:
            value = redis.get(key)
            vector_key, vector_value = cls.split_key_name(key)
            cache[str(vector_key) + ':' + str(vector_value)] = value
        return cache

    @classmethod
    def resolvable(cls, key):
        return key in cls._RESOLVABLE_KEYS

    @staticmethod
    def split_key_name(key):
        return key.split(':')[1:]
