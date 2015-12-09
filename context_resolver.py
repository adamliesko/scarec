import redis_client
import urllib.request
import json


class ContextResolver:
    _RESOLVABLE_KEYS = [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 28, 33]
    _cache = init_cache()

    def __init__(self):
        self.redis = redis_client.connection
        self.cache = self.init_cache()

    # 1,2,3 - cluster, 15 neda sa , 20 - to je cas, 21 - nema vyznam to je strana, 22 sa neda, 23 je hodina,  24 je count, 25 je cislo id itemu, 26 sa neda
    # 27 sa neda, 28 je opat priamy pocet, 29,30,31,32 sa neda, 34-38 sa neda,  39 je vlastne to iste cislo , 41 je id teamu toiste,  42 je to iste status
    # 49 je cas reakcie ot je int, 57 je user to iste

    def resolve(self, key, value):

        if (str(key) + ':' + str(value)) in self.cache:
            return self.cache[str(key) + ':' + str(value)]
        elif self.resolvable(key):
            print('not hitting')
            url = 'http://orp.plista.com/api/vector_resolution.php?vid=' + str(key) + '&aid=' + str(value)
            response = urllib.request.urlopen(url).read().decode('utf8')
            data = json.loads(response)
            value_resolved = data['value_resolved']
            self.store_resolved_context(key, value, value_resolved)
            return value_resolved
        else:
            return None #osetrit nvratovu hodnotu pre typ ktory nevieme resolvnut

    def store_resolved_context(self, vector_key, value, value_resolved):
        self.cache[str(vector_key) + ':' + str(value)] = value_resolved
        return self.redis.set('resolved_context:' + str(vector_key) + ':' + str(value), value_resolved)

    @classmethod
    def init_cache(cls):
        cache = {}
        keys = self.redis.keys('resolved_context:')
        for key in keys:
            value = self.redis.get(key)
            vector_key, vector_value = self.split_key_name(key)
            cache[str(vector_key) + ':' + str(vector_value)] = value
        return cache

    @classmethod
    def resolvable(cls, key):
        return key in cls._RESOLVABLE_KEYS

    @staticmethod
    def split_key_name( key):
       return key.split(':')[1:]


a = ContextResolver()
b = a.resolve(28, 28)



a = ContextResolver()
b = a.resolve(28, 28)
print(b)
