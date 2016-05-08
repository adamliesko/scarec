import numpy

from rediser import redis
from pyspark.mllib.linalg import SparseVector, DenseVector
from contextual.context import Context


# cache structure key
# key: context_encoder:key:value = array index
# input : {'23': 23, '33:464068': 1, '18': 0, '33:44507': 1, '33:491611': 8, '6': 9, '33:40853': 1, '42': 0, '2:6': 11, '33:2844': 0, '11': [1201425], '33:398': 0, '29': 17332, '33:2167572': 1, '3:1': 28, '24': 12, '33:50327': 0, '3:4': 28, '27': 59436, '2:4': 60, '39': 970, '33:15858': 0, '33:129052': 2, '16': 48811, '46:472375': 255, '9': 26889, '5': 6, '2:5': 23, 'recs': 1370296799113, '22': 69613, '1:7': 255, '14': 33331, '33:595102': 3, '2:3': 74, '57': 18741214652, '8': [18841, 18842], '4': 7, '47': 654013, '2:2': 50, '46:472376': 255, '10': [6, 13], '2:1': 11, '33:3869887': 1, '35': 3150303, '49': 47, '31': 0, '7': 18851, '19': 78119, 'timestamp': 1370296799113, '3:0': 50, '33:7910': 0, '25': 1283210663, '3:5': 15, '3:2': 34, '51:3': 255, '33:739179': 1, '13': 2, '46:472358': 255, '33:553567': 6, '33:37192': 1, '52': 1, '17': 48985, '2:0': 21, '3:3': 98, '33:19899293': 1, '56': 1138207, '37': 1205425}
# array index - index where to set flag to 1


class ContextEncoder:
    CURRENT_IDX_KEY = 'context_encoder_current_idx'
    redis.setnx(CURRENT_IDX_KEY, 0)

    @classmethod
    def current_idx(cls):
        return int(redis.get(cls.CURRENT_IDX_KEY))

    @classmethod
    def store_prop_value_pair_to_redis(cls, prop, value):
        property_idx = redis.incr(cls.CURRENT_IDX_KEY)
        redis.set(cls.build_context_encoder_key(prop, value), property_idx)
        return property_idx

    @classmethod
    def encode_context_to_sparse_vec(cls, context):
        vec_data = {}  # vector is a dict of 1s where certain dimension in vector is present
        for key, value in context.items():
            prop_name = None

            # figure out our property:value pairs
            if ':' in key:
                prop = key.split(':')[0]
                value = key.split(':')[1:]
            else:
                prop = key
            if isinstance(value, list):
                value = value[0]

            # skip if not included in vector for clustering
            if prop in Context.MAPPINGS.keys():
                prop_name = Context.MAPPINGS[prop]
            else:
                next

            # get our vector idx of property:value pair
            if prop_name in Context.CLUSTERING_PROPERTIES:
                property_idx = redis.get(cls.build_context_encoder_key(prop_name, value))
                if property_idx:
                    # cache hit
                    pass
                else:
                    # cache miss
                    property_idx = cls.store_prop_value_pair_to_redis(prop_name, value)
                vec_data[int(property_idx)] = 1
        vec_data[30000] = 1
        return SparseVector(len(vec_data.keys()), vec_data)

    @classmethod
    def encode_context_to_dense_vec(cls, context):
        vec_data = numpy.zeros(300)  # vector is a dict of 1s where certain dimension in vector is present
        for key, value in context.items():
            prop_name = None

            # figure out our property:value pairs
            if ':' in key:
                prop = key.split(':')[0]
                value = key.split(':')[1:]
            else:
                prop = key
            if isinstance(value, list):
                if len(value) > 0:
                    value = value[0]
                else:
                    next

            # skip if not included in vector for clustering
            if prop in Context.MAPPINGS.keys():
                prop_name = Context.MAPPINGS[prop]
            else:
                next

            # get our vector idx of property:value pair
            if prop_name in Context.CLUSTERING_PROPERTIES:
                property_idx = redis.get(cls.build_context_encoder_key(prop_name, value))
                if property_idx:
                    # cache hit
                    pass
                else:
                    # cache miss
                    property_idx = cls.store_prop_value_pair_to_redis(prop_name, value)
                vec_data[int(property_idx)] = 1
        return DenseVector(vec_data)

    @staticmethod
    def build_context_encoder_key(prop, val):
        return 'context_encoder' + ':' + str(Context.MAPPINGS_INV[prop]) + ':' + str(val)
