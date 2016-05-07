import sys
import os
import time
import re
import json
from pyspark.mllib.linalg import Vectors, SparseVector, DenseVector
from pyspark.mllib.regression import LabeledPoint

sys.path.append(os.environ.get('PYTHONPATH'))
from pyspark.mllib.recommendation import ALS
from spark_context import sc
from clustering_model import ClusteringModel
from context import Context
from context_encoder import ContextEncoder
from rediser import redis
from pyspark.mllib.tree import RandomForest, RandomForestModel, GradientBoostedTrees, GradientBoostedTreesModel
from pyspark.mllib.util import MLUtils

cluster_visits_key = 'eval:classifiers:cluster_visits:'

# load items
# map items to cluster_id /seen/unseen, count number of correct prediction - binary

RF_NUMBER_OF_TREES = 125
RF_DEPTH = 25

item_keys = redis.keys("eval:classifiers:item*")
all_items = []

digit_mask = re.compile(r'[^\d]+')
max = 0
domains = set()
publishers = set()


def load_data_to_memory():
    domains_map = {}
    d_idx = 0
    publishers_map = {}
    p_idx = 0
    # load vectors of items into memory
    for key in item_keys:
        item_id = key.decode('utf-8')
        item_id = digit_mask.sub('', item_id)
        item_content = redis.hgetall("eval:classifiers:item_content:" + item_id)
        item_int_content = {int(k): int(v) for k, v in item_content.items()}

        item_content = redis.hgetall(key)
        if item_content.get(b'domain_id', None) is not None:
            long_domain_id = int(item_content.get(b'domain_id').decode('utf-8'))
            if domains_map.get(long_domain_id, None) is not None:
                item_int_content[0] = domains_map[long_domain_id]
            else:
                domains_map[long_domain_id] = int(d_idx)
                item_int_content[0] = domains_map[long_domain_id]
                d_idx += 1
            domains.update(str(item_int_content[0]))

        if item_content.get(b'publisher_id', None) is not None:
            long_publisher_id = int(item_content.get(b'publisher_id').decode('utf8'))
            if publishers_map.get(long_publisher_id, None) is not None:
                item_int_content[1] = publishers_map[long_publisher_id]
            else:
                publishers_map[long_publisher_id] = int(p_idx)
                item_int_content[1] = publishers_map[long_publisher_id]
                p_idx += 1
            publishers.update(str(item_int_content[1]))

        item_vector = [item_id, SparseVector(300, item_int_content)]
        if len(item_int_content.keys()) < 300:
            all_items.append(item_vector)
    return all_items


all_items_rdd = sc.parallelize(load_data_to_memory())


def rf_eval():
    for cluster_id in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]:
        articles_key = "eval:classifiers:cluster_visits:" + str(cluster_id)
        positive = set()
        for article_id, count in redis.hgetall(articles_key).items():
            if int(count) >= 2:
                positive.add(article_id.decode('utf-8'))

        # [item_id, Vector] -- item === > LabeledPoint(1 || 0, vector)
        data = all_items_rdd.map(lambda item: LabeledPoint(1 if str(item[0]) in positive else 0, item[1]))
        model_id = 'cluster_id_' + str(cluster_id)
        model = RandomForest.trainRegressor(data,
                                            categoricalFeaturesInfo={},
                                            numTrees=RF_NUMBER_OF_TREES, featureSubsetStrategy="auto",
                                            impurity='variance', maxDepth=RF_DEPTH, maxBins=len(publishers))
        model.save(sc, '/Users/Adam/PycharmProjects/rf_models/' + model_id)
