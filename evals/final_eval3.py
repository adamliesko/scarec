import sys
import os
import time
import re
import json


sys.path.append('/home/rec/scarec/')
sys.path.append('/home/rec/scarec/models/')
sys.path.append('/home/rec/scarec/contextual/')
sys.path.append('/home/rec/scarec/clustering/')
sys.path.append(os.environ.get('PYTHONPATH'))

from rediser import redis
from utils import Utils
from spark_context import sc
from clustering.clustering_model import ClusteringModel
from context import Context
from context_encoder import ContextEncoder

from pyspark.mllib.recommendation import ALS
from pyspark.mllib.util import MLUtils
from pyspark.mllib.clustering import KMeans

from pyspark.mllib.linalg import Vectors, SparseVector, DenseVector
from pyspark.mllib.regression import LabeledPoint

from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils

train_files = ['/Users/Adam/PLISTA_DATASET/2013-06-01/impression_2013-06-01.log',
               '/Users/Adam/PLISTA_DATASET/2013-06-02/impression_2013-06-02.log',
               '/Users/Adam/PLISTA_DATASET/2013-06-03/impression_2013-06-03.log',
               '/Users/Adam/PLISTA_DATASET/2013-06-04/impression_2013-06-04.log',
               '/Users/Adam/PLISTA_DATASET/2013-06-05/impression_2013-06-05.log',
               '/Users/Adam/PLISTA_DATASET/2013-06-06/impression_2013-06-06.log',
               '/Users/Adam/PLISTA_DATASET/2013-06-07/impression_2013-06-07.log']

train_files_remote = [#'/home/rec/PLISTA_DATA/2013-06-01/impression_2013-06-01.log',
              # '/home/rec/PLISTA_DATA/2013-06-02/impression_2013-06-02.log',
               '/home/rec/PLISTA_DATA/2013-06-03/impression_2013-06-03.log']
               #'/home/rec/PLISTA_DATA/2013-06-04/impression_2013-06-04.log',
               #'/home/rec/PLISTA_DATA/2013-06-05/impression_2013-06-05.log',
               #'/home/rec/PLISTA_DATA/2013-06-06/impression_2013-06-06.log',
               #'/home/rec/PLISTA_DATA/2013-06-07/impression_2013-06-07.log']


item_train_files = ['/Users/Adam/PLISTA_DATASET/als_eval/create_2013-06-01.log',
                    '/Users/Adam/PLISTA_DATASET/als_eval/create_2013-06-02.log',
                    '/Users/Adam/PLISTA_DATASET/als_eval/create_2013-06-03.log',
                    '/Users/Adam/PLISTA_DATASET/als_eval/create_2013-06-04.log',
                    '/Users/Adam/PLISTA_DATASET/als_eval/create_2013-06-05.log',
                    '/Users/Adam/PLISTA_DATASET/als_eval/create_2013-06-06.log',
                    '/Users/Adam/PLISTA_DATASET/als_eval/create_2013-06-07.log',
                    '/Users/Adam/PLISTA_DATASET/als_eval/update_2013-06-01.log',
                    '/Users/Adam/PLISTA_DATASET/als_eval/update_2013-06-02.log',
                    '/Users/Adam/PLISTA_DATASET/als_eval/update_2013-06-03.log',
                    '/Users/Adam/PLISTA_DATASET/als_eval/update_2013-06-04.log',
                    '/Users/Adam/PLISTA_DATASET/als_eval/update_2013-06-05.log',
                    '/Users/Adam/PLISTA_DATASET/als_eval/update_2013-06-06.log',
                    '/Users/Adam/PLISTA_DATASET/als_eval/update_2013-06-07.log']

item_train_files_remote = ['/home/rec/PLISTA_DATA/als_eval/create_2013-06-01.log',
                    '/home/rec/PLISTA_DATA/als_eval/create_2013-06-02.log',
                    '/home/rec/PLISTA_DATA/als_eval/create_2013-06-03.log',
                    '/home/rec/PLISTA_DATA/als_eval/create_2013-06-04.log',
                    '/home/rec/PLISTA_DATA/als_eval/create_2013-06-05.log',
                    '/home/rec/PLISTA_DATA/als_eval/create_2013-06-06.log',
                    '/home/rec/PLISTA_DATA/als_eval/create_2013-06-07.log',
                    '/home/rec/PLISTA_DATA/als_eval/update_2013-06-01.log',
                    '/home/rec/PLISTA_DATA/als_eval/update_2013-06-02.log',
                    '/home/rec/PLISTA_DATA/als_eval/update_2013-06-03.log',
                    '/home/rec/PLISTA_DATA/als_eval/update_2013-06-04.log',
                    '/home/rec/PLISTA_DATA/als_eval/update_2013-06-05.log',
                    '/home/rec/PLISTA_DATA/als_eval/update_2013-06-06.log',
                    '/home/rec/PLISTA_DATA/als_eval/update_2013-06-07.log']


def add_user_visit_day(phase, day_no, user_id, item_id):
    key = phase + ':final_eval:user:' + str(user_id) + ':user_visits_day:' + (str(day_no))
    redis.sadd(key, item_id)


def add_user_visit(phase, user_id, item_id):
    key = phase + ':final_eval:user:' + str(user_id) + ':user_visits:'
    redis.sadd(key, item_id)


def als_add_user_visit_day(phase, day_no, user_id, item_id):
    key = phase + ':final_eval:als:user:' + str(user_id) + ':user_visits_day:' + (str(day_no))
    redis.sadd(key, item_id)


def als_add_user_visit(phase, user_id, item_id):
    key = phase + ':final_eval:als:user:' + str(user_id) + ':user_visits'
    redis.sadd(key, item_id)


def als_add_user_item_interaction_als(phase, enc_user_id, enc_item_id):
    key = phase + ':final_eval:als:user_item_interactions'
    redis.sadd(key, ':'.join([str(enc_user_id), str(enc_item_id)]))


def add_user_day(phase, day_no, user_id):
    key = phase + ':final_eval:users_day:' + (str(day_no)) + ''
    redis.sadd(key, user_id)


def add_user(phase, user_id):
    key = phase + ':final_eval:users'
    redis.sadd(key, user_id)


item_content_key = 'final_eval:item_content:'
item_key = 'final_eval:item:'
cluster_visits_key = 'final_eval:classifiers:cluster_visits:'
global_popularity_key = 'final_eval:global_popularity'

def load_train_data_into_redis(files):
    ClusteringModel.load_model()

    phase = 'train'
    for file in files:
        with open(file) as f:
            print('processing file:' + file)
            for line in f:
                jsond = json.loads(line)
                user_id = jsond['context']['simple'].get('57', None)
                item_id = jsond['context']['simple'].get('25', None)
                publisher_id = jsond['context']['simple'].get('27', None)

                if item_id:
                    redis.hincrby(global_popularity_key, item_id, 1)

                # load data for CLASSIFIER
                context = Context(jsond).extract_to_json()
                enc_context = ContextEncoder.encode_context_to_dense_vec(context)
                cluster_id = ClusteringModel.predict_cluster(enc_context)
                if jsond['context'].get('clusters', None):
                    kws = jsond['context']['clusters'].get('33', None)
                    r = redis.pipeline()
                    if kws:
                        for k, v in kws.items():
                            r.hset(item_content_key + str(item_id), k, v)
                    r.hset(item_key + str(item_id), 'publisher_id', publisher_id)
                    r.hincrby(cluster_visits_key + str(cluster_id), item_id,
                              1)  # incr count for cluster_id: item visit (hash histogram like structure)
                    r.execute()

                # load data for  ALS
                if user_id is not None or str(user_id) != '0' or item_id is not None:
                    encoded_user_id = Utils.encode_attribute('user_id', user_id)
                    encoded_item_id = Utils.encode_attribute('item_id', item_id)
                    als_add_user_item_interaction_als(phase, encoded_user_id, encoded_item_id)


def load_item_domains_into_redis(files):
    for file in files:
        print(file)
        with open(file) as f:
            for line in f:
                jsond = json.loads(line)
                domain_id = jsond['domainid']
                item_id = jsond['id']
                redis.hset(item_key + str(item_id), 'domain_id', domain_id)


RF_NUMBER_OF_TREES = 125
RF_DEPTH = 25


def learn_rf_models():
    domains_map = {}
    d_idx = 0
    publishers_map = {}
    p_idx = 0
    domains = set()
    publishers = set()
    all_items = []

    # load vectors of items into memory
    item_keys = redis.keys("final_eval:item:*")
    for key in item_keys:
        item_id = key.decode('utf-8')
        splits = item_id.split(':')
        item_id = splits[-1]
        item_content = redis.hgetall("final_eval:classifiers:item_content:" + item_id)
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

        # load it into Spark context
    all_items_rdd = sc.parallelize(all_items)

    # learn classifiers per clusters
    for cluster_id in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]:
        articles_key = "final_eval:classifiers:cluster_visits:" + str(cluster_id)
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

# precision at 5
# presicion at 10
# kolko userom sme boli schopn odporucit aspon 1
# ako sa to odvija po jednotlivych dnoch po teste
# ako je to total

load_train_data_into_redis(train_files_remote)
