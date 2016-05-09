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
from models.item import Item
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.recommendation import ALS
from pyspark.mllib.util import MLUtils
from pyspark.mllib.clustering import KMeans

from pyspark.mllib.linalg import Vectors, SparseVector, DenseVector
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel

from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils

train_files_remote = ['/home/rec/PLISTA_DATA/2013-06-01/impression_2013-06-01.log',
                      '/home/rec/PLISTA_DATA/2013-06-02/impression_2013-06-02.log',
                      '/home/rec/PLISTA_DATA/2013-06-03/impression_2013-06-03.log',
                      '/home/rec/PLISTA_DATA/2013-06-04/impression_2013-06-04.log',
                      '/home/rec/PLISTA_DATA/2013-06-05/impression_2013-06-05.log',
                      '/home/rec/PLISTA_DATA/2013-06-06/impression_2013-06-06.log',
                      '/home/rec/PLISTA_DATA/2013-06-07/impression_2013-06-07.log']

test_files_remote = ['/home/rec/PLISTA_DATA/2013-06-08/impression_2013-06-08.log',
                     '/home/rec/PLISTA_DATA/2013-06-09/impression_2013-06-09.log',
                     '/home/rec/PLISTA_DATA/2013-06-10/impression_2013-06-10.log',
                     '/home/rec/PLISTA_DATA/2013-06-11/impression_2013-06-11.log',
                     '/home/rec/PLISTA_DATA/2013-06-12/impression_2013-06-12.log']

item_train_files_remote = ['/home/rec/PLISTA_DATA/2013-06-01/create_2013-06-01.log',
                           '/home/rec/PLISTA_DATA/2013-06-02/create_2013-06-02.log',
                           '/home/rec/PLISTA_DATA/2013-06-03/create_2013-06-03.log',
                           '/home/rec/PLISTA_DATA/2013-06-04/create_2013-06-04.log',
                           '/home/rec/PLISTA_DATA/2013-06-05/create_2013-06-05.log',
                           '/home/rec/PLISTA_DATA/2013-06-06/create_2013-06-06.log',
                           '/home/rec/PLISTA_DATA/2013-06-07/create_2013-06-07.log',
                           '/home/rec/PLISTA_DATA/2013-06-01/update_2013-06-01.log',
                           '/home/rec/PLISTA_DATA/2013-06-02/update_2013-06-02.log',
                           '/home/rec/PLISTA_DATA/2013-06-03/update_2013-06-03.log',
                           '/home/rec/PLISTA_DATA/2013-06-04/update_2013-06-04.log',
                           '/home/rec/PLISTA_DATA/2013-06-05/update_2013-06-05.log',
                           '/home/rec/PLISTA_DATA/2013-06-06/update_2013-06-06.log',
                           '/home/rec/PLISTA_DATA/2013-06-07/update_2013-06-07.log']

item_test_files_remote = ['/home/rec/PLISTA_DATA/2013-06-08/create_2013-06-08.log',
                          '/home/rec/PLISTA_DATA/2013-06-09/create_2013-06-09.log',
                          '/home/rec/PLISTA_DATA/2013-06-10/create_2013-06-10.log',
                          '/home/rec/PLISTA_DATA/2013-06-11/create_2013-06-11.log',
                          '/home/rec/PLISTA_DATA/2013-06-12/create_2013-06-12.log',
                          '/home/rec/PLISTA_DATA/2013-06-08/update_2013-06-08.log',
                          '/home/rec/PLISTA_DATA/2013-06-09/update_2013-06-09.log',
                          '/home/rec/PLISTA_DATA/2013-06-10/update_2013-06-10.log',
                          '/home/rec/PLISTA_DATA/2013-06-11/update_2013-06-11.log',
                          '/home/rec/PLISTA_DATA/2013-06-12/update_2013-06-12.log']

item_content_key = 'final_eval:item_content:'
item_key = 'final_eval:item:'
cluster_visits_key = 'final_eval:classifiers:cluster_visits:'
global_popularity_key = 'final_eval:global_popularity'

test_user_count_key = 'final_eval:test_user_count'
test_users_key = 'final_eval:test_users'

cluster_recs_key = 'final_eval:recs:cluster_id:'


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


def get_users_day(phase, day_no):
    key = phase + ':final_eval:users_day:' + (str(day_no)) + ''
    return redis.smembers(key)


def get_user_day_visits(phase, day_no, user_id):
    key = phase + ':final_eval:user:' + str(user_id) + ':user_visits_day:' + (str(day_no))
    return [int(v.decode('utf-8')) for v in redis.smembers(key)]


def get_user_visits(phase, user_id):
    key = phase + ':final_eval:user:' + str(user_id) + ':user_visits:'
    return [int(v.decode('utf-8')) for v in redis.smembers(key)]


def add_user(phase, user_id):
    key = phase + ':final_eval:users'
    redis.sadd(key, user_id)


def add_cluster_visit(phase, cluster_id, item_id):
    key = phase + ':final_eval:cluster_visits:cluster_id:' + str(cluster_id)
    redis.sadd(key, item_id)


def add_user_cluster(phase, cluster_id, user_id):
    key = phase + ':final_eval:user_clusters:' + str(user_id)
    redis.hincrby(key, cluster_id, 1)


def get_user_clusters(phase, user_id):
    key = phase + ':final_eval:user_clusters:' + str(user_id)
    dictie = {}
    for cluster_id, v in redis.hgetall(key):
        dictie[int(cluster_id.decode('utf-8'))] = int(v)
    return dictie


def add_cluster_visit_day(phase, day, cluster_id, item_id):
    key = phase + ':final_eval:cluster_visits:cluster_id:' + str(cluster_id) + ':day:' + str(day)
    redis.sadd(key, item_id)


def add_cluster_rf_recs(cluster_id, items_rdd):
    items = items_rdd.collect()
    key = cluster_recs_key + str(cluster_id)
    r = redis.pipeline()
    for item_id, prediction in items:
        r.zadd(key, item_id, prediction)
    r.execute()


def get_cluster_rf_recs(cluster_id):
    key = cluster_recs_key + str(cluster_id)
    items = redis.zrange(key, 0, 99, desc=True, withscores=True)
    dicties = {}
    for item, v in items:
        if item and v:
            dicties[int(item.decode('utf-8'))] = int(v)
    return dicties


def load_test_data_into_redis(files, day):
    phase = 'test'
    ClusteringModel.load_model()
    for file in files:
        with open(file) as f:
            print('processing file:' + file)
            for line in f:
                jsond = json.loads(line)
                user_id = jsond['context']['simple'].get('57', None)
                item_id = jsond['context']['simple'].get('25', None)
                publisher_id = jsond['context']['simple'].get('27', None)

                if user_id is None or str(user_id) == '0' or item_id is None:
                    continue
                add_user(phase, user_id)
                add_user_day(phase, day, user_id)
                add_user_visit(phase, user_id, item_id)
                add_user_visit_day(phase, day, user_id, item_id)

                if jsond['context'].get('clusters', None):
                    kws = jsond['context']['clusters'].get('33', None)
                    r = redis.pipeline()
                    if kws and isinstance(kws, dict):
                        for k, v in kws.items():
                            r.hset(item_content_key + str(item_id), k, v)
                    r.hset(item_key + str(item_id), 'publisher_id', publisher_id)
                    r.execute()
                try:
                    context = Context(jsond).extract_to_json()
                    enc_context = ContextEncoder.encode_context_to_dense_vec(context)
                    cluster_id = ClusteringModel.predict_cluster(enc_context)
                    add_user_cluster(phase, cluster_id, user_id)
                    add_cluster_visit(phase, cluster_id, item_id)
                    add_cluster_visit_day(phase, day, cluster_id, item_id)
                except Exception:
                    continue


def precompute_rf_recs_test():
    domains_map = {}
    d_idx = 0
    publishers_map = {}
    p_idx = 0
    all_items = []

    # load vectors of items into memory
    item_keys = redis.keys("final_eval:item:*")
    for key in item_keys:
        item_id = key.decode('utf-8')
        splits = item_id.split(':')
        item_id = splits[-1]
        item_content = redis.hgetall(item_content_key + item_id)
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

        if item_content.get(b'publisher_id', None) is not None:
            long_publisher_id = int(item_content.get(b'publisher_id').decode('utf8'))
            if publishers_map.get(long_publisher_id, None) is not None:
                item_int_content[1] = publishers_map[long_publisher_id]
            else:
                publishers_map[long_publisher_id] = int(p_idx)
                item_int_content[1] = publishers_map[long_publisher_id]
                p_idx += 1

        item_vector = [item_id, SparseVector(300, item_int_content)]
        if len(item_int_content.keys()) < 300:
            all_items.append(item_vector)

            # load it into Spark context
    all_items_rdd = sc.parallelize(all_items)
    data = all_items_rdd

    # precompute recs for each cluster, store all of them in redis sorted sets
    for cluster_id in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]:
        print('Recommending for cluster:' + str(cluster_id))
        model_id = 'cluster_id_' + str(cluster_id)
        model = RandomForestModel.load(sc, os.environ.get('RF_MODEL_PATH_ROOT') + '/' + model_id)
        predictions = model.predict(data.map(lambda x: x[1]))
        idsAndPredictions = data.map(lambda x: x[0]).zip(predictions)
        add_cluster_rf_recs(cluster_id, idsAndPredictions)


# filter only users who had more than ten visits during the eval phases per day / global in all days

def find_user_ids_to_evaluate():
    # visits per day
    for day in [0, 1, 2, 3, 4]:
        print('xxxxx ' + str(day))
        addicted_ids_day = []
        users = get_users_day('test', day)
        print(len(users))
        for user in users:
            user_id = user.decode('utf-8')
            visits = get_user_day_visits('test', day, user_id)
            if len(visits) > 10:
                addicted_ids_day.append(user_id)
        print('Daily users to eval count ' + str(len(addicted_ids_day)))

        r = redis.pipeline()
        for user_id in addicted_ids_day:
            r.sadd('final_eval:users_to_eval_day:' + str(day), int(user_id))
        r.execute()

    # global visits
    addicted_users = redis.sinter('test:final_eval:users_day:0', 'test:final_eval:users_day:1',
                                  'test:final_eval:users_day:2', 'test:final_eval:users_day:3',
                                  'test:final_eval:users_day:4')

    addicted_ids = []
    for user in addicted_users:
        user_id = user.decode('utf-8')
        visits = get_user_visits('test', user_id)
        if len(visits) > 10:
            addicted_ids.append(user_id)
    print('Global users to eval count ' + str(len(addicted_ids)))
    r = redis.pipeline()
    for user_id in addicted_ids:
        r.sadd('final_eval:users_to_eval_all', int(user_id))
    r.execute()


def global_eval():
    phase = 'test'
    als_p3_global_key = 'als:final_eval:metrics:global:p3'
    als_p5_global_key = 'als:final_eval:metrics:global:p5'
    als_p10_global_key = 'als:final_eval:metrics:global:p10'
    als_user_recall_global_key = 'als:final_eval:metrics:global:user_recall'

    ctx_p3_global_key = 'ctx:final_eval:metrics:global:p3'
    ctx_p5_global_key = 'ctx:final_eval:metrics:global:p5'
    ctx_p10_global_key = 'ctx:final_eval:metrics:global:p10'
    ctx_user_recall_global_key = 'ctx:final_eval:metrics:global:user_recall'

    global_users_to_eval = redis.smembers('final_eval:users_to_eval_all')
    global_users_to_eval = [int(user_id.decode('utf-8')) for user_id in global_users_to_eval]
    global_user_count = len(global_users_to_eval)

    # LOAD CTX RECOMMENDATIONS
    ctx_recs = {}
    for cluster_id in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]:
        ctx_recs[cluster_id] = get_cluster_rf_recs(cluster_id)

    # GLOBAL_EVALS

    als = MatrixFactorizationModel.load(sc, os.environ.get('ALS_MODEL_PATH'))
    als_p3_global = 0
    als_p5_global = 0
    als_p10_global = 0

    ctx_p3_global = 0
    ctx_p5_global = 0
    ctx_p10_global = 0
    ctx_user_recall_set_global = set()
    als_user_recall_set_global = set()
    for user in global_users_to_eval:
        print('evaluating: ' + str(user))
        user_visits_global = get_user_visits(phase, user)
        encoded_user_id = Utils.encode_attribute('user_id', user)

        # ALS_COLLAB_RECOMMENDATIONS
        encoded_recs = als.recommendProducts(int(encoded_user_id), 10)

        als_recs = []
        for rec in encoded_recs:
            rec_id = Utils.decode_attribute('item_id', int(rec.product))
            als_recs.append(rec_id)

        good_recs = [rec for rec in als_recs if int(rec) in user_visits_global]
        if len(good_recs) > 0:
            als_user_recall_set_global.update(user)
        als_p10_global += (float(len(good_recs)) / 10.0)

        good_recs_5 = [rec for rec in als_recs[:5] if int(rec) in user_visits_global]
        als_p5_global += (float(len(good_recs_5)) / 5.0)

        good_recs_3 = [rec for rec in als_recs[:3] if int(rec) in user_visits_global]
        als_p3_global += (float(len(good_recs_3)) / 3.0)

        # CONTEXT_CLUSTERING RECS

        user_clusters = get_user_clusters(phase, user)
        total_count = 0

        for cluster, count in user_clusters:
            total_count += count

        for cluster, count in user_clusters:
            weight_of_cluster = float(count) / total_count
            ctx_recs = ctx_recs[cluster][:10]
            good_recs_10 = [rec for rec in ctx_recs if int(rec) in user_visits_global]
            if good_recs_10 > 0:
                ctx_user_recall_set_global.update(user)
            ctx_p10_global += (float(len(good_recs_10)) / 5.0) * weight_of_cluster

            good_recs_5 = [rec for rec in ctx_recs[:5] if int(rec) in user_visits_global]
            ctx_p5_global += (float(len(good_recs_5)) / 5.0) * weight_of_cluster

            good_recs_3 = [rec for rec in ctx_recs[:3] if int(rec) in user_visits_global]
            ctx_p3_global += (float(len(good_recs_3)) / 3.0) * weight_of_cluster

        # ES_CONTENT_BASED_RECS


        # REDIS_WRITE_RESULTS
        redis.set(ctx_p3_global_key, ctx_p3_global / float(global_user_count))
        redis.set(ctx_p5_global_key, ctx_p5_global / float(global_user_count))
        redis.set(ctx_p10_global_key, ctx_p10_global / float(global_user_count))
        redis.set(ctx_user_recall_global_key, len(ctx_user_recall_set_global))

        # REDIS_WRITE_RESULTS
        redis.set(als_p3_global_key, als_p3_global / float(global_user_count))
        redis.set(als_p5_global_key, als_p5_global / float(global_user_count))
        redis.set(als_p10_global_key, als_p10_global / float(global_user_count))
        redis.set(als_user_recall_global_key, len(als_user_recall_set_global))


def per_day_eval_cummulative():
    phase = 'test'
    als_p3_global_key = 'als:final_eval:metrics:global:p3'
    als_p5_global_key = 'als:final_eval:metrics:global:p5'
    als_p10_global_key = 'als:final_eval:metrics:global:p10'
    als_user_recall_global_key = 'als:final_eval:metrics:global:user_recall'

    ctx_p3_global_key = 'ctx:final_eval:metrics:global:p3'
    ctx_p5_global_key = 'ctx:final_eval:metrics:global:p5'
    ctx_p10_global_key = 'ctx:final_eval:metrics:global:p10'
    ctx_user_recall_global_key = 'ctx:final_eval:metrics:global:user_recall'

    global_users_to_eval = redis.smembers('final_eval:users_to_eval_all')
    global_users_to_eval = [int(user_id.decode('utf-8')) for user_id in global_users_to_eval]
    global_user_count = len(global_users_to_eval)

    # LOAD CTX RECOMMENDATIONS
    ctx_recs = {}
    for cluster_id in [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18]:
        ctx_recs[cluster_id] = get_cluster_rf_recs(cluster_id)

    # GLOBAL_EVALS

    als = MatrixFactorizationModel.load(sc, os.environ.get('ALS_MODEL_PATH'))
    als.recommendProducts()
    als_p3_global = 0
    als_p5_global = 0
    als_p10_global = 0

    ctx_p3_global = 0
    ctx_p5_global = 0
    ctx_p10_global = 0
    ctx_user_recall_set_global = set()
    als_user_recall_set_global = set()
    for user in global_users_to_eval:
        user_visits_global = get_user_visits(phase, user)
        encoded_user_id = Utils.encode_attribute('user_id', user)

        # ALS_COLLAB_RECOMMENDATIONS
        encoded_recs = als.recommendProducts(int(encoded_user_id), 10)

        als_recs = []
        for rec in encoded_recs:
            rec_id = Utils.decode_attribute('item_id', int(rec.product))
            als_recs.append(rec_id)

        good_recs = [rec for rec in als_recs if rec in user_visits_global]
        if len(good_recs) > 0:
            als_user_recall_set_global.update(user)
        als_p10_global += (float(len(good_recs)) / 10.0)

        good_recs_5 = [rec for rec in als_recs[:5] if rec in user_visits_global]
        als_p5_global += (float(len(good_recs_5)) / 5.0)

        good_recs_3 = [rec for rec in als_recs[:3] if rec in user_visits_global]
        als_p3_global += (float(len(good_recs_3)) / 3.0)

        # CONTEXT_CLUSTERING RECS

        user_clusters = get_user_clusters(phase, user)
        total_count = 0

        for cluster, count in user_clusters:
            total_count += count

        for cluster, count in user_clusters:
            weight_of_cluster = float(count) / total_count
            ctx_recs = ctx_recs[cluster][:10]
            good_recs_10 = [rec for rec in ctx_recs if rec in user_visits_global]
            if good_recs_10 > 0:
                ctx_user_recall_set_global.update(user)
            ctx_p10_global += (float(len(good_recs_10)) / 5.0) * weight_of_cluster

            good_recs_5 = [rec for rec in ctx_recs[:5] if rec in user_visits_global]
            ctx_p5_global += (float(len(good_recs_5)) / 5.0) * weight_of_cluster

            good_recs_3 = [rec for rec in ctx_recs[:3] if rec in user_visits_global]
            ctx_p3_global += (float(len(good_recs_3)) / 3.0) * weight_of_cluster

        # ES_CONTENT_BASED_RECS


        # REDIS_WRITE_RESULTS
        redis.set(ctx_p3_global_key, ctx_p3_global / float(global_user_count))
        redis.set(ctx_p5_global_key, ctx_p5_global / float(global_user_count))
        redis.set(ctx_p10_global_key, ctx_p10_global / float(global_user_count))
        redis.set(ctx_user_recall_global_key, len(ctx_user_recall_set_global))

        # REDIS_WRITE_RESULTS
        redis.set(als_p3_global_key, als_p3_global / float(global_user_count))
        redis.set(als_p5_global_key, als_p5_global / float(global_user_count))
        redis.set(als_p10_global_key, als_p10_global / float(global_user_count))
        redis.set(als_user_recall_global_key, len(als_user_recall_set_global))


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
                item = Item(jsond)
                item.process_item_change_event()
                domain_id = jsond['domainid']
                item_id = jsond['id']
                redis.hset(item_key + str(item_id), 'domain_id', domain_id)


def learn_rf_models():
    RF_NUMBER_OF_TREES = 125
    RF_DEPTH = 25

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
        model.save(sc, os.environ.get('RF_MODEL_PATH_ROOT') + '/' + model_id)


def learn_als_model():
    print('starting als = loading data')
    train_user_items_pre_rdd = redis.smembers('train:final_eval:als:user_item_interactions')
    train_user_items_rdd = sc.parallelize(train_user_items_pre_rdd)
    train_user_items_rdd_split = train_user_items_rdd.map(lambda pair_string: pair_string.decode('utf-8').split(':'))
    train_RDD = train_user_items_rdd_split.map(lambda visit: (int(visit[0]), int(visit[1]), 1))  # use 1 for seen
    train_RDD.cache()

    print(train_RDD.count())
    print('finish data loading')
    SEED = 42
    RANK = 20  # number of hidden latent factors
    ITERATIONS = 30  # lambda is automatically scaled

    start = time.time()
    model = ALS.trainImplicit(train_RDD, RANK, seed=SEED,
                              iterations=ITERATIONS)
    model.save(sc, os.environ.get('ALS_MODEL_PATH'))
    delta = time.time() - start
    print(str(delta))
    redis.set('final_eval:als:time_taken', delta)


def load_als_train_data_into_redis(files):
    phase = 'train'
    for file in files:
        with open(file) as f:
            print('processing file:' + file)
            for line in f:
                jsond = json.loads(line)
                user_id = jsond['context']['simple'].get('57', None)
                item_id = jsond['context']['simple'].get('25', None)

                if user_id is not None or str(user_id) != '0' or item_id is not None:
                    encoded_user_id = Utils.encode_attribute('user_id', user_id)
                    encoded_item_id = Utils.encode_attribute('item_id', item_id)
                    add_user_visit('test', user_id, item_id)
                    als_add_user_item_interaction_als(phase, encoded_user_id, encoded_item_id)


# precision at 5
# presicion at 10
# kolko userom sme boli schopn odporucit aspon 1
# ako sa to odvija po jednotlivych dnoch po teste
# ako je to total

# load_train_data_into_redis(train_files_remote)
# load_item_domains_into_redis(item_train_files_remote)
# learn_rf_models()
# learn_als_model()
# load_item_domains_into_redis(item_test_files_remote)
# load_test_data_into_redis(test_files_remote)
# precompute_rf_recs_test()

# load_test_data_into_redis(['/home/rec/PLISTA_DATA/2013-06-08/impression_2013-06-08.log'], 0)
# load_test_data_into_redis(['/home/rec/PLISTA_DATA/2013-06-09/impression_2013-06-09.log'], 1)
# load_test_data_into_redis(['/home/rec/PLISTA_DATA/2013-06-10/impression_2013-06-10.log'], 2)
# load_test_data_into_redis(['/home/rec/PLISTA_DATA/2013-06-11/impression_2013-06-11.log'], 3)
# load_test_data_into_redis([ '/home/rec/PLISTA_DATA/2013-06-12/impression_2013-06-12.log'], 4)


# load_item_domains_into_redis(item_train_files_remote)
# load_item_domains_into_redis(item_test_files_remote)

# find_user_ids_to_evaluate()

# global_eval()

learn_als_model()
