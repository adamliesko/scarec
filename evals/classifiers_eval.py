import sys
import os
import time
import re
import json
from pyspark.mllib.linalg import Vectors, SparseVector, DenseVector
from pyspark.mllib.regression import LabeledPoint

import numpy as np

sys.path.append(os.environ.get('PYTHONPATH'))
from pyspark.mllib.recommendation import ALS
from spark_context import sc
from clustering_model import ClusteringModel
from context import Context
from context_encoder import ContextEncoder
from rediser import redis
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils

train_users_key = 'eval:classifiers:train_users'
train_users_items_key = 'eval:classifiers:train_user_items'

test_users_key = 'eval:classifiers:train_users'
test_users_items_key = 'eval:classifiers:test_user_items'

# eval:classifiers:item:XXXX: {content: {tf hash} , domain_id, publisher_id: }
item_key = 'eval:classifiers:item'
item_content_key = 'eval:classifiers:item_content:'
cluster_visits_key = 'eval:classifiers:cluster_visits:'
# ClusteringModel.load_model()
#
# files = ['/Users/Adam/PLISTA_DATASET/als_eval/impression_2013-06-01.log',
#          '/Users/Adam/PLISTA_DATASET/als_eval/impression_2013-06-02.log',
#          '/Users/Adam/PLISTA_DATASET/als_eval/impression_2013-06-03.log']
#          #'/Users/Adam/PLISTA_DATASET/als_eval/impression_2013-06-04.log',
#          #'/Users/Adam/PLISTA_DATASET/als_eval/impression_2013-06-05.log',
#          #'/Users/Adam/PLISTA_DATASET/als_eval/impression_2013-06-06.log',
#          #'/Users/Adam/PLISTA_DATASET/als_eval/impression_2013-06-07.log'
#
#
# for file in files:
#     with open(file) as f:
#         print(file)
#         for line in f:
#             jsond = json.loads(line)
#             context = Context(jsond).extract_to_json()
#             enc_context = ContextEncoder.encode_context_to_dense_vec(context)
#             cluster_id = ClusteringModel.predict_cluster(enc_context)
#             item_id_long = jsond['context']['simple'].get('25', None)
#             publisher_id = jsond['context']['simple'].get('27', None)
#             if jsond['context'].get('clusters', None):
#                 kws = jsond['context']['clusters'].get('33', None)
#
#             r = redis.pipeline()
#             if kws:
#                 for k, v in kws.items():
#                     r.hset(item_content_key + str(item_id_long), k, v)
#             r.hset(item_key + str(item_id_long), 'publisher_id', publisher_id)
#             r.hincrby(cluster_visits_key + str(cluster_id), item_id_long,
#                           1)  # incr count for cluster_id: item visit (hash histogram like structure)
#             r.execute()
#
#
#
# files = ['/Users/Adam/PLISTA_DATASET/als_eval/create_2013-06-01.log',
#          '/Users/Adam/PLISTA_DATASET/als_eval/create_2013-06-02.log',
#          '/Users/Adam/PLISTA_DATASET/als_eval/create_2013-06-03.log',
#          #'/Users/Adam/PLISTA_DATASET/als_eval/create_2013-06-04.log',
#          #'/Users/Adam/PLISTA_DATASET/als_eval/create_2013-06-05.log',
#          #'/Users/Adam/PLISTA_DATASET/als_eval/create_2013-06-06.log',
#          #'/Users/Adam/PLISTA_DATASET/als_eval/create_2013-06-07.log',
#          '/Users/Adam/PLISTA_DATASET/als_eval/update_2013-06-01.log',
#          '/Users/Adam/PLISTA_DATASET/als_eval/update_2013-06-02.log',
#          '/Users/Adam/PLISTA_DATASET/als_eval/update_2013-06-03.log']
#          #'/Users/Adam/PLISTA_DATASET/als_eval/update_2013-06-04.log',
#          #'/Users/Adam/PLISTA_DATASET/als_eval/update_2013-06-05.log',
#          #'/Users/Adam/PLISTA_DATASET/als_eval/update_2013-06-06.log',
#          #'/Users/Adam/PLISTA_DATASET/als_eval/update_2013-06-07.log']
#
# for file in files:
#     with open(file) as f:
#         for line in f:
#             jsond = json.loads(line)
#             domain_id = jsond['domainid']
#             item_id_long = jsond['id']
#             redis.hset(item_key + str(item_id_long), 'domain_id', domain_id)
#
#
#
#


# load items
# map items to cluster_id /seen/unseen, count number of correct prediction - binary

RF_NUMBER_OF_TREES = [3, 6, 9, 12, 15, 20, 25, 30, 40, 50, 75, 100, 125, 150]
RF_DEPTH = [3, 6, 7, 8, 9, 12, 15, 20, 25, 30, 40, 50]

item_keys = redis.keys("eval:classifiers:item*")
all_items = []

digit_mask = re.compile(r'[^\d]+')

# load vectors of items into memory
for key in item_keys:
    item_id = key.decode('utf-8')
    item_id = digit_mask.sub('', item_id)
    item_content = redis.hgetall("eval:classifiers:item_content:" + item_id)
    item_int_content = {int(k): int(v) for k, v in item_content.items()}
    if item_content.get(b'domain_id', None) is not None:
        item_int_content[0] = item_content.get(b'domain_id')
    if item_content.get(b'publisher_id', None) is not None:
        item_int_content[1] = item_content.get(b'publisher_id')
    item_vector = [item_id, SparseVector(len(list(item_content.keys())), item_int_content)]
    all_items.append(item_vector)

all_items_rdd = sc.parallelize(all_items)

print('Data loading finished')
# figure out whether they are positive or negative examples, 1 = seen, 0 = unseen ... count > 2 marks item as seen
for cluster_id in [12, 57, 13, 82]:
    print(cluster_id)
    articles_key = "eval:classifiers:cluster_visits:" + str(cluster_id)
    positive = {}
    negative = {}
    for article_id, count in redis.hgetall(articles_key).items():
        if int(count) >= 2:
            positive.update(article_id.decode('utf-8'))

    # [item_id, Vector] -- item === > LabeledPoint(1 || 0, vector)
    data = all_items_rdd.map(lambda item: LabeledPoint(1 if item[0] in positive else 0, item[1]))
    (training_data, test_data) = data.randomSplit([0.8, 0.2])  # 0.8 to train, 0.2 to test
    for tc in RF_NUMBER_OF_TREES:
        print(tc)
        for d in RF_DEPTH:
            print(d)
            model_id = 'rf_model_tc_' + str(tc) + '_d_' + str(d)
            start = time.time()

            model = RandomForest.trainRegressor(training_data, numClasses=2, categoricalFeaturesInfo={},
                                                numTrees=tc, featureSubsetStrategy="auto",
                                                impurity='gini', maxDepth=d, maxBins=32)
            redis.set('eval:rf:' + model_id + 'cluster_id:' + str(cluster_id) + ':time:' + str(time.time() - start))

            predictions = model.predict(test_data.map(lambda x: x.features))
            labelsAndPredictions = test_data.map(lambda lp: lp.label).zip(predictions)
            testMSE = labelsAndPredictions.map(lambda v, p: (v - p) * (v - p)).sum() / \
                      float(test_data.count())

            redis.set('eval:rf:' + model_id + 'cluster_id:' + str(cluster_id) + ':mse:' + str(testMSE))

            print('Test Mean Squared Error = ' + str(testMSE))
            print(model.toDebugString())
