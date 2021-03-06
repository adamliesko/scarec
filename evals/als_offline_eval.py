import sys
import os
import time

sys.path.append(os.environ.get('PYTHONPATH'))
from pyspark.mllib.recommendation import ALS
from spark_context import sc
from rediser import redis

train_users_key = 'eval:als:train_users'
train_users_items_key = 'eval:als:train_user_items'

test_users_key = 'eval:als:train_users'
test_users_items_key = 'eval:als:test_user_items'

# LOAD_TRAIN

#
# files = ['/Users/Adam/PLISTA_DATASET/als_eval/impression_2013-06-01.log',
#         '/Users/Adam/PLISTA_DATASET/als_eval/impression_2013-06-02.log',
#          '/Users/Adam/PLISTA_DATASET/als_eval/impression_2013-06-03.log',
#          '/Users/Adam/PLISTA_DATASET/als_eval/impression_2013-06-04.log',
#          '/Users/Adam/PLISTA_DATASET/als_eval/impression_2013-06-05.log']
# for file in files:
#     with open(file) as f:
#         print(file)
#         for line in f:
#             jsond = json.loads(line)
#             user_id_long = jsond['context']['simple'].get('57', None)
#             item_id_long = jsond['context']['simple'].get('25', None)
#             if user_id_long is None or str(user_id_long) == '0' or item_id_long is None:
#                 continue
#             user_id = Utils.encode_attribute('user_id', user_id_long)
#             item_id = Utils.encode_attribute('item_id', item_id_long)
#             redis.sadd(train_users_key, user_id)
#             redis.sadd(train_users_items_key, ':'.join([str(user_id), str(item_id)]))
#
# #LOAD_TEST
#
# files = ['/Users/Adam/PLISTA_DATASET/als_eval/impression_2013-06-06.log',
#          '/Users/Adam/PLISTA_DATASET/als_eval/impression_2013-06-07.log']
# for file in files:
#     with open(file) as f:
#         print(file)
#         for line in f:
#             jsond = json.loads(line)
#             user_id_long = jsond['context']['simple'].get('57', None)
#             item_id_long = jsond['context']['simple'].get('25', None)
#             if user_id_long is None or str(user_id_long) == '0' or item_id_long is None:
#                 continue
#             user_id = Utils.encode_attribute('user_id', user_id_long)#
#             item_id = Utils.encode_attribute('item_id', item_id_long)
#             redis.sadd(test_users_key, user_id)
#             redis.sadd(test_users_items_key, ':'.join([str(user_id), str(item_id)]))
#             redis.sadd('als:eval:test:user_items:' + str(user_id), item_id)
#


# FIGURE_OUT_USERS_TO_EVALUATE
print('Getting user ids to eval')
users_ids_to_evaluate = redis.sinter(test_users_key, train_users_key)

user_visits = {}
for user_id in users_ids_to_evaluate:
    user = user_id.decode('utf-8')
    visits = redis.smembers('als:eval:test:user_items:' + user)
    if len(visits) > 20:
        user_visits[user] = [int(i) for i in visits]

# LOAD_DATA_INTO_THE_SPARK_RDD
print('starting loading data')
train_user_items_pre_rdd = redis.smembers(train_users_items_key)
train_user_items_rdd = sc.parallelize(train_user_items_pre_rdd)
train_user_items_rdd_split = train_user_items_rdd.map(lambda pair_string: pair_string.decode('utf-8').split(':'))
train_RDD = train_user_items_rdd_split.map(lambda visit: (int(visit[0]), int(visit[1]), 1))  # use 1 for seen
train_RDD.cache()

print('finish data loading')
# PREPARE_PARAMETERS_TO_TEST_OUT
SEED = 42
RANKS = [3, 5, 6, 7, 8, 10, 15, 20, 30, 40, 50]  # number of hidden latent factors
ITERATIONS = [1, 5, 8, 10, 15, 20, 30]  # lambda is automatically scaled
user_id_counts = len(users_ids_to_evaluate)

# ITERATE OVER OPTIONS, BUILD MODEL ON TRAIN DATA, EVAL on TEST_DATA, STORE RESULTS
for ranks in RANKS:
    for iters in ITERATIONS:
        print(iters)
        start = time.time()
        skips = 0
        map_10 = 0
        map_20 = 0
        model = ALS.trainImplicit(train_RDD, ranks, seed=SEED,
                                  iterations=iters)
        delta = time.time() - start
        model_id = 'als:rank:' + str(ranks) + ':iterations:' + str(iters)
        redis.set('eval:als:time_taken:' + model_id, delta)

        for user_id in user_visits.keys():
            try:
                recommendations = model.recommendProducts(int(user_id), 20)
            except Exception:
                recommendations = []

            if len(recommendations) < 20:
                print(skips)
                skips += 1
                continue

            recs_10 = recommendations[:10]
            recs_20 = recommendations
            visited_items_int = user_visits[user_id]
            recs_10_int = [int(r.product) for r in recs_10]
            recs_20_int = [int(r.product) for r in recs_20]

            matched_10 = [r_id for r_id in recs_10_int if r_id in visited_items_int]
            matched_10_size = len(matched_10)

            matched_20 = [r_id for r_id in recs_20_int if r_id in visited_items_int]
            matched_20_size = len(matched_20)

            map_10 += matched_10_size / 10.0
            map_20 += matched_20_size / 20.0

        map_10_final = map_10 / float(user_id_counts - skips)
        map_20_final = map_20 / float(user_id_counts - skips)
        redis.set('eval:als:map10:' + model_id, map_10_final)
        redis.set('eval:als:map20:' + model_id, map_20_final)
