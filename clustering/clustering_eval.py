# import os
# import sys
# import time
# from math import sqrt
# from random import randint
#
# from clustering.clustering_spark_context import sc
# from pyspark.mllib.util import MLUtils
# from clustering.clustering_model import ClusteringModel
# from pyspark.mllib.clustering import KMeans
# from rediser import redis
#
# SEED = 13
# # PRIMES = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101]
# PRIMES = [43, 47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101]
#
# ITERATIONS = [1, 2, 3, 5, 7, 10, 15, 20]
#
# sc.addPyFile("/Users/Adam/PycharmProjects/scarec/contextual.zip")
# sc.addPyFile("/Users/Adam/PycharmProjects/scarec/rediser.py")
# sc.addPyFile("/Users/Adam/PycharmProjects/scarec/clustering/clustering_evaluator.py")
# # Path for spark source folder
# os.environ['SPARK_HOME'] = "/Users/Adam/scarec/spark-1.6.1-bin-hadoop2.6"
#
# # Append pyspark  to Python Path
# sys.path.append("/Users/Adam/scarec/spark-1.6.1-bin-hadoop2.6/python")
# os.environ["PYSPARK_PYTHON"] = "/Users/Adam/Py3Env/bin/python"
# os.environ['PYTHONPATH'] = '/Users/Adam/scarec/spark-1.6.1-bin-hadoop2.6/'
#
# input_path = "/Users/Adam/PLISTA_DATASET/eval_rdd_labeled_points_300_dense_clicks"
# k_fold_n = 5
# weights = [1 / k_fold_n] * k_fold_n
#
# labeled_points = MLUtils.loadVectors(sc, input_path)
# splits = labeled_points.randomSplit(weights, SEED)
# i = 0
#
# model = None
# histogram = {}
#
#
# def error(point, model_pathx):
#     cluster = model.predict(point)
#     predicted_center = model.centers[cluster]
#     cluster_str = str(cluster)
#     if cluster_str in histogram[model_pathx]:
#         histogram[model_pathx][cluster_str] += 1
#     else:
#         histogram[model_pathx][cluster_str] = 1
#     return sqrt(sum([e ** 2 for e in (point - predicted_center)]))
#
#
# while i <= 4:
#     splits[i].cache()
#     i += 1
#
# for k in PRIMES:
#     for iters in ITERATIONS:
#         i = 0
#         init_mode = 'kmeans||'
#         wssse = 0
#         runs = 1
#         model_path = ClusteringModel.build_model_path(k, iters, runs)
#         time_taken = None
#         histogram[model_path] = {}
#         print(model_path)
#         while i <= 4:
#             test_data = splits[i]
#             train_data = splits[:i] + splits[i + 1:]
#             train_data = sc.union(train_data)
#
#             start_time = round(time.time())  # TODO: refactor it out to SW_PATTERN:DECORATOR
#
#             model = KMeans.train(train_data, k, maxIterations=iters, runs=1,
#                                  initializationMode=init_mode)
#             model.save(sc, "/Users/Adam/PycharmProjects/scarec/clustering/" + model_path.replace(':',
#                                                                                                  '_') + '_split_' + str(
#                 i) + '_' + str(randint(0, 99)))
#
#             time_taken_current = round(time.time()) - start_time
#             if time_taken is None or time_taken > time_taken_current:
#                 time_taken = time_taken_current
#
#             wssse = wssse + test_data.map(lambda point: error(point, model_path)).reduce(lambda x, y: x + y)
#             i += 1
#             #  redis.hmset('clustering_histogram:', histogram[model_path])  # set counts per clusters
#
#         redis.set('wssse:' + init_mode + ':' + str(model_path), wssse / 5)
#
#         redis.set('wssse:' + init_mode + ':' + str(model_path) + 'time_taken', time_taken)
