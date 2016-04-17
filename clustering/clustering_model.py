from pyspark.mllib.clustering import KMeans, KMeansModel
from pyspark import SparkContext
from contextual.context_encoder import ContextEncoder
from clustering.clustering_spark_context import sc
from math import sqrt
from rediser import redis
import time

import os
import sys
import json


# sc.addPyFile("/path/to/redis.zip")

# Path for spark source folder
os.environ['SPARK_HOME'] = "/Users/Adam/scarec/spark-1.6.1-bin-hadoop2.6"

# Append pyspark  to Python Path
sys.path.append("/Users/Adam/scarec/spark-1.6.1-bin-hadoop2.6/python")
os.environ["PYSPARK_PYTHON"] = "/Users/Adam/Py3Env/bin/python"
os.environ['PYTHONPATH'] = '/Users/Adam/scarec/spark-1.6.1-bin-hadoop2.6/'

class ClusteringModel:

    def __init__(self, model_path=None):
        if model_path:
            self.model = KMeansModel.load(sc, 'modelik')
        else:
            self.model = None

    def predict_cluster(self, point):
        if self.model:
            return self.model.predit(point)
        else:
            return None

    @classmethod
    def learn_model(cls, model_path, parsed_data, k=10, iterations=10):
        start_time = round(time.time())  # TODO: refactor it out to SW_PATTERN:DECORATOR

        model = KMeans.train(parsed_data, k, maxIterations=iterations, runs=1,
                              initializationMode="random")

        time_taken = round(time.time()) - start_time
        redis.set('time_taken:' + model_path, time_taken)

        model.save(sc, 'modelik')
        return model

    @classmethod
    def parse_data(cls, input_data):
        return input_data.map(lambda context: ContextEncoder.encode_context_to_sparse_vec(context))

    @staticmethod
    def build_model_path(k, iters, runs):
        return str('kmeans_clustering:' + 'k:' + str(k) + ':iters:' + str(
            iters) + ':runs:' + str(runs))
