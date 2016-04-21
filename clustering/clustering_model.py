import time

from pyspark.mllib.clustering import KMeans, KMeansModel
from contextual.context_encoder import ContextEncoder
from clustering.clustering_spark_context import sc
from rediser import redis


class ClusteringModel:
    MODEL = KMeansModel.load(sc, '/Users/Adam/PycharmProjects/scarec/clustering/kmeans_clustering_k_79_iters_2_runs_1_split_4_59')

    @classmethod
    def predict_cluster(cls, point):
        cluster = cls.MODEL.predict(point)
        return cluster

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
