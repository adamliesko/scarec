import os

from pyspark.mllib.clustering import KMeans, KMeansModel
from contextual.context_encoder import ContextEncoder
from spark_context import sc


class ClusteringModel:
    MODEL = None

    @classmethod
    def predict_cluster(cls, point):
        cluster = cls.MODEL.predict(point)
        return cluster

    @classmethod
    def learn_model(cls, model_path, parsed_data, k=10, iterations=10):
        model = KMeans.train(parsed_data, k, maxIterations=iterations, runs=1,
                             initializationMode="random")

        model.save(sc, cls.build_model_path(k, iterations))
        return model

    @classmethod
    def load_model(cls, path=os.environ.get('KMEANS_MODEL_PATH')):
        cls.MODEL = KMeansModel.load(sc, path)

    @classmethod
    def parse_data(cls, input_data):
        return input_data.map(lambda context: ContextEncoder.encode_context_to_sparse_vec(context))

    @staticmethod
    def build_model_path(k, iters):
        return str('kmeans_clustering:' + 'k:' + str(k) + ':iters:' + str(
            iters))

    @classmethod
    def update_model(cls):
        pass #TODO: load new items from es, do temp model load, switch MODEL to the new one
