from math import sqrt

from pyspark.mllib.clustering import KMeansModel
from pyspark.mllib.util import MLUtils
from clustering.clustering_spark_context import sc


# loads model from model_path, evaluates predictions on input_path and stores resulting wssse into redis

class ClusteringEvaluator:
    def __init__(self, model_path):
        self.model_path = model_path
        self.model = self.load_model_from_path(model_path)

    def load_model_from_path(self, path):
        return KMeansModel.load(sc, 'modelik')

    def load_input_data_text(self, input_path):
        return sc.textFile(input_path)

    # not used yet
    def load_input_data_text_sparse(self, input_path):
        return MLUtils.loadVectors(sc, input_path)

    def error(self, point):
        model = self.model
        print(point)
        predicted_center = model.centers[model.predict(point)]
        print(predicted_center)
        return sqrt(sum([e ** 2 for e in (point - predicted_center)]))

    # Within Set Sum of Squared Errors
    def calculate_wssse(self, input_data):
        wssse = input_data.map(lambda point: self.error(point)).reduce(lambda x, y: x + y)
        return wssse
