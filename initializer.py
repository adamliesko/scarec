from clustering.clustering_model import ClusteringModel
from classification.random_forester import RandomForester
from collaborative.als import ALS


class MLInitializer:
    @staticmethod
    def init():
        ClusteringModel.load_model()
        RandomForester.load_models()
        ALS.load_model()
