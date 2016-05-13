from pyspark.mllib.tree import RandomForest, RandomForestModel
from clustering.clustering_model import ClusteringModel
import os
from spark_context import sc


class RandomForester:
    MODELS = {}

    @classmethod
    def load_models(cls, path=os.environ.get('RF_MODEL_PATH_ROOT')):
        for k in ClusteringModel.CLUSTERS:
            model_id = 'cluster_id_' + str(k)
            cls.MODELS[k] = RandomForestModel.load(sc, path + '/' + model_id)
        return cls.MODELS

    # we will parse and convert to Spark RDD
    @classmethod
    def predict_items(cls, item_ids, cluster_id):
        data = []
        predictions = cls.MODELS[cluster_id].predict(data.map(lambda x: x[1]))
        ids_predictions = data.map(lambda x: x[0]).zip(predictions)

    @classmethod
    def prepare_items(cls, item_ids):
        pass

    @classmethod
    def update_model(cls):
        pass

    @classmethod
    def predict_new_item(cls, vector, item_id, cluster_id):
        return cls.MODELS[cluster_id].predict(vector)
