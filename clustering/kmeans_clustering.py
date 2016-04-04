from __future__ import print_function
import os
import sys
import json

# Append pyspark to Python Path
sys.path.append(os.environ.get('SPARK_PYTHON'))

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.clustering import StreamingKMeans

import os

class KmeansSettings:
    KMEANS_CLUSTERING_MODEL_PATH = os.environ.get('KMEANS_CLUSTERING_MODEL_PATH')
    KMEANS_KAFKA_TOPIC = 'my-topic'  # os.environ.get('KMEANS_KAFKA_TOPIC')
    KAFKA_SERVER = 'localhost:9092'  # os.environ.get('KAFKA_SERVER')
    KMEANS_DIM = 50000  # os.environ.get('KEMANS_DIM')
    KMEANS_K = 100  # os.environ.get('KMEANS_K')
    KMEANS_DECAY_FACTOR = 0.9  # os.environ.get('KMEANS_DECAY_FACTOR')


class KmeansClustering:
    def __init__(self, k, dim, decay_factor):
        model_path = KmeansSettings.KMEANS_CLUSTERING_MODEL_PATH
        self.requests_processed = 0
        if model_path and os.path.exists(self.model_path):  # stored model initialization
            self.model = self.init_model_from_stored_centers(k, dim, decay_factor)
        else:
            self.model = self.init_model_from_random_centers(k, dim, decay_factor)

    @staticmethod
    def init_model_from_random_centers(k, dim, decay_factor):
        return StreamingKMeans(k=k, decayFactor=decay_factor).setRandomCenters(dim, 1.0,
                                                                               0)  # dim, weight, seed random centers

    @staticmethod
    def init_model_from_stored_centers(k, dim, decay_factor):
        return StreamingKMeans(k=k, decayFactor=decay_factor).setInitCenters(dim)  # dim, weight, seed random centers

    def pre_process_request(self, request):
        json_request = json.loads(request[1])
        labeled_point = self.labeled_point_from_json(json_request)
        return labeled_point

    @staticmethod
    def labeled_point_from_json(json_request):
        label = json_request['request_id']
        vec = json_request['values']
        return LabeledPoint(label, vec)

    @classmethod
    def post_process_request(cls, prediction):
        request_id = prediction.label
        assigned_cluster = prediction.features
        return {'request_id': request_id, 'assigned_cluster': assigned_cluster}

    # TODO, reply na kafka mq topic ktory je temporary lived only, vytvoreny niekde vo future

    def store_centers(self):
        pass


if __name__ == "__main__":
    sc = SparkContext(appName="StreamingKMeansClustering")
    ssc = StreamingContext(sc, 1)
    clustering = KmeansClustering(KmeansSettings.KMEANS_K, KmeansSettings.KMEANS_DIM, KmeansSettings.KMEANS_DECAY_FACTOR)
    model = clustering.model
    log4jLogger = sc._jvm.org.apache.log4j
    LOGGER = log4jLogger.LogManager.getLogger('KmeansClustering')

    # dummy training phase
    trainingQueue = [[]]
    trainingStream = ssc.queueStream(trainingQueue)
    model.trainOn(trainingStream)

    # our prediction stream
    directKafkaStream = KafkaUtils.createDirectStream(ssc, [KmeansSettings.KMEANS_KAFKA_TOPIC], {"bootstrap.servers": KmeansSettings.KAFKA_SERVER})
    predictionStream = directKafkaStream.map(clustering.pre_process_request)

    # predictions
    result = model.predictOnValues(predictionStream.map(lambda lp: (lp.label, lp.features)))
    result.pprint()

    ssc.start()
    ssc.stop(stopSparkContext=True, stopGraceFully=True)

    # ssc.awaitTermination()  # Wait for the computation to terminate

    print("Final centers: " + str(model.latestModel().centers))
