import os
class KmeansSettings:
    KMEANS_CLUSTERING_MODEL_PATH = os.environ.get('KMEANS_CLUSTERING_MODEL_PATH')
    KMEANS_KAFKA_TOPIC = os.environ.get('KMEANS_KAFKA_TOPIC')
    KAFKA_SERVER = os.environ.get('KAFKA_SERVER')
    KMEANS_DIM = os.environ.get('KEMANS_DIM')
    KMEANS_K = os.environ.get('KMEANS_K')
    KMEANS_DECAY_FACTOR = os.environ.get('KMEANS_DECAY_FACTOR')
