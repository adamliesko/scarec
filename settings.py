import os


class KmeansSettings:
    KMEANS_CLUSTERING_MODEL_PATH = os.environ.get('KMEANS_CLUSTERING_MODEL_PATH')
    KMEANS_KAFKA_TOPIC = 'kmeans_clustering'  # os.environ.get('KMEANS_KAFKA_TOPIC')
    KAFKA_SERVER = 'localhost:9002'  # os.environ.get('KAFKA_SERVER')
    KMEANS_DIM = 50000  # os.environ.get('KEMANS_DIM')
    KMEANS_K = 100  # os.environ.get('KMEANS_K')
    KMEANS_DECAY_FACTOR = 0.9  # os.environ.get('KMEANS_DECAY_FACTOR')
