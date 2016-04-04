import json
from settings import KmeansSettings
from kafka import KafkaConsumer, KafkaProducer


class ClusteringConsumer:
    producer = KafkaProducer(client_id='scarec-kmeans-producer', bootstrap_servers=KmeansSettings.KAFKA_SERVER)

    @classmethod
    def clusterize_request(cls, request_id, vec):
        cls.producer.send(KmeansSettings.KMEANS_KAFKA_TOPIC,
                          json.dumps({'request_id': request_id, 'values': vec}).encode('utf-8'))
