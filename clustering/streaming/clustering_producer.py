import json

from kafka import SimpleProducer, SimpleClient

from settings import KmeansSettings


class ClusteringProducer:
    kafka = SimpleClient(KmeansSettings.KAFKA_SERVER)
    kafka.ensure_topic_exists(KmeansSettings.KMEANS_KAFKA_TOPIC)
    producer = SimpleProducer(kafka)

    @classmethod
    def clusterize_request(cls, request_id, vec, request_type):
        cls.producer.send_messages(KmeansSettings.KMEANS_KAFKA_TOPIC,
                                   json.dumps({'request_id': request_id, 'values': vec, 'type': request_type}).encode(
                                       'utf-8'))
