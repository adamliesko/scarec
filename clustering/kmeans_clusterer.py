import threading, logging, time, json

from kafka import KafkaConsumer, KafkaProducer

producer = KafkaProducer(client_id='scarec-kmeans-producer', bootstrap_servers='localhost:9092')
consumer = KafkaConsumer(bootstrap_servers='localhost:9092', auto_offset_reset='earliest')
consumer.subscribe(['kmeans-clustering-results'])


class KmeansClustering:
    TOPIC_NAME = 'kmeans-clustering'

    @classmethod
    def clusterize_request(cls, content, request_id):
        producer.send(cls.TOPIC_NAME, value=content, key=request_id)

    @classmethod
    def read_request_cluster(cls, request_id):
        pass


class Producer(threading.Thread):
    daemon = True

    def run(self):
        producer = KafkaProducer(bootstrap_servers='localhost:9092')

        while True:
            producer.send('my-topic', json.dumps({'request_id': '23323223', 'values': [0, 1, 0]}).encode('utf-8'))
            time.sleep(1)


def main():
    threads = [
        Producer()
    ]

    for t in threads:
        t.start()

    time.sleep(10)


if __name__ == "__main__":
    main()
