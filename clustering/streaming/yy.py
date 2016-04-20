from __future__ import print_function

import sys
import os

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingDirectKafkaWordCount")
    ssc = StreamingContext(sc, 2)

    kvs = KafkaUtils.createDirectStream(ssc, ['my-topic'], {"metadata.broker.list": "localhost:9092"})
    lines = kvs.map(lambda x: x[1])
    counts = lines.flatMap(lambda line: line)
    counts.pprint()

    ssc.start()
