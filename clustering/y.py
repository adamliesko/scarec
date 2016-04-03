from __future__ import print_function
import os
import sys
import json

# Path for spark source folder
os.environ['SPARK_HOME'] = "/Users/Adam/scarec/spark-1.6.1-bin-hadoop2.6"

# Append pyspark  to Python Path
sys.path.append("/Users/Adam/scarec/spark-1.6.1-bin-hadoop2.6/python")
os.environ["PYSPARK_PYTHON"] = "/Users/Adam/Py3Env/bin/python"

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.linalg import Vectors
from pyspark.streaming.kafka import KafkaUtils
from pyspark.mllib.clustering import StreamingKMeans

if __name__ == "__main__":
    sc = SparkContext(appName="StreamingKMeansExample")  # SparkContext
    ssc = StreamingContext(sc, 1)


    def parse_json(request):
        json_request = json.loads(request[1])
        print(json_request)
        label = json_request['request_id']
        vec = json_request['values']

        return LabeledPoint(label, vec)


    # $example on$
    # we make an input stream of vectors for training,
    # as well as a stream of vectors for testing
    def parse(lp):
        label = float(lp[lp.find('(') + 1: lp.find(')')])
        vec = Vectors.dense(lp[lp.find('[') + 1: lp.find(']')].split(','))

        return LabeledPoint(label, vec)


    trainingData = sc.textFile("/Users/Adam/PycharmProjects/scarec/clustering/kmeans_data.txt") \
        .map(lambda line: Vectors.dense([float(x) for x in line.strip().split(' ')]))

    # testingData = sc.textFile("streaming_kmeans_data_test.txt").map(parse)

    trainingQueue = [trainingData]
    # testingQueue = [testingData]

    trainingStream = ssc.queueStream(trainingQueue)
    directKafkaStream = KafkaUtils.createDirectStream(ssc, ['my-topic'], {"bootstrap.servers": "10.0.1.102:9092"})

    # testingStream = ssc.queueStream(testingQueue)
    testingStream = directKafkaStream.map(parse_json)

    # We create a model with random clusters and specify the number of clusters to find
    model = StreamingKMeans(k=99, decayFactor=1.0).setRandomCenters(3, 1.0, 0)

    # Now register the streams for training and testing and start the job,
    # printing the predicted cluster assignments on new data points as they arrive.
    model.trainOn(trainingStream)
    result = model.predictOnValues(testingStream.map(lambda lp: (lp.label, lp.features)))
    result.pprint()

    ssc.start()
    # ssc.stop(stopSparkContext=True, stopGraceFully=True)
    # $example off$

    ssc.awaitTermination()  # Wait for the computation to terminate

    # print("Final centers: " + str(model.latestModel().centers))


    # ideme s dense, budeme dopocitavat
    # gender 10
    # age 6
    # income 5
    # os
    # isp
    # geo user
    # LANG_USER
    # channel
    # category
    # weather
    # time of day
    # DEVICE_TYPE
    # content keywords
    # #USER_PUBLISHER_IMPRESSION
    # po x tisicoh sa posuniem
    # inicializovat dvoma sposobmi, bud random alebo set initial clusters, ktore pred tym stornem do suboru niekde cez model.latestModel.centers()
