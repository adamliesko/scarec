# from pyspark.mllib.linalg import Vectors
# from pyspark.mllib.regression import LabeledPoint
# from pyspark.mllib.clustering import StreamingKMeans
# from pyspark import SparkContext
# from pyspark.streaming import StreamingContext
# from pyspark.streaming.kafka import KafkaUtils
#
#
# def parse(lp):
#     print(111223)
#     label = float(lp[lp.find('(') + 1: lp.find(',')])
#     vec = Vectors.dense(lp[lp.find('[') + 1: lp.find(']')].split(','))
#     return LabeledPoint(label, vec)
#
#
# # Create a.txt local StreamingContext with two working thread and batch interval of 1 second
# sc = SparkContext("local[2]", "ContextualClusteringSC")
# ssc = StreamingContext(sc, 2)
#
# log4jLogger = sc._jvm.org.apache.log4j
# LOGGER = log4jLogger.LogManager.getLogger(__name__)
#
# directKafkaStream = KafkaUtils.createDirectStream(ssc, ['my-topic'], {"bootstrap.servers": "localhost:9092"})
#
# trainingData = directKafkaStream.map(Vectors.parse)
# testData = directKafkaStream.map(parse)
#
# model = StreamingKMeans(k=2, decayFactor=1.0).setRandomCenters(1, 1, 0)
# model.trainOn(trainingData)
# LOGGER.info("XXXXXYYYYY" + str(model.predictOnValues(testData.map(lambda lp: (lp.label, lp.features))).pprint()))
#
# result = model.predictOnValues(testData.map(lambda lp: (lp.label, lp.features)))
# result.pprint()
#
# ssc.start()
# ssc.awaitTermination()
