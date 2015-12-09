from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans


class KMeansClusterer:
    def parse(lp):
        label = float(lp[lp.find('(') + 1: lp.find(',')])
        vec = Vectors.dense(lp[lp.find('[') + 1: lp.find(']')].split(','))
        return LabeledPoint(label, vec)

    trainingData = ssc.textFileStream("/training/data/dir").map(Vectors.parse)
    testData = ssc.textFileStream("/testing/data/dir").map(parse)

    model = StreamingKMeans(k=2, decayFactor=1.0).setRandomCenters(3, 1.0, 0)
    Now register the streams for training and testing and start the job, printing the predicted cluster assignments on new data points as they arrive.

    model.trainOn(trainingData)
    print(model.predictOnValues(testData.map(lambda lp: (lp.label, lp.features))))

    ssc.start()
    ssc.awaitTermination()