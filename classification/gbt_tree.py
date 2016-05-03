# from pyspark.mllib.tree import GradientBoostedTrees, GradientBoostedTreesModel
# from pyspark.mllib.util import MLUtils
#
# # Load and parse the data file.
# data = MLUtils.loadLibSVMFile(sc, "data/mllib/sample_libsvm_data.txt")
# # Split the data into training and test sets (30% held out for testing)
# (trainingData, testData) = data.randomSplit([0.7, 0.3])
#
# # Train a GradientBoostedTrees model.
# #  Notes: (a) Empty categoricalFeaturesInfo indicates all features are continuous.
# #         (b) Use more iterations in practice.
# model = GradientBoostedTrees.trainRegressor(trainingData,
#                                             categoricalFeaturesInfo={}, numIterations=3)
#
# # Evaluate model on test instances and compute test error
# predictions = model.predict(testData.map(lambda x: x.features))
# labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
# testMSE = labelsAndPredictions.map(lambda (v, p): (v - p) * (v - p)).sum() / \
#           float(testData.count())
# print('Test Mean Squared Error = ' + str(testMSE))
# print('Learned regression GBT model:')
# print(model.toDebugString())
#
# # Save and load model
# model.save(sc, "target/tmp/myGradientBoostingRegressionModel")
# sameModel = GradientBoostedTreesModel.load(sc, "target/tmp/myGradientBoostingRegressionModel")

class GbtTree:
    pass