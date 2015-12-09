class FeatureVectorizer:
    @staticmethod
    def pairing_function(x, y):
        if x > y:
            return x ** 2 + x + y
        else:
            return y ** 2 + x


print(FeatureVectorizer.pairing_function(10, 12))