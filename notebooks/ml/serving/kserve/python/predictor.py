import joblib
from hops import hdfs

class Predict(object):

    def __init__(self):
        """Initializes the serving state, reads a trained model from HDFS"""
        self.model_path = "Models/irisflowerclassifier/1/iris_knn.pkl"
        print("Copying scikit-learn model from HDFS to local directory")
        hdfs.copy_to_local(self.model_path)
        print("Reading local scikit-learn model for serving")
        self.model = joblib.load("./iris_knn.pkl")
        print("Initialization complete")


    def predict(self, inputs):
        """ Serves a prediction request using a trained model"""
        return self.model.predict(inputs).tolist() # Numpy Arrays are note JSON serializable

    def classify(self, inputs):
        """ Serves a classification request using a trained model"""
        return "not implemented"

    def regress(self, inputs):
        """ Serves a regression request using a trained model"""
        return "not implemented"
