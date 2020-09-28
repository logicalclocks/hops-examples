from hops import hdfs
import os
import pickle

class Predict(object):

    def __init__(self):
        """ Initializes the serving state, reads a trained model from HDFS"""
        self.model_path = "Models/XGBoost_Churn_Classifier/1/xgb_reg.pkl"
        print("Copying model from HDFS to local directory")
        hdfs.copy_to_local(self.model_path)
        print("Reading local model for serving")
        self.model = xgb_model_loaded = pickle.load(open("xgb_reg.pkl", "rb"))
        print("Initialization Complete")


    def predict(self, inputs):
        """ Serves a prediction request usign a trained model"""
        return self.model.predict(inputs).tolist() # Numpy Arrays are note JSON serializable

    def classify(self, inputs):
        """ Serves a classification request using a trained model"""
        return "not implemented"

    def regress(self, inputs):
        """ Serves a regression request using a trained model"""
        return "not implemented"
