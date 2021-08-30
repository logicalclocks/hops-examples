import hsfs

class Transformer(object):
    def __init__(self):
        # Connect to the Feature Store
        self.conn = hsfs.connection()
        self.fs = self.conn.get_feature_store()
        self.td = self.fs.get_training_dataset("card_fraud_model", 1)
    
    def preprocess(self, inputs):
        # Return ordered feature vector
        inputs['instances'] = [self.td.get_serving_vector(cc_num) for cc_num in inputs['instances']]
        return inputs
        
    def postprocess(self, outputs):
        return outputs