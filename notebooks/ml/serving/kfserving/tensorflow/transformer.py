class Transformer(object):
    def __init__(self):
        print("[Transformer] Initializing...")
        # Initialization code goes here

    def preprocess(self, inputs):
        # Transform the request inputs here. The object returned by this method will be used as model input.
        return inputs

    def postprocess(self, outputs):
        # Transform the predictions computed by the model before returning a response.
        return outputs