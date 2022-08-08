import io
import base64
import json
import tornado
from typing import List, Dict
from PIL import Image
import torchvision.transforms as transforms
import logging
import kserve

logging.basicConfig(level=kserve.constants.KSERVE_LOGLEVEL)


EXPLAINER_URL_FORMAT = "http://{0}/v1/models/{1}:explain"

image_processing = transforms.Compose([
        transforms.ToTensor(),
        transforms.Normalize((0.1307,), (0.3081,))
    ])


def image_transform(instance):
    """converts the input image of Bytes Array into Tensor

    Args:
        instance (dict): The request input to make an inference
        request for.

    Returns:
        list: Returns the data key's value and converts that into a list
        after converting it into a tensor
    """
    byte_array = base64.b64decode(instance["data"])
    image = Image.open(io.BytesIO(byte_array))
    instance["data"] = image_processing(image).tolist()
    return instance


class Transformer(object):
    """ A class object for the data handling activities of Image Classification
    Task and returns a KServe compatible response.

    Args:
        kserve (class object): The Model class from the KServe
        module is passed here.
    """
    def __init__(self):
        pass

    def preprocess(self, inputs: Dict) -> Dict:
        """Pre-process activity of the Image Input data.

        Args:
            inputs (Dict): KServe http request

        Returns:
            Dict: Returns the request input after converting it into a tensor
        """
        return {'instances': [image_transform(instance) for instance in inputs['instances']]}

    def postprocess(self, inputs: List) -> List:
        """Post process function of Torchserve on the KServe side is
        written here.

        Args:
            inputs (List): The list of the inputs

        Returns:
            List: If a post process functionality is specified, it converts that into
            a list.
        """
        return inputs
