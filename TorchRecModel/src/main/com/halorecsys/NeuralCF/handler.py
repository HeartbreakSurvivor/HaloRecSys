import io
import os
import logging
import torch
import json
import numpy as np

from torch.autograd import Variable
from torchvision import transforms

logger = logging.getLogger(__name__)

ncf_config = {
    'num_epoch': 2,
    'batch_size': 256,
    'optimizer': 'adam',
    'lr': 0.001,
    'num_users': 1074,
    'num_items': 982,
    'latent_dim_gmf': 8,
    'latent_dim_mlp': 8,
    'layers': [16,64,32,8],  # layers[0] is the concat of latent user vector & latent item vector
    'l2_regularization': 0.01,
    'device_id': 0,
    'use_cuda': True,
    'train_file': './newRatings.csv',
    'model_name': './TrainedModels/NCF.pth'
}

class NCFHandler(object):
    """
    NCFHandler handler class. This handler takes a user index and a item iindex
    and returns wether the user like the movie
    """

    def __init__(self):
        self.model = None
        self.mapping = None
        self.device = None
        self.initialized = False

    def initialize(self, ctx):
        # """
        # Initialize model. This will be called during model loading time
        # :param context: Initial context contains model server system properties.
        # :return:
        # """
        # #  load the model
        print("**************** initialize *****************")
        properties = ctx.system_properties
        self.device = torch.device("cuda:" + str(properties.get("gpu_id")) if torch.cuda.is_available() else "cpu")
        model_dir = properties.get("model_dir")

        print("model_dir: ", model_dir)
        # Read model serialize/pt file
        model_pt_path = os.path.join(model_dir, "NCF.pth")
        # Read model definition file
        model_def_path = os.path.join(model_dir, "NeuralCF.py")
        if not os.path.isfile(model_def_path):
            raise RuntimeError("Missing the model definition file")

        from NeuralCF import NCF
        state_dict = torch.load(model_pt_path, map_location=self.device)
        self.model = NCF(ncf_config)
        self.model.load_state_dict(state_dict)
        self.model.to(self.device)
        self.model.eval()

        logger.debug('Model file {0} loaded successfully'.format(model_pt_path))
        self.initialized = True

    def preprocess(self, data):
        """
        Transform raw input into model input data.
        :param batch: list of raw requests, should match batch size
        :return: list of preprocessed model input data
        """
        print("(***************** preprocess ***************")
        # Take the input data and make it inference ready
        preprocessed_data = data[0].get("data")
        if preprocessed_data is None:
            preprocessed_data = data[0].get("body")

        print("preprocessed_data: ", preprocessed_data)
        print("preprocessed_data type: ", type(preprocessed_data))

        jsonstring = preprocessed_data["test"]
        print("jsonstring: ", jsonstring)
        print("jsonstring type: ", type(jsonstring))
        user_indexes, item_indexes = [], []
        for js in jsonstring:
            user_indexes.append(js["user_idx"])
            item_indexes.append(js["item_idx"])

        print(user_indexes, item_indexes)
        return user_indexes, item_indexes

    def inference(self, users, items):
        """
        Internal inference methods
        :param model_input: transformed model input data
        :return: list of inference output in NDArray
        """
        print("(***************** inference ***************")

        # construct user and item tensor
        users = torch.tensor(users)
        items = torch.tensor(items)

        self.model.eval()
        users = Variable(users).to(self.device)
        items = Variable(items).to(self.device)

        outputs = self.model.forward(users, items)
        return outputs

    def postprocess(self, inference_output):
        """
        Return inference result.
        :param inference_output: list of inference output
        :return: list of predict results
        """
        print("(***************** postprocess ***************")
        # the Object of type 'float32' is not JSON serializable, so we need to use itm() to convert it to native python type
        res = [[y.item() for y in inference_output.detach().cpu().numpy()]]
        print(res)
        return res


_service = NCFHandler()

def handle(data, context):
    print("************************* handle ***************************")

    if not _service.initialized:
        _service.initialize(context)

    if data is None:
        return None

    user_indexes, item_indexes = _service.preprocess(data)
    outputs = _service.inference(user_indexes, item_indexes)
    res = _service.postprocess(outputs)

    return res