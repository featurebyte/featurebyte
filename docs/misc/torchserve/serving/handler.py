import logging

import torch
from sentence_transformers import SentenceTransformer
from ts.torch_handler.base_handler import BaseHandler

logger = logging.getLogger(__name__)


class TransformerHandler(BaseHandler):
    def __init__(self):
        super().__init__()
        self.initialized = False

    def initialize(self, ctx):
        self.manifest = ctx.manifest
        properties = ctx.system_properties

        self.device = "cuda" if torch.cuda.is_available() else "cpu"

        self.model = SentenceTransformer("sentence-transformers/all-MiniLM-L12-v2")
        self.model.encode(["warmup"])

        self.initialized = True

    def preprocess(self, requests):
        logging.info("Accepted %s requests, data: %s", len(requests), requests)

        texts = []
        for data in requests:
            texts.append(data["body"]["text"])
        return texts

    def inference(self, inputs):
        embeddings = self.model.encode(inputs, show_progress_bar=False)
        return embeddings.tolist()

    def postprocess(self, inference_output):
        results = []
        for out in inference_output:
            results.append({"result": out})
        return results
