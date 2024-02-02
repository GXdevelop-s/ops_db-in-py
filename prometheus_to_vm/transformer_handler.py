import time

from client.transformer_model import TransformerModel
from constant.time_control import TimeControl

class TransformerHandler:
    def __init__(self):
        self.client = TransformerModel()
        self.scrape_interval = TimeControl.SCRAPE_INTERVAL  # 5秒一抓取

    def run(self):
            self.client.test_transform()

