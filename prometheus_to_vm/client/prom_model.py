import json
import time
from urllib.parse import urlencode
import urllib3


class PromModel(object):
    def __init__(self):
        self.query_url = 'http://localhost:9090/api/v1/query?'
        self.headers = {'Content-Type': 'application/json'}
        self.http_pool = urllib3.PoolManager()

    def get_metrics(self, metric_name):
        '''
        Get instant metrics from Prometheus
        '''
        query_params = {
            'query': metric_name,
        }
        url = self.query_url + urlencode(query_params)
        query_result = self.http_pool.request('GET', url)
        if query_result.status == 200:
            query_result = json.loads(query_result.data)
            '''
            # query_result
            {
                "status" : "success",
                "data" : {
                    "resultType" : "vector",
                    "result" : [
                        {
                            "metric" : {
                                "__name__" : "up",
                                "job" : "prometheus",
                                "instance" : "localhost:9090"
                            },
                            "value": [ 1435781451.781, "1" ]
                        },
                    ]
                }
            }
            '''
            result = query_result['data']['result'][0]
            print(f'获取到{result}')
            return result
        else:
            return None
