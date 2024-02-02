import json

import urllib3


class VmModel(object):
    def __init__(self, metric_name):
        self.insert_url = 'http://127.0.0.1:8428/api/v1/import'
        self.headers = {'Content-Type': 'application/json'}
        self.http_pool = urllib3.PoolManager()
        self.metric_name = metric_name

    def insert_into_vm(self, query_result):
        '''
        Insert the metrics into the VM
        '''
        json_data = json.dumps(query_result)
        print('发送')
        response = self.http_pool.request('POST', self.insert_url, body=json_data, headers=self.headers)
        print(response.status, f"data is {response.data}")
