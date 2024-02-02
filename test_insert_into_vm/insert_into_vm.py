import time
import json
import requests
import urllib3


def insert_into_vm1(business, data_source, ad_task, ts, rows, product_type=1):
    # VictoriaMetrics 的地址和端口
    url = 'http://127.0.0.1:8428/api/v1/import'

    # 处理标签值
    labels = []
    if business:
        labels.append(f'business=\"{str(business)}\"')
    if ad_task:
        labels.append(f'ad_task=\"{str(ad_task)}\"')
    if data_source:
        labels.append(f'data_source=\"{str(data_source)}\"')
    if product_type:
        labels.append(f'product_type=\"{str(product_type)}\"')
    label_part = ','.join(labels)

    # 处理value值
    value_part = ''
    params = [int(ts), rows]
    for param in params:
        value_part += f' {param}'  # 留出空格

    # 要插入的数据，按照 VictoriaMetrics 的格式
    data = f'analysis_data_volume_rows{{{label_part}}} {value_part}'

    # 配置并发请求
    response = requests.post(url, data=data)

    # 检查响应
    if response.ok:
        print("insert_into_vm1数据插入成功")
        print(response.content)
    else:
        print("insert_into_vm1数据插入失败：", response.text)


def insert_into_vm2(business, data_source, ad_task, ts, rows, product_type=1):
    # VictoriaMetrics 的地址和端口
    url = 'http://127.0.0.1:8428/api/v1/import'
    # json数据
    data = json.dumps({
        "metric": {
            "__name__": "analysis_data_volume_rows",
            "business": str(business),
            "data_source": str(data_source),
            "ad_task": str(ad_task),
            "product_type": str(product_type)
        },
        "values": [rows],
        "timestamps": [ts]
    })

    # 配置并发请求
    headers = {"Content-Type": "application/json"}
    http_pool = urllib3.PoolManager()
    response = http_pool.request('POST', url, body=data, headers=headers)
    print(response.status, f"data is {response.data}")


if __name__ == '__main__':
    # 参数：business, data_source, ad_task, ts, rows, product_type=1
    print('insert_into_vm1:')
    # insert_into_vm1(1, 5, 6, int(time.time()*1000), 777, 1)
    print('\ninsert_into_vm2:')
    insert_into_vm2(1, 5, 6, int(time.time()*1000), 777, 1)
