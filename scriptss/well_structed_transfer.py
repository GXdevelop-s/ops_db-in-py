import sys
from datetime import datetime

import pandas as pd
import json

from confluent_kafka import Producer


def pre_process(df):
    def delete_sec_info(ts):
        dt = datetime.fromtimestamp(ts / 1000)  # 将毫秒级时间戳转换为datetime对象
        new_dt = dt.replace(second=0, microsecond=0)  # 去除秒和微秒
        # 再转换成timestamp,秒级别
        neww_dt = new_dt.timestamp()
        return int(neww_dt * 1000)

    df['created_at'] = df['created_at'].apply(delete_sec_info)

    def transfer_str(sql):
        sql = str(sql)
        return sql

    df['sql_stt_c'] = df['sql_stt_c'].apply(transfer_str)
    return df


def data_construct(pre_processed_df):
    duplicated_ts = []
    # 先把所有ts全加进来
    for index, row in pre_processed_df.iterrows():
        duplicated_ts.append(row['created_at'])
    # 去重
    unique_ts = list(set(duplicated_ts))
    # 排序
    unique_ts.sort()
    # 构造一个大字典，每一个ts都是一个key，其对应的value是一个空dict
    data = {ts: {} for ts in unique_ts}
    '''
        data = {
        1779808924000: {  }
        1749280892400: {  },
        .......
    }
    '''
    # 初始化data
    for index, row in pre_processed_df.iterrows():
        # 即使是有重复的，但是赋值都是0，所以覆盖之前的也不影响
        data[row['created_at']][row['sql_stt_c']] = {
            'count': 0,
            'total': 0,
            'average': 0
        }
    '''
        data = {
        1779808924000: {  
            'sql1':{
                'count':0,
                'total':0,
                'average': 0
            },
            'sql2':{
                'count':0,
                'total':0,
                'average': 0
            },
            .......
          },
        1749280892400: {  
            'sql1':{
                'count':0,
                'total':0,
                'average': 0
            },
            'sql2':{
                'count':0,
                'total':0,
                'average': 0
            },
            .......
         },
        .......
    '''
    return data


def fill_in_data(pre_processed_df, initialized_data):
    to_fill_data = initialized_data
    # 开始遍历赋值
    for index, row in pre_processed_df.iterrows():
        # 计数累加
        to_fill_data.get(row['created_at'])[row['sql_stt_c']]['count'] += 1
        # 总量累加
        to_fill_data.get(row['created_at'])[row['sql_stt_c']]['total'] += row['latency_msec']
        # 算平均值
        to_fill_data.get(row['created_at'])[row['sql_stt_c']]['average'] = (to_fill_data.get(row['created_at'])[
            row['sql_stt_c']]['total']) / (to_fill_data.get(row['created_at'])[row['sql_stt_c']]['count'])
    return to_fill_data


def make_metrics(full_data):
    metric_list = []
    for key, value in full_data.items():
        for inner_key, inner_value in value.items():
            temp_count_metric = {
                "_ts": key,  # 时间戳 13位 必须
                "_metric": 'count',  # 指标名称 必须
                "_value": inner_value['count'],  # 指标值  必须
                "_tags": {  # 指标维度信息，可根据实际数据生成
                    "sql": inner_key,
                },
                # 其它可能的参数
            }
            metric_list.append(temp_count_metric)
            temp_total_metric = {
                "_ts": key,  # 时间戳 13位 必须
                "_metric": 'total',  # 指标名称 必须
                "_value": inner_value['total'],  # 指标值  必须
                "_tags": {  # 指标维度信息，可根据实际数据生成
                    "sql": inner_key,
                },
                # 其它可能的参数
            }
            metric_list.append(temp_total_metric)
            temp_average_metric = {
                "_ts": key,  # 时间戳 13位 必须
                "_metric": 'average',  # 指标名称 必须
                "_value": inner_value['average'],  # 指标值  必须
                "_tags": {  # 指标维度信息，可根据实际数据生成
                    "sql": inner_key,
                },
                # 其它可能的参数
            }
            metric_list.append(temp_average_metric)
    return metric_list


def send_metrics(standard_list):
    kafka_config = {
        "bootstrap.servers": '10.1.22.113:9092',
        "queue.buffering.max.messages": 100000000,
        "batch.num.messages": 1000000,
        "linger.ms": 5
    }
    producer = Producer(kafka_config)
    # 开始发送
    while len(standard_list) > 0:
        ready_element = standard_list.pop(0)
        producer.produce('garrett_test_s1', value=json.dumps(ready_element))
        # producer.poll(0)  # 允许生产者处理事件队列
    producer.flush()


if __name__ == '__main__':
    original_df = pd.read_excel('/Users/garrett/office_file/hera数据/sql数据.xlsx',
                                sheet_name='Result 1',
                                usecols=['sql_stt_c', 'created_at', 'latency_msec',
                                         'trans_transfer_ms'])
    # 预处理
    pre_processed_df = pre_process(original_df)
    # 初始化数据结构
    constructed_data = data_construct(pre_processed_df)
    # 填值并计算
    filled_data = fill_in_data(pre_processed_df, constructed_data)
    # 构造发送列表
    ready_to_send = make_metrics(filled_data)
    print(len(ready_to_send))
    # 发送
    send_metrics(ready_to_send)
    # 完成
    print('finished')

