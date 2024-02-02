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


def master_process_first(pre_precessed_df):
    # 最终的结果
    result = {}
    for index, row in pre_precessed_df.iterrows():
        # 初次遇到某个sql模式
        if row['sql_stt_c 提参后sql'] not in result.keys():
            print(row['sql_stt_c 提参后sql'])
            count = 1  # 记录当下出现的次数
            current_latency_ms = row['latency_msec 业务响应时间']  # 本次延迟耗时
            latency_total_ms = row['latency_msec 业务响应时间']  # 延迟总耗时=当前sql延迟耗时
            latency_avg_ms = row['latency_msec 业务响应时间']  # 平均延迟耗时=当前sql延迟耗时
            current_transfer_ms = row['trans_transfer_ms 网络响应时间']  # 本次传输耗时
            transfer_total_ms = row['trans_transfer_ms 网络响应时间']  # 传输总耗时=当前sql传输耗时
            transfer_avg_ms = row['trans_transfer_ms 网络响应时间']  # 平均传输耗时=当前sql传输耗时
            current_cost = row['latency_msec 业务响应时间'] + row['trans_transfer_ms 网络响应时间']  # 本次sql耗时
            cost_total_ms = row['latency_msec 业务响应时间'] + row[
                'trans_transfer_ms 网络响应时间']  # sql总耗时=当前延迟总耗时+当前传输总耗时
            cost_avg_ms = row['latency_msec 业务响应时间'] + row[
                'trans_transfer_ms 网络响应时间']  # sql平均耗时=当前延迟平均耗时+当前传输平均耗时
            # 为每一个sql模式都创建一个键
            result[row['sql_stt_c 提参后sql']] = []
            # 向二维数组里面塞值
            result[row['sql_stt_c 提参后sql']].append(
                [row['created_at 创建时间'], count, current_latency_ms, latency_total_ms, latency_avg_ms,
                 current_transfer_ms,
                 transfer_total_ms, transfer_avg_ms, current_cost, cost_total_ms, cost_avg_ms])
        else:
            # 第n次遇到该sql模式，n不等于1
            # 读取该sql模式的二维数组最后一行值
            last_insert = result[row['sql_stt_c 提参后sql']][-1]
            count = last_insert[1] + 1  # 上一次+1
            current_latency_ms = row['latency_msec 业务响应时间']  # 本次延迟耗时
            latency_total_ms = last_insert[3] + current_latency_ms  # 延迟总耗时=上一次+本次
            latency_avg_ms = latency_total_ms / count  # 平均延迟耗时=(上一次+本次)/count
            latency_avg_ms = round(latency_avg_ms, 3)  # 保留三位小数
            current_transfer_ms = row['trans_transfer_ms 网络响应时间']  # 本次传输耗时
            transfer_total_ms = last_insert[6] + current_transfer_ms  # 传输总耗时=上一次+本次
            transfer_avg_ms = transfer_total_ms / count  # 平均传输耗时=(上一次+本次)/count
            transfer_avg_ms = round(transfer_avg_ms, 3)  # 保留三位小数
            current_cost = row['latency_msec 业务响应时间'] + row['trans_transfer_ms 网络响应时间']  # 本次sql耗时
            cost_total_ms = latency_total_ms + transfer_total_ms  # sql总耗时=延迟总耗时+传输总耗时
            cost_avg_ms = cost_total_ms / count  # sql平均耗时
            cost_avg_ms = round(cost_avg_ms, 3)  # 保留三位小数

            # 向二维数组里面塞值
            result[row['sql_stt_c 提参后sql']].append(
                [row['created_at 创建时间'], count, current_latency_ms, latency_total_ms, latency_avg_ms,
                 current_transfer_ms,
                 transfer_total_ms, transfer_avg_ms, current_cost, cost_total_ms, cost_avg_ms])
    return result


def master_process_second(pre_precessed_df):
    # !!!!!!!!!!!!处理count逻辑
    count_result = []

    def further_process_count(cl):
        '''
        处理count值的函数
        :param cl:
        :return:
        '''
        new_count_result = []
        last_single_dict = {}  # 前一个值
        for single_dict in cl:
            # 第一次
            if last_single_dict == {}:
                # 新的指标
                new_count_result.append(single_dict)
                # 更新last_single_dict
                last_single_dict = single_dict
                continue
            # 如果符合指标合并要求，就合并
            if single_dict['_ts'] == last_single_dict['_ts'] and single_dict['_tags']['sql'] == \
                    last_single_dict['_tags']['sql']:
                last_single_dict['_value'] = last_single_dict['_value'] + single_dict['_value']
                # 更新last_single_dict
                new_count_result.pop()
                new_count_result.append(last_single_dict)
            else:
                # 新的指标
                new_count_result.append(single_dict)
                # 更新last_single_dict
                last_single_dict = single_dict
        return new_count_result

    # 专门处理count值
    for index, row in pre_precessed_df.iterrows():
        temp = {
            "_ts": row['created_at'],  # 时间戳 13位 必须
            "_metric": 'count',  # 指标名称 必须
            "_value": 1,  # 指标值  必须
            "_tags": {  # 指标维度信息，可根据实际数据生成
                "sql": row['sql_stt_c'],
            },
            # 其它可能的参数
        }
        count_result.append(temp)
    # 依据ts进行排序
    sorted_count_result = sorted(count_result, key=lambda x: (x['_tags']['sql'], x['_ts']))
    # 再进行处理，further_process函数的处理逻辑需要排序过的
    final_count_result = further_process_count(sorted_count_result)

    # !!!!!!!!!!!!处理latency逻辑
    latency_result = []

    def further_process_latency(cl):
        '''
        处理latency值的函数
        :param cl:
        :return:
        '''
        new_latency_result = []
        last_single_dict = {}  # 前一个值
        for single_dict in cl:
            # 第一次
            if last_single_dict == {}:
                # 新的指标
                new_latency_result.append(single_dict)
                # 更新last_single_dict
                last_single_dict = single_dict
                continue
            # 如果符合指标合并要求，就合并
            if single_dict['_ts'] == last_single_dict['_ts'] and single_dict['_tags']['sql'] == \
                    last_single_dict['_tags']['sql']:
                last_single_dict['_value'] = last_single_dict['_value'] + single_dict['_value']
                # 更新last_single_dict
                new_latency_result.pop()
                new_latency_result.append(last_single_dict)
            else:
                # 新的指标
                new_latency_result.append(single_dict)
                # 更新last_single_dict
                last_single_dict = single_dict
        return new_latency_result

    # 专门处理latency值
    for index, row in pre_precessed_df.iterrows():
        temp = {
            "_ts": row['created_at'],  # 时间戳 13位 必须
            "_metric": 'latency',  # 指标名称 必须
            "_value": row['latency_msec'],  # 指标值  必须
            "_tags": {  # 指标维度信息，可根据实际数据生成
                "sql": row['sql_stt_c'],
            },
            # 其它可能的参数
        }
        latency_result.append(temp)
    # 先依据sql，再依据ts进行排序
    sorted_latency_result = sorted(latency_result, key=lambda x: (x['_tags']['sql'], x['_ts']))
    # 再进行处理，further_process函数的处理逻辑需要排序过的
    final_latency_result = further_process_latency(sorted_latency_result)

    # !!!!!!!!!!!!处理latency_avg值
    final_avg_latency_result = []
    for single_dict in final_latency_result:
        # 以latency为基准，加入count进来，
        ts = single_dict['_ts']
        sql = single_dict['_tags']['sql']
        for item in final_count_result:
            # ts和sql都吻合，就直接算平均值
            if item['_tags']['sql'] == sql and item['_ts'] == ts:
                value = single_dict['_value'] / item['_value']
                temp = {
                    "_ts": single_dict['_ts'],  # 时间戳 13位 必须
                    "_metric": 'avg_latency',  # 指标名称 必须
                    "_value": value,  # 指标值  必须
                    "_tags": {  # 指标维度信息，可根据实际数据生成
                        "sql": sql,
                    },
                    # 其它可能的参数
                }
                final_avg_latency_result.append(temp)
                # 找到立马break
                break
    return final_count_result, final_latency_result, final_avg_latency_result


def send_metrics(final_count_result, final_latency_result, final_avg_latency_result):
    kafka_config = {
        "bootstrap.servers": '10.1.22.113:9092',
        "queue.buffering.max.messages": 100000000,
        "batch.num.messages": 1000000,
        "linger.ms": 5
    }
    producer = Producer(kafka_config)
    kafka_data_holder = []
    # 只要一个列表还有值就继续添加
    while len(final_count_result) > 0 or len(final_latency_result) > 0 or len(final_avg_latency_result) > 0:
        if len(final_count_result) > 0:
            # 移出第一个元素
            element = final_count_result.pop(0)
            # 加入待发送列表
            kafka_data_holder.append(element)
        if len(final_latency_result) > 0:
            element = final_latency_result.pop(0)
            kafka_data_holder.append(element)
        if len(final_avg_latency_result) > 0:
            element = final_avg_latency_result.pop(0)
            kafka_data_holder.append(element)
    # 开始发送
    while len(kafka_data_holder) > 0:
        ready_element = kafka_data_holder.pop(0)
        producer.produce('garrett_test_s5', value=json.dumps(ready_element))
        # producer.poll(0)  # 允许生产者处理事件队列
    producer.flush()


def de_bug(cl):
    result = []
    for single_dict in cl:
        if single_dict['_tags']['sql'] == 'SP_EXECUTESQL':
            print(single_dict['_tags']['sql'])
            result.append(single_dict)
    return result


if __name__ == '__main__':
    original_df = pd.read_excel('/Users/garrett/office_file/hera数据/sql数据.xlsx',
                                sheet_name='Result 1',
                                usecols=['sql_stt_c', 'created_at', 'latency_msec',
                                         'trans_transfer_ms'])
    pre_processed_df = pre_process(original_df)

    final_count_result, final_latency_result, final_avg_latency_result = master_process_second(pre_processed_df)
    # debug_result = []
    # for single_dict in final_latency_result:
    #     if single_dict['_tags']['sql'] == 'SP_EXECUTESQL':
    #         print(single_dict)
    #         ts = single_dict['_ts']
    #         dt = datetime.fromtimestamp(ts / 1000)  # 将毫秒级时间戳转换为datetime对象
    #         print(dt)
    #         debug_result.append(single_dict)
    # print(debug_result)
    # print(len(debug_result))

    print(len(final_count_result), len(final_latency_result), len(final_avg_latency_result))
    # print(final_count_result[0])
    # print(final_count_result, final_latency_result, final_avg_latency_result)

    # 发送到kafka
    #send_metrics(final_count_result, final_latency_result, final_avg_latency_result)
    # print(final_count_result)
    print('finished')
    # sorted_count_result = sorted(count_result, key=lambda x: x['_ts']) 这个排序的依据是x['_ts']，我希望在x['_ts']相同的时候以x['sql']作为二级排序的依据
