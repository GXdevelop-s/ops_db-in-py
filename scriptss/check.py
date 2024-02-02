from datetime import datetime

import pandas as pd


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


if __name__ == '__main__':
    original_df = pd.read_excel('/Users/garrett/office_file/hera数据/sql数据.xlsx',
                                sheet_name='Result 1',
                                usecols=['sql_stt_c', 'created_at', 'latency_msec',
                                         'trans_transfer_ms'])
    pre_processed_df = pre_process(original_df)
    all_metrics = []
    for index, row in pre_processed_df.iterrows():
        temp = [row['created_at'], row['sql_stt_c']]
        all_metrics.append(temp)
    need_metrics = []
    for item in all_metrics:
        if item[1] == 'SP_EXECUTESQL':
            need_metrics.append(tuple(item))
    result = list(set(need_metrics))
    print(len(result))
