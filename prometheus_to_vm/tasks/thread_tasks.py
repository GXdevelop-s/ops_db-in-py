import time

from prometheus_to_vm.client.prom_model import PromModel
from prometheus_to_vm.client.vm_model import VmModel
from prometheus_to_vm.constant.time_control import TimeControl


class ThreadTasks:
    def __init__(self, data_set, data_set_lock):
        self.data_set = data_set
        self.data_set_lock = data_set_lock
        self.time_series_list = []  # 修改全部的targets
        # self.metric_name = 'redis_allocator_frag_ratio'
        self.metric_name = 'redis_allocator_frag_bytes'
        self.prometheus_client = PromModel()
        self.vm_client = VmModel(self.metric_name)
        self.count = TimeControl.THREAD_RETRY_TIME

    def master_task(self):
        '''
        主任务，有着查询、插入、判断的执行调度逻辑
        '''
        data = self.prom_task()  # 有值传值，没值传None
        for i in range(TimeControl.THREAD_RETRY_TIME):
            flag = self.check_replica(data)
            print(flag)
            # 有新数据在进行插入，否则就再尝试2次
            if flag:
                print('准备调用vm_task')
                self.vm_task(data)
                self.update_global_set(data)
            else:
                # 休眠1s再查
                time.sleep(TimeControl.THREAD_RETRY_TIME_INTERVAL_SECONDS)
                data = self.prom_task()  # 有值传值，没值传None

    def prom_task(self):
        '''
        执行查询最近一次的数据
        :return:查询到的数据
        '''
        data = PromModel().get_metrics(self.metric_name)
        if data is not None:
            return data
        else:
            return None

    def vm_task(self, data):
        '''
        插入数据
        :return: nothing
        '''
        #构建符合vm结构的数据
        vm_data = {
            "metric": {
                "__name__": data["metric"]["__name__"]
            },
            "values": [int(data["value"][1])],
            "timestamps": [int(float(data["value"][0])*1000)]
        }
        print('准备调用vm——client')
        VmModel(self.metric_name).insert_into_vm(vm_data)

    def check_replica(self, data):
        '''
        用当次查询到的数据和全局set进行对比，
        :return: True代表有新数据，False代表不是新数据，还是上一次的数据
        '''
        # 值为空，返会 False
        if data is None:
            return False
        # 遍历set，和该次传入的值进行比较，读取无需加锁
        inner_flag = True
        for history_data in self.data_set:
            if history_data == data:
                return False  # 发现有一样的直接返回
            else:
                continue  # 不一样就继续检查
        return inner_flag

    def update_global_set(self, data):
        '''
        更新set
        :return:
        '''
        # 加锁操作set，添加新数据
        with self.data_set_lock:
            self.data_set.append(data)
