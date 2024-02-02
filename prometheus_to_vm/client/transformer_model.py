import threading
import time
from concurrent.futures import ThreadPoolExecutor
from prometheus_to_vm.tasks.thread_tasks import ThreadTasks
from prometheus_to_vm.constant.time_control import TimeControl


class TransformerModel:
    def __init__(self):
        self.data_set = set()  # 全局变量set封装10个历史数据，用来判断是否是历史数据；使用add和pop方法视为一个只存放独立元素的序列
        self.data_set_lock = threading.Lock()  # 保证线程安全的锁
        self.thread_pool = ThreadPoolExecutor(max_workers=5)  # 线程池
        self.task_manager = ThreadTasks(self.data_set, self.data_set_lock)

    def transform(self):
        print('ok1')
        # 第一次开始定时器
        threading.Timer(0.1, self.schedule_task).start()

    def schedule_task(self):
        print('ok2')
        # 调度派遣线程
        self.thread_pool.submit(self.task_manager.master_task)
        # 若是len（set）大于10需要将集合中的左边的元素移除视为无用的历史数据
        if len(self.data_set) > TimeControl.HISTORY_DATA_NUMS_MAX:
            # 全局变量加锁移除
            with self.data_set_lock:
                self.data_set.pop()  # 移除左边第一个

        # 定时器完成任务后，再次调用schedule_task来再次递归调用设置定时器，5秒启动
        threading.Timer(TimeControl.TIMER_INTERVAL_SECONDS, self.schedule_task).start()

    def test_transform(self):
        while True:
            self.thread_pool.submit(self.task_manager.master_task)
            # 若是len（set）大于10需要将集合中的左边的元素移除视为无用的历史数据
            if len(self.data_set) > TimeControl.HISTORY_DATA_NUMS_MAX:
                # 全局变量加锁移除
                with self.data_set_lock:
                    self.data_set.pop()  # 移除左边第一个
            print('!!!')
            time.sleep(TimeControl.SCRAPE_INTERVAL)
