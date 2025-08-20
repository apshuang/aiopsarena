import os
import json
import time
import asyncio
import pandas as pd
from pathlib import Path
from kubernetes import config, client
from datetime import datetime, timedelta, timezone
from elasticsearch import Elasticsearch
from zoneinfo import ZoneInfo
from elasticsearch.exceptions import ConnectionTimeout


# ========================
# 1. 日志提取器
# ========================
class LogExtractor:
    def __init__(self, url: str, username: str, password: str, index: str):
        self.elastic = Elasticsearch(
            [url],
            basic_auth=(username, password),
            verify_certs=False,
            request_timeout=60,
            max_retries=5,
            retry_on_timeout=True
        )
        
        # 屏蔽 InsecureRequestWarning
        import warnings
        import urllib3
        warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)
        
        self.index = index
        config.kube_config.load_kube_config(config_file='~/.kube/config')
        k8s_client = client.CoreV1Api()
        self.log_pod_list = [pod for pod in self.get_pod_list(k8s_client) if not pod.startswith('loadgenerator-') and not pod.startswith('redis-cart')]
        
    def get_pod_list(self, k8s_client, namespace='default'):
        pod_list = k8s_client.list_namespaced_pod(namespace)
        pod_names = []
        # 遍历获取到的Pods,把名称存储到列表中
        for pod in pod_list.items:
            pod_names.append(pod.metadata.name)
        return pod_names
  
    def message_extract(self, json_str):
        message = json_str
        try:
            if 'severity' in json_str:
                data = json.loads(json_str)
                message = ''.join(['severity:', data['severity'], ',', 'message:', data['message']])
            elif 'level' in json_str:
                data = json.loads(json_str)
                message = ''.join(['level:', data['level'], ',', 'message:', data['message']])
        except:
            pass
        return message    
    
    
    def log_processing(self, logs):
        log_id_list = []
        ts_list = []
        date_list = []
        pod_list = []
        ms_list = []
        for log in logs:
            try:
                cmdb_id = log['_source']['kubernetes']['pod']['name']
                if cmdb_id not in self.log_pod_list:
                    continue
                timestamp = log['_source']['@timestamp']
                dt = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
                dt = dt.replace(tzinfo=timezone.utc)   # 关键！加上 UTC，否则会转错！！
                timestamp = dt.timestamp()
                format_ts = log['_source']['@timestamp']
                message = self.message_extract(log['_source']['message'])
            except Exception as e:
                continue
            log_id_list.append(log['_id'])
            pod_list.append(cmdb_id)
            date_list.append(format_ts)
            ts_list.append(timestamp)
            ms_list.append(message)
        dt = pd.DataFrame({
            'log_id': log_id_list,
            'timestamp': ts_list,
            'date': date_list,
            'cmdb_id': pod_list,
            'message': ms_list
        })
        return dt
    
    def log_for_query_filter(self, logs):
        filtered_log = []
        for log in logs:
            try:
                cmdb_id = log['_source']['kubernetes']['pod']['name']
                if cmdb_id not in self.log_pod_list:
                    continue
            except Exception as e:
                continue
            filtered_log.append(log)
        return filtered_log
    
    def choose_index_template(self, indices, start_time, end_time):
        indices_template = set()
        for index in indices:
            date_str = '.'.join(index.split('-')[1].split('.')[:-1])
            indices_template.add('logstash-' + date_str + '*')

        start_datetime_utc = datetime.fromtimestamp(start_time, tz=timezone.utc)
        end_datetime_utc = datetime.fromtimestamp(end_time, tz=timezone.utc)

        dates_in_range = set()
        current_datetime = start_datetime_utc

        while current_datetime <= end_datetime_utc:
            dates_in_range.add('logstash-' + current_datetime.strftime("%Y.%m.%d") + '*')
            current_datetime += timedelta(days=1)

        dates_in_range.add('logstash-' + end_datetime_utc.strftime("%Y.%m.%d") + '*')

        selected_patterns = indices_template.intersection(dates_in_range)

        return selected_patterns

    def log_extract(self, start_time, end_time, path):
        print(start_time)
        original_start_time = start_time
        time_interval = 5 * 60
        csv_list = []
        os.makedirs(path, exist_ok=True)
        while start_time < end_time:
            current_end_time = start_time + time_interval
            if current_end_time > end_time:
                current_end_time = end_time
            data = self.log_extract_(start_time=start_time, end_time=current_end_time)
            if len(data) != 0:
                # 临时导出
                data.to_csv(f'{path}/log-{start_time}_{current_end_time}.csv', index=False)
                csv_list.append(f'{path}/log-{start_time}_{current_end_time}.csv')
            start_time = current_end_time
            time.sleep(1)

        data_list = []
        for i, folder_path in enumerate(csv_list):
            data = pd.read_csv(folder_path)
            data_list.append(data)
            os.remove(folder_path)
        all_data = pd.concat(data_list)
        all_data = all_data.sort_values(by="timestamp")
        all_data = all_data.reset_index(drop=True)
        
        start_datetime = datetime.fromtimestamp(original_start_time)
        formatted_start_time = start_datetime.strftime("%Y-%m-%d_%H-%M-%S")
        file_path = os.path.join(path, f"log_{formatted_start_time}.csv")
        all_data.to_csv(file_path)

    def to_iso_utc(self, x):
        """转成 ES 里的 ISO8601 UTC 字符串（带 Z）"""
        if isinstance(x, int):
            dt = datetime.fromtimestamp(x, tz=timezone.utc)
        elif isinstance(x, datetime):
            dt = x.astimezone(timezone.utc) if x.tzinfo else x.replace(tzinfo=timezone.utc)
        else:
            raise TypeError("时间必须是 int(秒) 或 datetime")
        return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # log数据导出功能
    def log_extract_(self, start_time, end_time):

        quert_size = 7500

        # 获取时间段内需要使用的indices
        indices = self.elastic.indices.get(index="logstash-*")
        indices = self.choose_index_template(indices, start_time, end_time)
        print('indices', indices)

        # timestamp是全球统一的，而elastic存储的时候是以utc存储的，所以这里必须转为utc
        # if isinstance(start_time, int):
        #     start_time = datetime.fromtimestamp(start_time, tz=timezone.utc)
        # if isinstance(end_time, int):
        #     end_time = datetime.fromtimestamp(end_time, tz=timezone.utc)
        
        # start_time = start_time.isoformat().replace("+00:00", "Z")
        # end_time = end_time.isoformat().replace("+00:00", "Z")
        
        # print(start_time)
        # print(end_time)
        
        start_iso = self.to_iso_utc(start_time)
        end_iso = self.to_iso_utc(end_time)
        print(start_iso)
        print(end_iso)
        
        
        query = {
            "size": quert_size,
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": start_iso,
                                    "lte": end_iso
                                }
                            }
                        }
                    ]
                }
            },
            "sort": ["_doc"]
        }
        
        data = []

        st_time = time.time()
        for index in indices:
            try:
                print(index)
                page = self.elastic.search(index=index, body=query, scroll='15s')  # type: ignore

                data.extend(page["hits"]["hits"])
                scroll_id = page['_scroll_id']

                while True:
                    page = self.elastic.scroll(scroll_id=scroll_id, scroll='15s')
                    hits_len = len(page["hits"]["hits"])
                    data.extend(page["hits"]["hits"])
                    if hits_len < quert_size:
                        break
                    scroll_id = page["_scroll_id"]

            except ConnectionTimeout as e:
                print('Connection Timeout:', e)

        print('search time: ', time.time() - st_time)
        st_time = time.time()
        data = self.log_processing(data)
        print('process time:', time.time() - st_time)
        return data
    

# ========================
# 2. 日志维护器
# ========================
class LogMaintainer:
    def __init__(
        self,
        extractor: LogExtractor,
        output_root: str = "./multi_modal_data",
        state_file: str = "log_state.json",
        max_retries: int = 1
    ):
        self.extractor = extractor
        self.output_root = Path(output_root)
        self.state_file = Path(state_file)
        self.max_retries = max_retries
        self.state = self._load_state()
        self.running = False

    def _load_state(self) -> dict:
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_state(self):
        try:
            with open(self.state_file, "w") as f:
                json.dump(self.state, f)
        except Exception as e:
            print(f"⚠️ Failed to save state: {e}")

    async def start(self):
        """启动每小时定时任务"""
        self.running = True
        while self.running:
            now = datetime.now(timezone.utc)
            next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            wait_seconds = (next_hour - now).total_seconds()
            await asyncio.sleep(wait_seconds)
            await self._extract_previous_hour()

    async def stop(self):
        self.running = False

    async def _extract_previous_hour(self):
        end_dt = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        start_dt = end_dt - timedelta(hours=1)
        await self.extract_for_hour(start_dt)

    async def extract_for_hour(self, start_dt_utc: datetime):
        if start_dt_utc.tzinfo != timezone.utc:
            raise ValueError("start_dt_utc 必须是 UTC 时间")

        hour_key = start_dt_utc.strftime("%Y-%m-%d_%H:%M:%S")
        if self.state.get(hour_key, False):
            print(f"⏩ {hour_key} 已提取，跳过")
            return

        end_dt_utc = start_dt_utc + timedelta(hours=1)
        print(f"📦 提取日志: {start_dt_utc} ~ {end_dt_utc} (UTC)")

        tz_utc8 = ZoneInfo("Asia/Shanghai")

        date_str = start_dt_utc.astimezone(tz_utc8).strftime("%Y-%m-%d")  # 存储的时候用UTC+8
        output_dir = self.output_root / date_str / "log"
        output_dir.mkdir(parents=True, exist_ok=True)

        success = False
        for attempt in range(1, self.max_retries + 1):
            try:
                await asyncio.to_thread(
                    self.extractor.log_extract,
                    int(start_dt_utc.timestamp()),
                    int(end_dt_utc.timestamp()),
                    output_dir
                )
                success = True
                break
            except Exception as e:
                wait_time = 2 ** attempt
                print(f"❌ 第{attempt}次提取失败: {e}，{wait_time}s后重试...")
                await asyncio.sleep(wait_time)

        if success:
            self.state[hour_key] = True
            self._save_state()


# ========================
# 3. Shortcut 工具函数
# ========================
async def extract_full_day(
    maintainer: LogMaintainer,
    date_str: str,
    tz: timezone = timezone.utc
):
    """
    提取指定日期的完整日志（按小时）
    :param date_str: 'YYYY-MM-DD'
    :param tz: 输入日期的时区（默认 UTC）
    """
    start_dt_local = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=tz)
    start_dt_utc = start_dt_local.astimezone(timezone.utc)

    for h in range(6):
        hour_dt = start_dt_utc + timedelta(hours=h)
        await maintainer.extract_for_hour(hour_dt)


# ========================
# 使用示例
# ========================

# ATTENTION!! 整份代码到处充斥着时区转换的问题，因为elastic存储数据时用的是utc，所以需要各种转换
# ATTENTION!! 然而如果把unix时间戳转换为可读的datetime格式，非常容易面临转换时默认utc+8的问题，所以一定要认真检查

async def main():
    extractor = LogExtractor(
        url="https://localhost:9200",
        username="elastic",
        password="elastic",
        index="logstash-*"
    )
    
    maintainer = LogMaintainer(extractor, output_root="./multi_modal_data")

    # 启动定时模式
    # await maintainer.start()

    # 或者：提取某一天的所有日志
    await extract_full_day(maintainer, "2025-08-19", tz=timezone(timedelta(hours=8)))  # 输入北京时间


if __name__ == "__main__":
    asyncio.run(main())
