import os
import json
import pytz
import time
import asyncio
import pandas as pd
from pathlib import Path
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, timezone
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionTimeout


class TraceExtractor:
    def __init__(self, url: str, username: str, password: str):
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
        self.indices_template = ".ds-traces-apm-default-*"
        
        
    def timezone_adjust(self, local_datetime):
        utc_time = local_datetime.astimezone(pytz.utc)
        # 格式化为 ISO 8601 格式字符串
        timestamp_str = utc_time.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        return timestamp_str
    
    def sort_by_timestamp(self, element):
        return element['timestamp']['us']
    
    def trace_processing(self, traces):
        grouped_trace = {}
        # 以trace id为key，创建字典，存储data['_source']的内容
        for trace in traces:
            trace_id = trace['_source']['trace']['id']
            if trace_id in grouped_trace:
                grouped_trace[trace_id].append(trace['_source'])
            else:
                grouped_trace[trace_id] = [trace['_source']]

        timestamp_list = []
        cmdb_id_list = []
        span_id_list = []
        trace_id_list = []
        duration_list = []
        type_list = []
        status_code_list = []
        operation_name_list = []
        parent_span_id = []

        # 将grouped_trace中每个trace id下的list按照时间戳从小到大排序
        for trace_id, trace_list in grouped_trace.items():
            trace_list = sorted(trace_list, key=self.sort_by_timestamp)
            # 从每条trace中提取出需要的数据
            for trace in trace_list:
                try:
                    # 判断processor下的name是span还是transaction
                    processor_name = trace['processor']['event']
                    if 'health' in trace[processor_name]['name'] or 'POST unknown route' in trace[processor_name]['name']:
                        continue
                    span_id_list.append(trace[processor_name]['id'])
                    duration_list.append(trace[processor_name]['duration']['us'])
                    type_list.append(trace[processor_name]['type'])
                    operation_name_list.append(trace[processor_name]['name'])
                    timestamp_list.append(trace['timestamp']['us'])
                    cmdb_id_list.append(trace['service']['name'])
                    trace_id_list.append(trace_id)
                    # 判断status_code是否存在
                    if 'http' in trace:
                        if 'response' in trace['http']:
                            if 'status_code' in trace['http']['response']:
                                status_code_list.append(trace['http']['response']['status_code'])
                            else:
                                status_code_list.append(0)
                        else:
                            status_code_list.append(0)
                    else:
                        status_code_list.append(0)
                    # 判断parent_id是否存在
                    if 'parent' in trace:
                        parent_span_id.append(trace['parent']['id'])
                    else:
                        parent_span_id.append('')
                except Exception as e:
                    print(trace)

        # 创建dataframe
        df = pd.DataFrame({
            'timestamp': timestamp_list,
            'cmdb_id': cmdb_id_list,
            'span_id': span_id_list,
            'trace_id': trace_id_list,
            'duration': duration_list,
            'type': type_list,
            'status_code': status_code_list,
            'operation_name': operation_name_list,
            'parent_span': parent_span_id
        })

        return df

    def trace_extract(self, start_time, end_time, path):
        print(start_time)
        original_start_time = start_time
        time_interval = 5 * 60
        csv_list = []
        os.makedirs(path, exist_ok=True)
        while start_time < end_time:
            current_end_time = start_time + time_interval
            if current_end_time > end_time:
                current_end_time = end_time
            data = self.trace_extract_(start_time=start_time, end_time=current_end_time)
            if len(data) != 0:
                data.to_csv(f'{path}/trace-{start_time}_{current_end_time}.csv', index=False)
                csv_list.append(f'{path}/trace-{start_time}_{current_end_time}.csv')
            start_time = current_end_time
            time.sleep(1)
            
        data_list = []
        for i, folder_path in enumerate(csv_list):
            data = pd.read_csv(folder_path)
            data_list.append(data)
            os.remove(folder_path)
        all_data = pd.concat(data_list)
        all_data = all_data.reset_index(drop=True)
        start_datetime = datetime.fromtimestamp(original_start_time)
        formatted_start_time = start_datetime.strftime("%Y-%m-%d_%H-%M-%S")
        file_path = os.path.join(path, f"trace_{formatted_start_time}.csv")
        all_data.to_csv(file_path)

    # trace数据导出
    def trace_extract_(self, start_time=None, end_time=None):
        trace_query_size = 5000

        if isinstance(start_time, int):
            start_time = datetime.fromtimestamp(start_time)
        if isinstance(end_time, int):
            end_time = datetime.fromtimestamp(end_time)

        start_time = self.timezone_adjust(start_time)
        end_time = self.timezone_adjust(end_time)

        query = {
            "size": trace_query_size,
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": start_time,
                                    "lte": end_time
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
        try:
            page = self.elastic.search(index=self.indices_template, body=query, scroll='15s')

            data.extend(page["hits"]["hits"])
            scroll_id = page['_scroll_id']

            while True:
                page = self.elastic.scroll(scroll_id=scroll_id, scroll='15s')
                hits_len = len(page["hits"]["hits"])
                data.extend(page["hits"]["hits"])
                if hits_len < trace_query_size:
                    break
                scroll_id = page["_scroll_id"]
                time.sleep(1)

        except ConnectionTimeout as e:
            print('Connection Timeout:', e)

        df = self.trace_processing(data)
        ed_time = time.time()
        print('run time: ', ed_time - st_time)
        return df
    

class TraceMaintainer:
    def __init__(
        self,
        extractor: TraceExtractor,
        output_root: str = "./multi_modal_data",
        state_file: str = "trace_state.json",
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
        print(f"📦 提取Trace: {start_dt_utc} ~ {end_dt_utc} (UTC)")

        tz_utc8 = ZoneInfo("Asia/Shanghai")
        date_str = start_dt_utc.astimezone(tz_utc8).strftime("%Y-%m-%d")
        output_dir = self.output_root / date_str / "trace"
        output_dir.mkdir(parents=True, exist_ok=True)

        success = False
        for attempt in range(1, self.max_retries + 1):
            try:
                await asyncio.to_thread(
                    self.extractor.trace_extract,
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
# Shortcut 工具函数
# ========================
async def extract_full_day(
    maintainer: TraceMaintainer,
    date_str: str,
    tz: timezone = timezone.utc
):
    """提取指定日期的完整 trace（按小时）"""
    start_dt_local = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=tz)
    start_dt_utc = start_dt_local.astimezone(timezone.utc)

    for h in range(24):
        hour_dt = start_dt_utc + timedelta(hours=h)
        await maintainer.extract_for_hour(hour_dt)


# ========================
# 使用示例
# ========================
async def main():
    extractor = TraceExtractor(
        url="https://localhost:9200",
        username="elastic",
        password="elastic"
    )
    
    maintainer = TraceMaintainer(extractor, output_root="./multi_modal_data")

    # 启动定时模式
    # await maintainer.start()

    # 或者：提取某一天的所有 trace
    day_list = [4, 5, 6, 7, 8, 9, 10]
    for day in day_list:
        await extract_full_day(maintainer, f"2025-12-{day}", tz=timezone(timedelta(hours=8)))  # 输入北京时间


if __name__ == "__main__":
    asyncio.run(main())
