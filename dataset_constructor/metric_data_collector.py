import os
import json
import asyncio
import pandas as pd
from pathlib import Path
from zoneinfo import ZoneInfo
from kubernetes import config, client
from datetime import datetime, timedelta, timezone
from prometheus_api_client import PrometheusConnect


normal_metrics = [
    # 根据筛选的指标构建需要的指标集合
    # cpu
    "container_cpu_usage_seconds_total",
    "container_cpu_user_seconds_total",
    "container_cpu_system_seconds_total",
    "container_cpu_cfs_throttled_seconds_total",
    "container_cpu_cfs_throttled_periods_total",
    "container_cpu_cfs_periods_total",
    "container_cpu_load_average_10s",
    # memory
    "container_memory_cache",
    "container_memory_usage_bytes",
    "container_memory_working_set_bytes",
    "container_memory_rss",
    "container_memory_mapped_file",
    # spec
    "container_spec_cpu_period",
    "container_spec_cpu_quota",
    "container_spec_memory_limit_bytes",
    "container_spec_cpu_shares",
    # threads
    "container_threads",
    "container_threads_max",
    # filesystem
    "container_fs_reads_total",
    "container_fs_writes_total",
    "container_fs_reads_bytes_total",
    "container_fs_writes_bytes_total",
    # network
    "container_network_receive_errors_total",
    "container_network_receive_packets_dropped_total",
    "container_network_receive_packets_total",
    "container_network_receive_bytes_total",
    "container_network_transmit_bytes_total",
    "container_network_transmit_errors_total",
    "container_network_transmit_packets_dropped_total",
    "container_network_transmit_packets_total"
]
istio_metrics = [
    # istio
    "istio_requests_total",
    "istio_request_duration_milliseconds_sum",
    "istio_request_bytes_sum",
    "istio_response_bytes_sum",
    "istio_request_messages_total",
    "istio_response_messages_total",
    "istio_tcp_sent_bytes_total",
    "istio_tcp_received_bytes_total",
    "istio_tcp_connections_opened_total",
    "istio_tcp_connections_closed_total"
]
filesystem_metrics = [
    # filesystem
    "container_fs_reads_total",
    "container_fs_writes_total",
    "container_fs_reads_bytes_total",
    "container_fs_writes_bytes_total",
    "container_fs_read_seconds_total",
    "container_fs_write_seconds_total",
    "container_fs_io_time_seconds_total",
    "container_fs_io_time_weighted_seconds_total"
]

network_metrics = [
    # network
    "container_network_receive_errors_total",
    "container_network_receive_packets_dropped_total",
    "container_network_receive_packets_total",
    "container_network_receive_bytes_total",
    "container_network_transmit_bytes_total",
    "container_network_transmit_errors_total",
    "container_network_transmit_packets_dropped_total",
    "container_network_transmit_packets_total"
]


class MetricDataExtractor:
    def __init__(self, url: str):
        self.client = PrometheusConnect(url, disable_ssl=True)
        config.kube_config.load_kube_config(config_file='~/.kube/config')
        k8s_client = client.CoreV1Api()
        self.metric_pod_list = [pod for pod in self.get_pod_list(k8s_client) if not pod.startswith('loadgenerator-') and not pod.startswith('redis-cart')]
        self.service_list = self.get_services_list(k8s_client)

    def get_services_list(self, k8s_client, namespace='default'):
        # 获取指定命名空间下的所有服务
        service_list = k8s_client.list_namespaced_service(namespace)

        # 初始化一个列表来存储服务的名称
        services_names = []

        # 遍历获取到的服务，把名称存储到列表中
        for service in service_list.items:
            services_names.append(service.metadata.name)

        return services_names

    def get_pod_list(self, k8s_client, namespace='default'):
        pod_list = k8s_client.list_namespaced_pod(namespace)
        pod_names = []
        # 遍历获取到的Pods,把名称存储到列表中
        for pod in pod_list.items:
            pod_names.append(pod.metadata.name)
        return pod_names    
        
    def time_format_transform(self, time):
        # 将int型time数据转换成date型
        if isinstance(time, int):
            time = datetime.fromtimestamp(time)
        # postman测试用例需要，发的是str型
        if isinstance(time, str) or isinstance(time, float):
            time = int(time)
            time = datetime.fromtimestamp(time)
        return time
    
    def network_kpi_name_format(self, metric):
        kpi_name = metric['__name__']
        if 'interface' in metric:
            kpi_name = '.'.join([kpi_name, metric['interface']])
        return kpi_name
    
    def istio_cmdb_id_format(self, metric):
        pod = metric['pod']
        service = pod.split('-')[0]
        source_service = metric['source_canonical_service']
        destination_service = metric['destination_canonical_service']

        if source_service not in self.service_list and source_service != 'unknown':
            return ''
        if destination_service not in self.service_list and source_service != 'unknown':
            return ''

        if service == source_service:
            cmdb_id = '.'.join([pod, 'source', source_service, destination_service])
        else:
            cmdb_id = '.'.join([pod, 'destination', source_service, destination_service])
        return cmdb_id
    
    def istio_kpi_name_format(self, metric):
        kpi_name = metric['__name__']
        if 'request_protocol' in metric:
            protocol = metric['request_protocol']
            response_code = ''
            if 'response_code' in metric:
                response_code = metric['response_code']

            grpc_response_status = ''
            if 'grpc_response_status' in metric:
                grpc_response_status = metric['grpc_response_status']

            if protocol == 'tcp':
                response_flag = metric['response_flags']
                kpi_name = '.'.join([kpi_name, response_flag])
            else:
                kpi_name = '.'.join([kpi_name, protocol, response_code, grpc_response_status])
        return kpi_name

    def export_all_metrics(self, start_time, end_time, save_path, step=15):
        print('export_all_metrics')
        save_path = os.path.join(save_path, 'metric')
        os.makedirs(save_path, exist_ok=True)
        namespace = 'default'
        
        # 采集container指标
        container_save_path = os.path.join(save_path, 'container')
        os.makedirs(container_save_path, exist_ok=True)
        # 采集istio指标
        istio_save_path = os.path.join(save_path, 'istio')
        os.makedirs(istio_save_path, exist_ok=True)

        interval_time = 2 * 60 * 60
        while start_time < end_time:
            if start_time + interval_time > end_time:
                current_et = end_time
            else:
                current_et = start_time + interval_time
            for metric in normal_metrics:
                if "total" in metric:
                    # 对于累积型指标，提取出原本的情况
                    if metric in network_metrics:
                        data_raw = self.client.custom_query_range(
                        f"rate({metric}{{namespace='{namespace}'}}[3m])",
                        self.time_format_transform(start_time),
                        self.time_format_transform(current_et),
                        step=str(step)
                    )
                    else:
                        data_raw = self.client.custom_query_range(
                            f"rate({metric}{{namespace='{namespace}', container!='', image!~'.*pause.*'}}[3m])",
                            self.time_format_transform(start_time),
                            self.time_format_transform(current_et),
                            step=str(step)
                        )
                    metric = metric.strip("_total")
                    
                else:
                    data_raw = self.client.custom_query_range(f"{metric}{{namespace='{namespace}'}}", 
                                                            self.time_format_transform(start_time), 
                                                            self.time_format_transform(current_et), 
                                                            step=str(step))
                if len(data_raw) == 0:
                    continue
                timestamp_list = []
                cmdb_id_list = []
                kpi_list = []
                value_list = []
                for data in data_raw:
                    if data['metric']['pod'] not in self.metric_pod_list:
                        continue
                    cmdb_id = data['metric']['instance'] + '.' + data['metric']['pod']
                    if cmdb_id == '':
                        continue
                    kpi_name = metric
                    if metric in network_metrics:
                        kpi_name = self.network_kpi_name_format(data['metric'])
                    for d in data['values']:
                        timestamp_list.append(int(d[0]))
                        cmdb_id_list.append(cmdb_id)
                        kpi_list.append(kpi_name)
                        value_list.append(round(float(d[1]), 3))
                dt = pd.DataFrame({
                    'timestamp': timestamp_list,
                    'cmdb_id': cmdb_id_list,
                    'kpi_name': kpi_list,
                    'value': value_list
                })
                dt = dt.sort_values(by='timestamp')
                file_path = os.path.join(container_save_path, 'kpi_'+metric+'.csv')
                if os.path.exists(file_path):
                    # 文件存在，追加数据
                    with open(file_path, 'a', encoding='utf-8', newline='') as f:
                        dt.to_csv(f, header=False, index=False)
                else:
                    # 文件不存在，写入新文件
                    dt.to_csv(file_path, index=False)
            
            for metric in istio_metrics:
                data_raw = self.client.custom_query_range(f"{metric}{{namespace='{namespace}'}}", 
                                                            self.time_format_transform(start_time), 
                                                            self.time_format_transform(current_et), 
                                                            step=str(step))
                if len(data_raw) == 0:
                    continue
                timestamp_list = []
                cmdb_id_list = []
                kpi_list = []
                value_list = []
                for data in data_raw:
                    cmdb_id = self.istio_cmdb_id_format(data['metric'])
                    pod_name = cmdb_id.split('.')[0]
                    if cmdb_id == '' or pod_name not in self.metric_pod_list:
                        continue
                    kpi_name = self.istio_kpi_name_format(data['metric'])
                    for d in data['values']:
                        timestamp_list.append(int(d[0]))
                        cmdb_id_list.append(cmdb_id)
                        kpi_list.append(kpi_name)
                        value_list.append(round(float(d[1]), 3))
                dt = pd.DataFrame({
                    'timestamp': timestamp_list,
                    'cmdb_id': cmdb_id_list,
                    'kpi_name': kpi_list,
                    'value': value_list
                })
                dt = dt.sort_values(by='timestamp')
                file_path = os.path.join(istio_save_path, 'kpi_'+metric+'.csv')
                if os.path.exists(file_path):
                    # 文件存在，追加数据
                    with open(file_path, 'a', encoding='utf-8', newline='') as f:
                        dt.to_csv(f, header=False, index=False)
                else:
                    # 文件不存在，写入新文件
                    dt.to_csv(file_path, index=False)
            start_time = current_et
            

class MetricMaintainer:
    def __init__(
        self,
        extractor: MetricDataExtractor,
        output_root: str = "./multi_modal_data",
        state_file: str = "metric_state.json",
        max_retries: int = 1,
        step: int = 15
    ):
        self.extractor = extractor
        self.output_root = Path(output_root)
        self.state_file = Path(state_file)
        self.max_retries = max_retries
        self.step = step
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
        print(f"📦 提取指标数据: {start_dt_utc} ~ {end_dt_utc} (UTC)")

        tz_utc8 = ZoneInfo("Asia/Shanghai")
        date_str = start_dt_utc.astimezone(tz_utc8).strftime("%Y-%m-%d")
        output_dir = self.output_root / date_str
        output_dir.mkdir(parents=True, exist_ok=True)

        success = False
        for attempt in range(1, self.max_retries + 1):
            try:
                await asyncio.to_thread(
                    self.extractor.export_all_metrics,
                    start_dt_utc.timestamp(),
                    end_dt_utc.timestamp(),
                    output_dir,
                    self.step
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
    maintainer: MetricMaintainer,
    date_str: str,
    tz: timezone = timezone.utc
):
    """
    提取指定日期的完整指标数据（按小时）
    :param date_str: 'YYYY-MM-DD'
    :param tz: 输入日期的时区（默认 UTC）
    """
    start_dt_local = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=tz)
    start_dt_utc = start_dt_local.astimezone(timezone.utc)

    for h in range(24):
        hour_dt = start_dt_utc + timedelta(hours=h)
        await maintainer.extract_for_hour(hour_dt)


# ========================
# 使用示例
# ========================
async def main():
    extractor = MetricDataExtractor(
        url="http://localhost:9090"
    )

    maintainer = MetricMaintainer(extractor, output_root="./multi_modal_data")

    # 启动定时模式
    # await maintainer.start()

    # 或者：提取某一天的所有指标数据
    await extract_full_day(maintainer, "2025-08-26", tz=timezone(timedelta(hours=8)))  # 输入北京时间


if __name__ == "__main__":
    asyncio.run(main())