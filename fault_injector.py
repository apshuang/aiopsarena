import random
import json
import time
import requests
import logging
import os
from datetime import datetime, timedelta, timezone


# -------------------- 模块1：日志 --------------------
def init_logger():
    logger = logging.getLogger("FaultInjector")
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s")

    # 文件日志处理器
    file_handler = logging.FileHandler("fault_injection.log", mode="a", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    # 控制台日志处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)  # 控制台可选择显示 INFO 以上等级
    console_handler.setFormatter(formatter)

    # 防止重复添加 handler（尤其在多次运行时）
    if not logger.handlers:
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger


logger = init_logger()


# -------------------- 模块2：数据生成器 --------------------
class FaultDataGenerator:
    def __init__(self):
        self.apps = [
            'cartservice', 'checkoutservice', 'currencyservice', 'emailservice',
            'frontend', 'paymentservice', 'productcatalogservice',
            'recommendationservice', 'redis-cart', 'shippingservice'
        ]

    def get_timestamp_name(self):
        now = datetime.now()
        return now.strftime('%Y%m%d-%H%M')
    
    def generate_schedule_time(self):
        # 当前时间为本地时间（UTC+8）
        now_local = datetime.now(timezone(timedelta(hours=8)))

        # 延迟2分钟注入
        schedule_time_utc = now_local + timedelta(minutes=2)

        # 转换为 UTC 时间
        schedule_time_utc = schedule_time_utc.astimezone(timezone.utc)

        # 构建 cron 表达式（UTC）
        minute = schedule_time_utc.minute
        hour = schedule_time_utc.hour
        day = schedule_time_utc.day
        month = schedule_time_utc.month
        return f"{minute} {hour} {day} {month} *"
    

    def get_pods(self, app):
        return [f"{app}-{i}" for i in range(3)]

    def generate_stress_fault(self):
        app = random.choice(self.apps)
        duration = f"{random.randint(2, 7)}m"
        name = self.get_timestamp_name()
        data = {
            "name": name,
            "app": app,
            "duration": duration,
            "schedule": self.generate_schedule_time(),
            "historyLimit": "1000",
            "pods": self.get_pods(app),
            "selected_template": "cpu",
            "inject_type": "experiment",
            "fault_type": "stress",
            "workers": random.randint(2, 6),
            "load": random.randint(80, 100)
        }
        logger.info(f"Generated StressChaos: {json.dumps(data)}")
        return data

    def generate_http_fault(self):
        app = random.choice(self.apps)
        name = self.get_timestamp_name()
        duration = f"{random.randint(2, 7)}m"
        data = {
            "name": name,
            "app": app,
            "duration": duration,
            "schedule": self.generate_schedule_time(),
            "historyLimit": "1000",
            "pods": self.get_pods(app),
            "selected_template": "http-delay",
            "inject_type": "experiment",
            "fault_type": "http",
            "delay": f"{random.randint(200, 800)}ms"
        }
        logger.info(f"Generated HTTPChaos: {json.dumps(data)}")
        return data

    def generate_random_fault(self):
        if random.choice(["stress", "http"]) == "stress":
            return self.generate_stress_fault()
        else:
            return self.generate_http_fault()


# -------------------- 模块3：注入器 --------------------
class FaultInjector:
    def __init__(self, url: str, token: str = "", cookie: str = ""):
        self.url = url
        self.session = requests.Session()
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        if token:
            headers["Authorization"] = f"token {token}"
        if cookie:
            headers["Cookie"] = cookie
        self.session.headers.update(headers)

    def inject(self, data: dict):
        try:
            logger.info(f"Injecting fault: {data['name']}")
            response = self.session.post(self.url, json={'data': data})
            if response.status_code == 200:
                logger.info(f"✅ Injection succeeded: {response.json()}")
            else:
                logger.error(f"❌ Injection failed: {response.status_code} - {response.text}")
        except Exception as e:
            logger.exception(f"🚨 Exception during injection: {e}")


# -------------------- 可选：调度器 --------------------
STATE_FILE = "fault_state.json"

class Scheduler:
    def __init__(self, injector: FaultInjector, generator: FaultDataGenerator, state_file=STATE_FILE):
        self.injector = injector
        self.generator = generator
        self.state_file = state_file
        self.load_state()

    def load_state(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                self.state = json.load(f)
                # state 格式: { "YYYYMMDDHHMM": {"injected": bool, "inject_time": isoformat} }
        else:
            self.state = {}

    def save_state(self):
        with open(self.state_file, 'w') as f:
            json.dump(self.state, f)

    def get_current_window_start(self, now):
        """ 获取当前时间所属的30分钟窗口起点（UTC） """
        minute = 0 if now.minute < 30 else 30
        return now.replace(minute=minute, second=0, microsecond=0)

    def compute_injection_time(self, window_start):
        """ 随机生成注入时间，范围：窗口开始+3min 到 窗口开始+23min """
        valid_start = window_start + timedelta(minutes=3)
        valid_end = window_start + timedelta(minutes=23)
        delta_seconds = int((valid_end - valid_start).total_seconds())
        random_offset = random.randint(0, delta_seconds)
        return valid_start + timedelta(seconds=random_offset)

    def run(self):
        while True:
            try:
                now = datetime.now(timezone.utc)
                window_start = self.get_current_window_start(now)
                window_key = window_start.strftime("%Y%m%d%H%M")
                
                # 取状态
                state_entry = self.state.get(window_key, {})

                # 是否已注入
                injected = state_entry.get("injected", False)
                inject_time_str = state_entry.get("inject_time")
                inject_time = datetime.fromisoformat(inject_time_str) if inject_time_str else None

                if not inject_time:
                    # 没有预设注入时间则生成一个随机时间并保存
                    inject_time = self.compute_injection_time(window_start)
                    self.state[window_key] = {
                        "injected": False,
                        "inject_time": inject_time.isoformat()
                    }
                    self.save_state()
                    logger.info(f"Scheduled injection at {inject_time.isoformat()} for window {window_key}")

                if not injected and now >= inject_time:
                    # 如果此刻已经到达了注入时间，那么就生成故障并注入（因为在生成后2分钟才能注入，所以时间窗口也需改成3min-23min）
                    # 生成并注入故障
                    data = self.generator.generate_random_fault()
                    self.injector.inject(data)

                    # 标记注入完成，保存状态
                    self.state[window_key]["injected"] = True
                    self.save_state()
                    logger.info(f"Injected fault at {now.isoformat()} for window {window_key}")

                time.sleep(15)
            except Exception as e:
                logger.exception(f"Scheduler error: {e}")
                time.sleep(15)


# -------------------- 主程序入口 --------------------
if __name__ == "__main__":
    INJECT_FAULT_URL = "http://localhost:9988/api/chaosmesh/inject"

    TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE3NTQ1Mzc2OTUsImlhdCI6MTc1NDQ1MTI5NSwiZGF0YSI6eyJpZCI6MX19.nd4GeFyLhU4z2Y6sM4XXDBhxasetpxqICCCba9KbWqI"
    COOKIE = "R_PCS=light; R_LOCALE=en-us; R_USERNAME=admin"

    generator = FaultDataGenerator()
    injector = FaultInjector(INJECT_FAULT_URL, TOKEN, COOKIE)

    # 单次注入
    # fault_data = generator.generate_random_fault()
    # injector.inject(fault_data)
    
    scheduler = Scheduler(injector, generator)
    scheduler.run()

    # 如果想循环运行注入器，取消注释：
    # run_scheduler(injector, generator, interval=120)
