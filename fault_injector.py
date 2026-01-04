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

    def generate_cpu_stress_fault(self):
        """生成CPU类型的stress故障"""
        app = random.choice(self.apps)
        duration = f"{random.randint(2, 5)}m"
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
        logger.info(f"Generated CPU StressChaos: {json.dumps(data)}")
        return data

    def generate_memory_stress_fault(self):
        """生成Memory类型的stress故障"""
        app = random.choice(self.apps)
        duration = f"{random.randint(2, 5)}m"
        name = self.get_timestamp_name()
        data = {
            "name": name,
            "app": app,
            "duration": duration,
            "schedule": self.generate_schedule_time(),
            "historyLimit": "1000",
            "pods": self.get_pods(app),
            "selected_template": "memory",
            "inject_type": "experiment",
            "fault_type": "stress",
            "workers": random.randint(2, 6),
            "size": f"{random.randint(80, 100)}%"
        }
        logger.info(f"Generated Memory StressChaos: {json.dumps(data)}")
        return data

    def generate_http_fault(self):
        app = random.choice(self.apps)
        name = self.get_timestamp_name()
        duration = f"{random.randint(2, 5)}m"
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
        # 确保三种故障的概率相等：CPU stress、Memory stress、HTTP
        # 使用1-3的随机数，每个数字对应一种故障类型
        fault_choice = random.randint(1, 3)
        
        if fault_choice == 1:
            return self.generate_cpu_stress_fault()
        elif fault_choice == 2:
            return self.generate_memory_stress_fault()
        else:  # fault_choice == 3
            return self.generate_http_fault()
        
        
# -------------------- 模块3：token维护器 --------------------
class TokenManager:
    def __init__(self, base_url, username, password):
        self.login_url = f"{base_url.rstrip('/')}/loginByUsername"
        self.username = username
        self.password = password
        self.token = None

    def get_token(self):
        if not self.token:
            self._fetch_token()
        return self.token

    def force_refresh(self):
        """强制刷新 Token"""
        self.token = None
        self._fetch_token()

    def _fetch_token(self):
        try:
            resp = requests.post(
                self.login_url,
                headers={"Content-Type": "application/json"},
                data=json.dumps({
                    "username": self.username,
                    "password": self.password
                })
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") == 0:
                self.token = data["data"]["token"]
                logger.info("✅ Token fetched successfully.")
            else:
                raise Exception(f"Login failed: {data.get('message')}")
        except Exception as e:
            logger.error(f"❌ Failed to fetch token: {e}")
            raise


# -------------------- 模块4：注入器 --------------------
class FaultInjector:
    def __init__(self, base_url: str, token_manager: TokenManager, cookie: str = ""):
        self.inject_url = f"{base_url.rstrip('/')}/chaosmesh/inject"
        self.token_manager = token_manager
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json"
        })
        if cookie:
            self.session.headers.update({"Cookie": cookie})

    def inject(self, data: dict, retry_on_auth_error=True):
        try:
            # 每次注入前都使用最新 token
            token = self.token_manager.get_token()
            self.session.headers.update({"Authorization": f"token {token}"})

            logger.info(f"Injecting fault: {data['name']}")
            response = self.session.post(self.inject_url, json={"data": data})

            if response.status_code == 200:
                logger.info(f"✅ Injection succeeded: {response.json()}")
                return True
            else:
                # 检测 token 过期/认证失败
                if retry_on_auth_error and (
                    response.status_code == 401
                    or "Token" in response.text
                    or "authenticate" in response.text
                ):
                    logger.warning("⚠️ Token might be expired. Refreshing and retrying...")
                    self.token_manager.force_refresh()
                    return self.inject(data, retry_on_auth_error=False)
                else:
                    logger.error(f"❌ Injection failed: {response.status_code} - {response.text}")
                    return False

        except Exception as e:
            logger.exception(f"🚨 Exception during injection: {e}")
            return False


# -------------------- 模块5：调度器 --------------------
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
        """ 获取当前时间所属的20分钟窗口起点（UTC） """
        minute = 0 if now.minute < 20 else 20 if now.minute < 40 else 40
        return now.replace(minute=minute, second=0, microsecond=0)

    def compute_injection_time(self, window_start):
        """ 随机生成注入时间，范围：窗口开始+2min 到 窗口开始+14min """
        valid_start = window_start + timedelta(minutes=2)
        valid_end = window_start + timedelta(minutes=14)
        delta_seconds = int((valid_end - valid_start).total_seconds())
        random_offset = random.randint(0, delta_seconds)
        return valid_start + timedelta(seconds=random_offset)

    def run(self):
        while True:
            try:
                now = datetime.now(timezone.utc)
                
                # 在北京时间0点到1点的时候不注入故障，以形成一个normal的数据
                if 16 <= now.hour < 17:
                    logger.info("⏸ 北京时间 0~1 点，跳过故障注入窗口")
                    time.sleep(60)  # 休眠 1 分钟再检查
                    continue
                
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
                    # 如果此刻已经到达了注入时间，那么就生成故障并注入（因为在生成后2分钟才能注入，所以时间窗口也需改成2min-14min）
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
    BASE_API_URL = "http://localhost:9988/api"

    COOKIE = "R_PCS=light; R_LOCALE=en-us; R_USERNAME=admin"
    USERNAME = "strangepro"
    PASSWORD = "password1"
    
    token_manager = TokenManager(BASE_API_URL, USERNAME, PASSWORD)

    generator = FaultDataGenerator()
    injector = FaultInjector(BASE_API_URL, token_manager, COOKIE)

    # 单次注入
    # fault_data = generator.generate_random_fault()
    # injector.inject(fault_data)
    
    # 循环注入
    scheduler = Scheduler(injector, generator)
    scheduler.run()

