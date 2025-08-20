from __future__ import annotations

import os
import re
import json
import pymysql
from pymysql.cursors import DictCursor
from datetime import datetime, timedelta, time, timezone
from typing import Protocol
from zoneinfo import ZoneInfo
from pathlib import Path
import pandas as pd


# =============================
# 异常检测器接口（传时间范围，不直接给 query_df）
# =============================
class MetricAnomalyDetector(Protocol):
    def detect(
        self,
        full_df: pd.DataFrame,      # 该指标全量数据
        cmdb_id: str,
        metric_name: str,
        baseline_df: pd.DataFrame,  # 正常段数据
        start_time: datetime,       # 待检测时间段开始
        end_time: datetime          # 待检测时间段结束
    ) -> list[dict]:
        ...


# =============================
# 工具函数
# =============================
def _ensure_time_window(df: pd.DataFrame, start: time, duration: timedelta) -> pd.DataFrame:
    """按每日固定时刻窗口抽取数据（不跨天）"""
    if df.empty:
        return df

    start_sec = start.hour * 3600 + start.minute * 60 + start.second
    end_sec = start_sec + int(duration.total_seconds())

    t_sec = (
        df["timestamp"].dt.hour * 3600 +
        df["timestamp"].dt.minute * 60 +
        df["timestamp"].dt.second
    )
    mask = (t_sec >= start_sec) & (t_sec < end_sec)
    return df.loc[mask].copy()


def _resample(df: pd.DataFrame, rule: str | None) -> pd.DataFrame:
    """可选重采样"""
    if df.empty or not rule:
        return df.copy()
    tmp = df.set_index("timestamp").sort_index()
    res_val = tmp[["value"]].resample(rule).mean().interpolate("time")
    for col in ("cmdb_id", "kpi_name"):
        if col in tmp.columns:
            res_val[col] = tmp[col].iloc[0]
    return res_val.reset_index()


# =============================
# SAS 检测器
# =============================
class SASDetector:
    def __init__(
        self,
        n_sigma: float = 3.0,
        min_duration_minutes: int = 5,
        direction: str = "double",
        resample_rule: str | None = "1min"
    ):
        self.n_sigma = n_sigma
        self.min_duration_minutes = min_duration_minutes
        self.direction = direction
        self.resample_rule = resample_rule

    def detect(
        self,
        full_df: pd.DataFrame,
        cmdb_id: str,
        metric_name: str,
        baseline_df: pd.DataFrame,
        start_time: datetime,
        end_time: datetime
    ) -> list[dict]:
        results: list[dict] = []
        if baseline_df.empty:
            return results
        
        base = _resample(baseline_df, self.resample_rule)
        query_df = _resample(
            full_df[(full_df["timestamp"] >= start_time) & (full_df["timestamp"] <= end_time)],
            self.resample_rule
        ).sort_values("timestamp")
        
        if query_df.empty:
            return results
        
        mean = base["value"].mean()
        std = base["value"].std(ddof=0) if len(base) > 1 else 0.0

        std = max(std, 1e-3)

        up_th = mean + self.n_sigma * std
        dn_th = mean - self.n_sigma * std

        if self.direction == "greater":
            query_df["is_anomaly"] = query_df["value"] > up_th
        elif self.direction == "less":
            query_df["is_anomaly"] = query_df["value"] < dn_th
        else:
            query_df["is_anomaly"] = (query_df["value"] > up_th) | (query_df["value"] < dn_th)

        if not query_df["is_anomaly"].any():
            return results

        # 找连续异常段
        grp_id = (query_df["is_anomaly"].ne(query_df["is_anomaly"].shift())).cumsum()
        query_df["grp"] = grp_id

        for _, seg in query_df.groupby("grp"):
            if not seg["is_anomaly"].iloc[0]:
                continue
            seg = seg.sort_values("timestamp")
            duration_min = (seg["timestamp"].iloc[-1] - seg["timestamp"].iloc[0]).total_seconds() / 60.0
            if duration_min >= self.min_duration_minutes:
                results.append({
                    "cmdb_id": cmdb_id,
                    "pattern": "SAS",
                    "metric_name": metric_name,
                    # "metric_data": seg.copy(),
                    "start": seg["timestamp"].iloc[0],
                    "end": seg["timestamp"].iloc[-1],
                    "thresholds": {"up": up_th, "down": dn_th, "mean": mean, "std": float(std)},
                    "anomaly_score": abs((seg["value"].mean() - mean) / std)  # 这一段的z-score平均值
                })

        return results


# =============================
# 滑动窗口 Z-score 检测器
# =============================
class SlidingZScoreDetector:
    def __init__(self, window_size: int = 30, z_threshold: float = 3.0, resample_rule: str | None = "1min"):
        self.window_size = window_size
        self.z_threshold = z_threshold
        self.resample_rule = resample_rule

    def detect(
        self,
        full_df: pd.DataFrame,
        cmdb_id: str,
        metric_name: str,
        baseline_df: pd.DataFrame,  # 这里不直接用 baseline，但保留接口兼容性
        start_time: datetime,
        end_time: datetime
    ) -> list[dict]:
        query_df = _resample(
            full_df[(full_df["timestamp"] >= start_time) & (full_df["timestamp"] <= end_time)],
            self.resample_rule
        ).sort_values("timestamp")

        results: list[dict] = []
        if query_df.empty or len(query_df) <= self.window_size:
            return results

        query_df["is_anomaly"] = False
        for i in range(self.window_size, len(query_df)):
            window = query_df.iloc[i - self.window_size : i]["value"]
            mean = window.mean()
            std = window.std(ddof=0) if len(window) > 1 else 1e-9
            std = max(std, 1e-3)
            z = (query_df.iloc[i]["value"] - mean) / std
            if abs(z) > self.z_threshold:
                query_df.at[query_df.index[i], "is_anomaly"] = True

        if not query_df["is_anomaly"].any():
            return results

        grp_id = (query_df["is_anomaly"].ne(query_df["is_anomaly"].shift())).cumsum()
        query_df["grp"] = grp_id

        for _, seg in query_df.groupby("grp"):
            if not seg["is_anomaly"].iloc[0]:
                continue
            results.append({
                "cmdb_id": cmdb_id,
                "pattern": "ZScore",
                "metric_name": metric_name,
                "metric_data": seg.copy(),
                "start": seg["timestamp"].iloc[0],
                "end": seg["timestamp"].iloc[-1],
                "z_threshold": self.z_threshold,
                "window_size": self.window_size
            })

        return results


# =============================
# Metric 数据管理类
# =============================
class MetricAnalyzer:
    def __init__(
        self,
        baseline_start_tod: time = time(0, 5, 0),
        baseline_duration: timedelta = timedelta(minutes=55)
    ):
        self.metrics: dict[str, pd.DataFrame] = {}
        self.detectors: list[MetricAnomalyDetector] = []
        self.baseline_start_tod = baseline_start_tod
        self.baseline_duration = baseline_duration

    def load_metrics_from_folder(self, folder_path: Path) -> None:
        files = [folder_path / f for f in os.listdir(folder_path) if f.endswith(".csv")]
        for file in files:
            df = pd.read_csv(file)
            df = df.rename(columns={c: c.lower() for c in df.columns})
            if pd.api.types.is_integer_dtype(df["timestamp"]):
                max_ts = df["timestamp"].max()
                if max_ts > 1e12:
                    # 毫秒级时间戳
                    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
                else:
                    # 秒级时间戳
                    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
            else:
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True)

            # 转到北京时间
            df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Shanghai")
            df = df[["timestamp", "cmdb_id", "kpi_name", "value"]].dropna(subset=["timestamp", "cmdb_id", "kpi_name", "value"])
            
            df = df.sort_values("timestamp")
            for (cmdb_id, kpi), g in df.groupby(["cmdb_id", "kpi_name"]):
                key = f"{cmdb_id}::{kpi}"
                self.metrics.setdefault(key, pd.DataFrame())
                self.metrics[key] = pd.concat([self.metrics[key], g], ignore_index=True)
                
        for key in self.metrics:
            self.metrics[key] = self.metrics[key].sort_values("timestamp").reset_index(drop=True)

    def _baseline_df_for(self, df: pd.DataFrame) -> pd.DataFrame:
        return _ensure_time_window(df, self.baseline_start_tod, self.baseline_duration)

    def register_detector(self, detector: MetricAnomalyDetector) -> None:
        self.detectors.append(detector)

    def query_anomalies(self, start_time: datetime, end_time: datetime) -> list[dict]:
        results: list[dict] = []
        for key, full_df in self.metrics.items():
            cmdb_id, metric_name = key.split("::", 1)
            baseline_df = self._baseline_df_for(full_df)
            for detector in self.detectors:
                results.extend(detector.detect(full_df, cmdb_id, metric_name, baseline_df, start_time, end_time))
        return results
    
    
# =========================================
# 日志异常检测器接口
# =========================================
class LogAnomalyDetector(Protocol):
    def detect(
        self,
        log_df: pd.DataFrame,
        start_time: datetime,
        end_time: datetime
    ) -> list[dict]:
        ...

# =========================================
# 日志关键词/错误码检测器
# =========================================
class KeywordAndCodeDetector:
    """
    在 [start_time, end_time] 区间内，统计同一 cmdb_id + (关键词/错误码) 的出现次数。
    只要达到 min_count 即返回一个聚合事件，不做连续性判定。
    """
    def __init__(
        self,
        include_keywords: list[str] = [],
        exclude_keywords: list[str] = [],
        error_codes: list[str] = [],
        min_count: int = 3,
        text_columns: tuple[str, ...] = ("log_message", "message", "msg"),
    ):
        self.include_keywords = [kw.lower() for kw in include_keywords]
        self.exclude_keywords = [kw.lower() for kw in exclude_keywords]
        self.error_codes = [str(c) for c in error_codes]
        self.min_count = min_count
        self.text_columns = text_columns

    def _pick_text_column(self, df: pd.DataFrame) -> str:
        for col in self.text_columns:
            if col in df.columns:
                return col
        raise ValueError(f"未找到日志文本列，请提供其中之一：{self.text_columns}")

    def detect(
        self,
        log_df: pd.DataFrame,
        start_time: datetime,
        end_time: datetime
    ) -> list[dict]:
        results: list[dict] = []
        if log_df.empty:
            return results

        # 时间过滤
        df = log_df.loc[(log_df["timestamp"] >= start_time) & (log_df["timestamp"] <= end_time)].copy()
        if df.empty:
            return results

        # 必须有 cmdb_id
        if "cmdb_id" not in df.columns:
            raise ValueError("日志DataFrame缺少 'cmdb_id' 列。")
        # 文本列
        text_col = self._pick_text_column(df)
        df[text_col] = df[text_col].astype(str)
        df["text_lower"] = df[text_col].str.lower()

        # 先做 exclude 过滤
        if self.exclude_keywords:
            def has_exclude(s: str) -> bool:
                return any(ex in s for ex in self.exclude_keywords)
            df = df[~df["text_lower"].apply(has_exclude)].copy()
            if df.empty:
                return results

        # 匹配 include 关键词（每条日志可能命中多个）
        def match_includes(s: str) -> list[str]:
            return [kw for kw in self.include_keywords if kw in s]

        df["matched_keywords"] = df["text_lower"].apply(match_includes)

        # 匹配错误码：优先用 error_code 列；否则从文本里找子串
        if "error_code" in df.columns:
            # 统一为字符串
            ec = df["error_code"].astype(str)
            if self.error_codes:
                df["matched_codes"] = ec.apply(
                    lambda v: [v] if v in self.error_codes else []
                )
            else:
                # 未指定 codes，则不使用列值作为匹配来源
                df["matched_codes"] = [[] for _ in range(len(df))]
        else:
            if self.error_codes:
                df["matched_codes"] = df[text_col].apply(
                    lambda s: [c for c in self.error_codes if c in str(s)]
                )
            else:
                df["matched_codes"] = [[] for _ in range(len(df))]

        # 只保留至少匹配了关键词或错误码的日志
        df["matched_any"] = df.apply(
            lambda r: bool(r["matched_keywords"]) or bool(r["matched_codes"]),
            axis=1
        )
        df = df[df["matched_any"]].copy()
        if df.empty:
            return results

        # “爆炸”成逐项匹配行，便于统计
        exploded_rows = []
        for _, row in df.iterrows():
            base = {
                "timestamp": row["timestamp"],
                "cmdb_id": row["cmdb_id"],
                "text": row[text_col],
            }
            for kw in row["matched_keywords"]:
                exploded_rows.append({**base, "match_type": "keyword", "match_value": kw})
            for code in row["matched_codes"]:
                exploded_rows.append({**base, "match_type": "error_code", "match_value": code})

        if not exploded_rows:
            return results

        ex_df = pd.DataFrame(exploded_rows)

        # 统计：cmdb_id + match_type + match_value
        agg = (
            ex_df
            .groupby(["cmdb_id", "match_type", "match_value"], as_index=False)
            .agg(
                count=("timestamp", "size"),
                start=("timestamp", "min"),
                end=("timestamp", "max"),
            )
        )

        # 阈值过滤
        agg = agg[agg["count"] >= self.min_count]
        if agg.empty:
            return results

        # 生成结果，增加 message 摘要（top N）
        TOP_N = 3
        for _, row in agg.iterrows():
            # 筛选属于这一类的行
            sub_df = ex_df[
                (ex_df["cmdb_id"] == row["cmdb_id"]) &
                (ex_df["match_type"] == row["match_type"]) &
                (ex_df["match_value"] == row["match_value"])
            ]

            # 统计 message 频次
            msg_counts = (
                sub_df.groupby("text")
                .size()
                .reset_index(name="count")
                .sort_values("count", ascending=False)
                .head(TOP_N)
            )

            # 转成 {message: count} 的 dict
            top_messages = dict(zip(msg_counts["text"], msg_counts["count"]))

            results.append({
                "cmdb_id": row["cmdb_id"],
                "pattern": "AnomalyKeyword",
                "match_type": row["match_type"],     # "keyword" / "error_code"
                "match_value": row["match_value"],   # 具体关键词/错误码
                "event_count": int(row["count"]),
                "start": row["start"],
                "end": row["end"],
                "top_messages": top_messages         # 新增字段
            })
        return results


# =========================================
# 日志数据管理类
# =========================================
class LogAnalyzer:
    def __init__(self, log_root: Path):
        self.log_root = log_root
        self.detectors: list[LogAnomalyDetector] = []
        self.loaded_log: dict[str, pd.DataFrame] = {}  # key: hour_key -> df
        self.max_cache_hours = 1  # 最多缓存几个小时的数据，1表示只缓存当前小时

    def _hour_key(self, dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d_%H-00-00")

    def _load_log_for_time(self, target_time: datetime) -> pd.DataFrame:
        date_str = target_time.strftime("%Y-%m-%d")
        hour_str = target_time.strftime("%H-00-00")  # 假设文件按整点存
        log_dir = self.log_root / date_str / "log"
        file_path = log_dir / f"log_{date_str}_{hour_str}.csv"

        cache_key = f"{date_str}_{hour_str}"

        # 清理旧缓存，保持缓存量 <= max_cache_hours
        if cache_key not in self.loaded_log:
            if len(self.loaded_log) >= self.max_cache_hours:
                self.loaded_log.clear()  # 清掉旧的
        else:
            return self.loaded_log[cache_key]

        if not file_path.exists():
            return pd.DataFrame()

        df = pd.read_csv(file_path)

        # 处理 timestamp 列
        if pd.api.types.is_integer_dtype(df["timestamp"]):
            max_ts = df["timestamp"].max()
            if max_ts > 1e12:
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
            else:
                df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
        elif pd.api.types.is_float_dtype(df["timestamp"]):
            # 假设浮点数是秒
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
        else:
            # 字符串
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            
        # 转到北京时间
        df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")      # 告诉 pandas 这是 UTC
        df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Shanghai")  # 转到北京时间
        df = df.dropna(subset=["timestamp", "cmdb_id", "message"])
        df = df.sort_values("timestamp")
        self.loaded_log[cache_key] = df
        return df

    def register_detector(self, detector: LogAnomalyDetector) -> None:
        self.detectors.append(detector)

    def query_anomalies(self, start_time: datetime, end_time: datetime) -> list[dict]:
        results: list[dict] = []

        # 遍历涉及到的小时
        cur_time = start_time.replace(minute=0, second=0, microsecond=0)
        while cur_time <= end_time:
            log_df = self._load_log_for_time(cur_time)
            if not log_df.empty:
                for cmdb_id, g in log_df.groupby("cmdb_id"):
                    for detector in self.detectors:
                        results.extend(detector.detect(g, start_time, end_time))
            cur_time += pd.Timedelta(hours=1)

        return results


# =========================================
# Trace 异常检测器接口
# =========================================
class TraceAnomalyDetector(Protocol):
    def detect(
        self,
        trace_df: pd.DataFrame,
        baseline_df: pd.DataFrame,
        start_time: datetime,
        end_time: datetime
    ) -> list[dict]:
        ...

# =========================================
# 工具函数：基线提取
# =========================================
def _trace_baseline_df(df: pd.DataFrame, start: time, duration: timedelta) -> pd.DataFrame:
    if df.empty:
        return df
    start_sec = start.hour * 3600 + start.minute * 60 + start.second
    end_sec = start_sec + int(duration.total_seconds())
    t_sec = (
        df["timestamp"].dt.hour * 3600 +
        df["timestamp"].dt.minute * 60 +
        df["timestamp"].dt.second
    )
    mask = (t_sec >= start_sec) & (t_sec < end_sec)
    return df.loc[mask].copy()

# =========================================
# 高延迟检测器
# =========================================
class TraceLatencyDetector:
    def __init__(self, n_sigma: float = 3.0, min_count: int = 5):
        self.n_sigma = n_sigma
        self.min_count = min_count

    def detect(
        self,
        trace_df: pd.DataFrame,
        baseline_df: pd.DataFrame,
        start_time: datetime,
        end_time: datetime
    ) -> list[dict]:
        results: list[dict] = []
        if baseline_df.empty or trace_df.empty:
            return results

        # 遍历 (cmdb_id, operation_name) 分组
        grouped_baseline = baseline_df.groupby(["cmdb_id", "operation_name"])
        for (cmdb_id, op), base in grouped_baseline:
            mean = base["duration"].mean()
            std = base["duration"].std(ddof=0) if len(base) > 1 else 1e-9
            std = max(std, 1e-3)
            up_th = mean + self.n_sigma * std

            # 在检测区间内筛选对应的 traces
            q = trace_df[
                (trace_df["timestamp"] >= start_time) &
                (trace_df["timestamp"] <= end_time) &
                (trace_df["cmdb_id"] == cmdb_id) &
                (trace_df["operation_name"] == op)
            ]

            if q.empty:
                continue
            abn = q[q["duration"] > up_th]
            if len(abn) >= self.min_count:
                results.append({
                    "pattern": "TraceLatency",
                    "cmdb_id": cmdb_id,
                    "operation_name": op,
                    "start": abn["timestamp"].min(),
                    "end": abn["timestamp"].max(),
                    # "threshold": up_th,
                    "count": len(abn)
                })

        return results

# =========================================
# 错误率检测器
# =========================================
class TraceErrorRateDetector:
    def __init__(self, ratio_threshold: float = 0.2):
        self.ratio_threshold = ratio_threshold

    def detect(
        self,
        trace_df: pd.DataFrame,
        baseline_df: pd.DataFrame,
        start_time: datetime,
        end_time: datetime
    ) -> list[dict]:
        results: list[dict] = []
        if trace_df.empty:
            return results

        # 基线错误率
        if not baseline_df.empty:
            baseline_err_ratio = (baseline_df["status_code"] >= 400).mean()
        else:
            baseline_err_ratio = 0.0

        q = trace_df[(trace_df["timestamp"] >= start_time) & (trace_df["timestamp"] <= end_time)]
        if q.empty:
            return results

        err_ratio = (q["status_code"] >= 400).mean()
        if err_ratio - baseline_err_ratio >= self.ratio_threshold:
            results.append({
                "pattern": "TraceErrorRate",
                "cmdb_ids": q["cmdb_id"].unique().tolist(),
                "start": q["timestamp"].min(),
                "end": q["timestamp"].max(),
                "baseline_err_ratio": baseline_err_ratio,
                "cur_err_ratio": err_ratio
            })
        return results

# =========================================
# 拓扑模式检测器（基础版）
# =========================================
class TraceTopologyChangeDetector:
    def detect(
        self,
        trace_df: pd.DataFrame,
        baseline_df: pd.DataFrame,
        start_time: datetime,
        end_time: datetime
    ) -> list[dict]:
        results: list[dict] = []
        if trace_df.empty:
            return results

        base_edges = set()
        for _, r in baseline_df.iterrows():
            if pd.notna(r["parent_span"]):
                base_edges.add((r["parent_span"], r["operation_name"]))

        q = trace_df[(trace_df["timestamp"] >= start_time) & (trace_df["timestamp"] <= end_time)]
        if q.empty:
            return results

        new_edges = set()
        for _, r in q.iterrows():
            if pd.notna(r["parent_span"]):
                edge = (r["parent_span"], r["operation_name"])
                if edge not in base_edges:
                    new_edges.add(edge)

        if new_edges:
            results.append({
                "pattern": "TraceTopologyNewEdge",
                "edges": list(new_edges),
                "start": q["timestamp"].min(),
                "end": q["timestamp"].max()
            })
        return results

# =========================================
# Trace Analyzer
# =========================================
class TraceAnalyzer:
    def __init__(
        self,
        trace_root: Path,
        baseline_start_tod: time = time(0, 5, 0),
        baseline_duration: timedelta = timedelta(minutes=55)
    ):
        self.trace_root = trace_root
        self.detectors: list[TraceAnomalyDetector] = []
        self.traces: pd.DataFrame | None = None
        self.baseline_start_tod = baseline_start_tod
        self.baseline_duration = baseline_duration

    def load_traces_from_folder(self, folder_path: Path) -> None:
        files = [folder_path / f for f in os.listdir(folder_path) if f.endswith(".csv")]
        all_df = []
        for f in files:
            df = pd.read_csv(f)
            df = df.rename(columns={c: c.lower() for c in df.columns})
            # 处理 timestamp
            if pd.api.types.is_integer_dtype(df["timestamp"]):
                max_ts = df["timestamp"].max()
                if max_ts > 1e15:  # 微秒
                    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="us")
                elif max_ts > 1e12:  # 毫秒
                    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
                else:  # 秒
                    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="s")
            else:
                df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
                
            # 转到北京时间
            df["timestamp"] = df["timestamp"].dt.tz_localize("UTC")      # 告诉 pandas 这是 UTC
            df["timestamp"] = df["timestamp"].dt.tz_convert("Asia/Shanghai")

            all_df.append(df)
        if all_df:
            self.traces = pd.concat(all_df, ignore_index=True).dropna(subset=["timestamp", "cmdb_id", "trace_id", "span_id"])
            self.traces = self.traces.sort_values("timestamp").reset_index(drop=True)

    def register_detector(self, detector: TraceAnomalyDetector) -> None:
        self.detectors.append(detector)

    def query_anomalies(self, start_time: datetime, end_time: datetime) -> list[dict]:
        results: list[dict] = []
        if self.traces is None or self.traces.empty:
            return results

        baseline_df = _trace_baseline_df(self.traces, self.baseline_start_tod, self.baseline_duration)
        for detector in self.detectors:
            results.extend(detector.detect(self.traces, baseline_df, start_time, end_time))
        return results

def serialize(anomaly):
    a = anomaly.copy()
    # 把 Timestamp/datetime 转成 ISO 字符串
    for key in ["start", "end"]:
        if key in a and hasattr(a[key], "isoformat"):
            a[key] = a[key].isoformat()
    return a


class GroundTruthExtractor:
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
            charset="utf8mb4",
            cursorclass=DictCursor
        )
        
    def parse_duration_str(self, duration_str: str) -> int:
        """
        将类似 '2d3h4m5s' 的 duration 字符串解析为总秒数
        支持 d（天）、h（小时）、m（分钟）、s（秒）组合
        """
        pattern = r'(\d+)([dhms])'
        matches = re.findall(pattern, duration_str.lower())
        if not matches:
            raise ValueError(f"无法解析 duration: {duration_str}")

        total_seconds = 0
        for value, unit in matches:
            value = int(value)
            if unit == 'd':
                total_seconds += value * 86400
            elif unit == 'h':
                total_seconds += value * 3600
            elif unit == 'm':
                total_seconds += value * 60
            elif unit == 's':
                total_seconds += value
            else:
                raise ValueError(f"未知时间单位: {unit}")
        return total_seconds

    def query_injection(self, start_time, end_time):
        """
        start_time / end_time 为 UTC datetime
        返回的 start_time / finish_time 为 UTC+8 datetime
        """
        tz_offset = 8  # UTC+8

        sql = """
        SELECT *
        FROM experiments
        WHERE start_time BETWEEN %s AND %s
        """
        start_utc = start_time.astimezone(ZoneInfo("UTC"))
        end_utc   = end_time.astimezone(ZoneInfo("UTC"))
        
        with self.conn.cursor() as cursor:
            cursor.execute(sql, (start_utc, end_utc))
            rows = cursor.fetchall()

        results = []
        for row in rows:
            experiment_dict = json.loads(row["experiment"])
            metadata_dict = json.loads(list(experiment_dict["metadata"]["annotations"].values())[0])
            spec_dict = dict(metadata_dict["spec"])
            
            stressor = list(spec_dict["stressors"].keys())[0]
            duration_str = spec_dict["duration"]
            duration_sec = self.parse_duration_str(duration_str)
            
            start_utc = row["start_time"]
            finish_utc = start_utc + timedelta(seconds=duration_sec)

            tz = timezone(timedelta(hours=8))
            inject_time = (start_utc + timedelta(hours=tz_offset)).replace(tzinfo=tz).isoformat()
            recover_time = (finish_utc + timedelta(hours=tz_offset)).replace(tzinfo=tz).isoformat()
            
            results.append({
                "inject_time": inject_time,
                "recover_time": recover_time,
                "inject_type": f'{row.get("kind")}-{stressor}',
                "inject_component": spec_dict["selector"]["labelSelectors"]["app"]
            })

        if len(results) > 1:
            raise ValueError("查询结果超过 1 条，时间区间不应重叠！")
        return results[0] if results else None

    def close_connection(self):
        self.conn.close()


if __name__ == "__main__":
    data_root_path = Path("./multi_modal_data")
    
    metric_analyzer = MetricAnalyzer()
    
    metric_analyzer.register_detector(SASDetector(n_sigma=3, min_duration_minutes=5))
    metric_analyzer.register_detector(SlidingZScoreDetector(window_size=30, z_threshold=3.0))
    
    log_analyzer = LogAnalyzer(data_root_path)
    log_analyzer.register_detector(
        KeywordAndCodeDetector(
            include_keywords=["error", "fail"],
            exclude_keywords=["test", "ignore", "http://10.99.94.30:8200"],
            error_codes=["400", "500"],
            min_count=3
        )
    )
    
    trace_analyzer = TraceAnalyzer(data_root_path)
    trace_analyzer.register_detector(TraceLatencyDetector())
    trace_analyzer.register_detector(TraceErrorRateDetector())
    # trace_analyzer.register_detector(TraceTopologyChangeDetector())
    
    ground_truth_extractor = GroundTruthExtractor(
        host="172.17.0.2",
        port=3306,
        user="root",
        password="elastic",
        database="chaos_mesh"
    )
    
    tz = "Asia/Shanghai"
    start = pd.Timestamp(datetime(2025, 8, 19, 1, 0, 0), tz=tz)
    end   = pd.Timestamp(datetime(2025, 8, 19, 1, 29, 59), tz=tz)
    
    date = start.strftime("%Y-%m-%d")
    
    metric_analyzer.load_metrics_from_folder(data_root_path / date / "metric" / "container")
    trace_analyzer.load_traces_from_folder(data_root_path / date / "trace")
    metric_anomalies = metric_analyzer.query_anomalies(start, end)
    log_anomalies = log_analyzer.query_anomalies(start, end)
    trace_anomalies = trace_analyzer.query_anomalies(start, end)
    
    anomaly_information = metric_anomalies + log_anomalies + trace_anomalies

    ground_truth = ground_truth_extractor.query_injection(start, end)
    
    time_range_str = f"{start.isoformat()} ~ {end.isoformat()}"
    
    case = {
        "fault_time_window": time_range_str,
        "anomaly_information": [serialize(a) for a in anomaly_information],
        "ground_truth": ground_truth
    }
    
    output_file = Path(f"case_{start.strftime('%Y%m%d_%H-%M-%S')}.json")
    with output_file.open("w", encoding="utf-8") as f:
        json.dump(case, f, ensure_ascii=False, indent=4)

    # print(metric_anomalies)
    # for a in metric_anomalies:
        # print(a)
        # print(a["cmdb_id"], a["pattern"], a["metric_name"], a["start"], "->", a["end"], a["thresholds"])
        
    ground_truth_extractor.close_connection()
