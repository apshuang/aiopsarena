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
from collections import defaultdict
import pandas as pd

AGGREGATE_MODE = True

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
        
    def normalize_cmdb_id(self, cmdb_id: str) -> str:
        # 去掉副本号
        base = cmdb_id.rsplit("-", 1)[0]
        # 去掉 minikube. 前缀
        if base.startswith("minikube."):
            base = base.split("minikube.", 1)[1]
        return base
    
    def to_datetime(self, val):
        if isinstance(val, datetime):
            return val
        return datetime.fromisoformat(val)

    def aggregate_metric_anomalies(self, anomalies: list[dict]) -> list[dict]:
        """
        聚合同一 service/pattern/metric 的 metric 异常
        """
        groups = defaultdict(list)

        for anomaly in anomalies:
            cmdb_id = self.normalize_cmdb_id(anomaly["cmdb_id"])
            key = (cmdb_id, anomaly["pattern"], anomaly["metric_name"])
            groups[key].append(anomaly)

        results = []
        for (cmdb_id, pattern, metric_name), group in groups.items():
            start_times = [self.to_datetime(a["start"]) for a in group]
            end_times   = [self.to_datetime(a["end"]) for a in group]
            scores      = [a["anomaly_score"] for a in group]

            aggregated = {
                "cmdb_id": cmdb_id,
                "pattern": "Static-Anomaly-Static",
                "metric_name": metric_name,
                "start": min(start_times).isoformat(),
                "end": max(end_times).isoformat(),
                "anomaly_score": max(scores)
            }
            results.append(aggregated)

        return results

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
                    "pattern": "Static-Anomaly-Static",
                    "metric_name": metric_name,
                    # "metric_data": seg.copy(),
                    "start": seg["timestamp"].iloc[0],
                    "end": seg["timestamp"].iloc[-1],
                    "thresholds": {"up": up_th, "down": dn_th, "mean": mean, "std": float(std)},
                    "anomaly_score": abs((seg["value"].mean() - mean) / std)  # 这一段的z-score平均值
                })
        if AGGREGATE_MODE:
            return self.aggregate_metric_anomalies(results)
        else:
            return results


# =============================
# Spike 峰值检测器
# =============================
class SpikeDetector:
    """
    专门用于检测突发、短暂、极强烈的峰值的检测器
    
    特点：
    1. 检测突发性峰值（spike）
    2. 识别短暂但极强烈的异常
    3. 使用多种算法组合提高检测准确性
    4. 支持配置峰值检测的敏感度和持续时间要求
    """
    
    def __init__(
        self,
        spike_threshold: float = 5.0,           # 峰值检测阈值（相对于基线的倍数）
        min_spike_duration_seconds: int = 10,   # 最小峰值持续时间（秒）
        max_spike_duration_seconds: int = 300,  # 最大峰值持续时间（秒）
        relative_threshold: float = 3.0,        # 相对阈值（相对于周围数据的倍数）
        resample_rule: str | None = "10s",      # 重采样规则
        enable_derivative_detection: bool = True,  # 启用导数检测
        derivative_threshold: float = 2.0,      # 导数检测阈值
        enable_pattern_detection: bool = True,  # 启用模式检测
        min_peak_height: float = 0.1            # 最小峰值高度（相对于基线）
    ):
        self.spike_threshold = spike_threshold
        self.min_spike_duration_seconds = min_spike_duration_seconds
        self.max_spike_duration_seconds = max_spike_duration_seconds
        self.relative_threshold = relative_threshold
        self.resample_rule = resample_rule
        self.enable_derivative_detection = enable_derivative_detection
        self.derivative_threshold = derivative_threshold
        self.enable_pattern_detection = enable_pattern_detection
        self.min_peak_height = min_peak_height
        
    def normalize_cmdb_id(self, cmdb_id: str) -> str:
        # 去掉副本号
        base = cmdb_id.rsplit("-", 1)[0]
        # 去掉 minikube. 前缀
        if base.startswith("minikube."):
            base = base.split("minikube.", 1)[1]
        return base
    
    def to_datetime(self, val):
        if isinstance(val, datetime):
            return val
        return datetime.fromisoformat(val)

    def aggregate_metric_anomalies(self, anomalies: list[dict]) -> list[dict]:
        """
        聚合同一 service/pattern/metric 的 spike 异常
        """
        groups = defaultdict(list)

        for anomaly in anomalies:
            cmdb_id = self.normalize_cmdb_id(anomaly["cmdb_id"])
            key = (cmdb_id, anomaly["pattern"], anomaly["metric_name"])
            groups[key].append(anomaly)

        results = []
        for (cmdb_id, pattern, metric_name), group in groups.items():
            start_times = [self.to_datetime(a["start"]) for a in group]
            end_times   = [self.to_datetime(a["end"]) for a in group]
            scores      = [a["anomaly_score"] for a in group]
            peak_values = [a.get("peak_value", 0) for a in group]

            aggregated = {
                "cmdb_id": cmdb_id,
                "pattern": "Spike",
                "metric_name": metric_name,
                "start": min(start_times).isoformat(),
                "end": max(end_times).isoformat(),
                "anomaly_score": max(scores),
                # "peak_value": max(peak_values) if peak_values else 0
            }
            results.append(aggregated)

        return results

    def _detect_spikes_by_threshold(self, query_df: pd.DataFrame, baseline_mean: float, baseline_std: float) -> pd.DataFrame:
        """基于阈值的峰值检测"""
        # 计算相对于基线的倍数
        query_df["baseline_ratio"] = query_df["value"] / max(baseline_mean, 1e-6)
        
        # 计算相对于周围数据的倍数（滑动窗口）
        window_size = max(3, len(query_df) // 10)
        if len(query_df) >= window_size:
            query_df["local_ratio"] = query_df["value"].rolling(
                window=window_size, center=True, min_periods=1
            ).apply(lambda x: x.iloc[len(x)//2] / x.mean() if x.mean() > 0 else 1.0)
        else:
            query_df["local_ratio"] = 1.0
        
        # 峰值检测条件
        spike_condition = (
            (query_df["baseline_ratio"] >= self.spike_threshold) |
            (query_df["local_ratio"] >= self.relative_threshold)
        )
        
        # 最小峰值高度要求
        if self.min_peak_height > 0:
            height_condition = abs(query_df["value"] - baseline_mean) >= (self.min_peak_height * baseline_mean)
            spike_condition = spike_condition & height_condition
        
        query_df["is_spike"] = spike_condition
        return query_df

    def _detect_spikes_by_derivative(self, query_df: pd.DataFrame) -> pd.DataFrame:
        """基于导数的峰值检测"""
        if not self.enable_derivative_detection or len(query_df) < 3:
            query_df["is_derivative_spike"] = False
            return query_df
        
        # 计算一阶导数（变化率）
        query_df["derivative"] = query_df["value"].diff().abs()
        
        # 计算导数的统计信息
        derivative_mean = query_df["derivative"].mean()
        derivative_std = query_df["derivative"].std()
        
        if derivative_std > 0:
            # 基于z-score的导数检测
            query_df["derivative_zscore"] = (query_df["derivative"] - derivative_mean) / derivative_std
            query_df["is_derivative_spike"] = query_df["derivative_zscore"] > self.derivative_threshold
        else:
            query_df["is_derivative_spike"] = False
        
        return query_df

    def _detect_spikes_by_pattern(self, query_df: pd.DataFrame) -> pd.DataFrame:
        """基于模式的峰值检测"""
        if not self.enable_pattern_detection or len(query_df) < 5:
            query_df["is_pattern_spike"] = False
            return query_df
        
        # 检测尖峰模式：突然上升然后快速下降
        query_df["is_pattern_spike"] = False
        
        for i in range(2, len(query_df) - 2):
            current = query_df.iloc[i]["value"]
            prev_2 = query_df.iloc[i-2]["value"]
            prev_1 = query_df.iloc[i-1]["value"]
            next_1 = query_df.iloc[i+1]["value"]
            next_2 = query_df.iloc[i+2]["value"]
            
            # 尖峰模式：中间值明显高于前后值
            if (current > prev_1 * 1.5 and current > next_1 * 1.5 and
                current > prev_2 * 1.3 and current > next_2 * 1.3):
                query_df.iloc[i, query_df.columns.get_loc("is_pattern_spike")] = True
        
        return query_df

    def _merge_spike_detections(self, query_df: pd.DataFrame) -> pd.DataFrame:
        """合并多种检测结果"""
        # 综合判断是否为峰值
        query_df["is_spike_final"] = (
            query_df["is_spike"] |
            query_df["is_derivative_spike"] |
            query_df["is_pattern_spike"]
        )
        return query_df

    def _extract_spike_segments(self, query_df: pd.DataFrame) -> list[dict]:
        """提取峰值段"""
        if not bool(query_df["is_spike_final"].any()):
            return []
        
        # 找连续峰值段
        grp_id = (query_df["is_spike_final"].ne(query_df["is_spike_final"].shift())).cumsum()
        query_df["grp"] = grp_id
        
        spike_segments = []
        for _, seg in query_df.groupby("grp"):
            if not bool(seg["is_spike_final"].iloc[0]):
                continue
                
            seg = seg.sort_values("timestamp")
            duration_seconds = (seg["timestamp"].iloc[-1] - seg["timestamp"].iloc[0]).total_seconds()
            
            # 检查持续时间要求
            if (duration_seconds >= self.min_spike_duration_seconds and 
                duration_seconds <= self.max_spike_duration_seconds):
                
                # 计算峰值特征
                peak_value = seg["value"].max()
                peak_time = seg.loc[seg["value"].idxmax(), "timestamp"]
                baseline_ratio = peak_value / max(seg["baseline_ratio"].iloc[0], 1e-6)
                
                # 计算异常分数
                anomaly_score = max(
                    seg["baseline_ratio"].max(),
                    seg["local_ratio"].max() if "local_ratio" in seg.columns else 0,
                    seg["derivative_zscore"].max() if "derivative_zscore" in seg.columns else 0
                )
                
                spike_segments.append({
                    "start": seg["timestamp"].iloc[0],
                    "end": seg["timestamp"].iloc[-1],
                    "peak_time": peak_time,
                    "peak_value": peak_value,
                    "baseline_ratio": baseline_ratio,
                    "duration_seconds": duration_seconds,
                    "anomaly_score": anomaly_score,
                    "segment_data": seg
                })
        
        return spike_segments

    def detect(
        self,
        full_df: pd.DataFrame,
        cmdb_id: str,
        metric_name: str,
        baseline_df: pd.DataFrame,
        start_time: datetime,
        end_time: datetime
    ) -> list[dict]:
        """
        检测指定时间范围内的峰值异常
        
        Args:
            full_df: 该指标全量数据
            cmdb_id: 服务ID
            metric_name: 指标名称
            baseline_df: 正常段数据
            start_time: 待检测时间段开始
            end_time: 待检测时间段结束
            
        Returns:
            峰值异常列表
        """
        results: list[dict] = []
        
        if baseline_df.empty:
            return results
        
        # 重采样数据
        base = _resample(baseline_df, self.resample_rule)
        query_df = _resample(
            full_df[(full_df["timestamp"] >= start_time) & (full_df["timestamp"] <= end_time)],
            self.resample_rule
        ).sort_values("timestamp")
        
        if query_df.empty:
            return results
        
        # 计算基线统计信息
        baseline_mean = float(base["value"].mean())
        baseline_std = float(base["value"].std(ddof=0)) if len(base) > 1 else 0.0
        
        # 多种峰值检测方法
        query_df = self._detect_spikes_by_threshold(query_df, baseline_mean, baseline_std)
        query_df = self._detect_spikes_by_derivative(query_df)
        query_df = self._detect_spikes_by_pattern(query_df)
        query_df = self._merge_spike_detections(query_df)
        
        # 提取峰值段
        spike_segments = self._extract_spike_segments(query_df)
        
        # 转换为结果格式
        for segment in spike_segments:
            results.append({
                "cmdb_id": cmdb_id,
                "pattern": "Spike",
                "metric_name": metric_name,
                "start": segment["start"],
                "end": segment["end"],
                "peak_time": segment["peak_time"],
                "peak_value": segment["peak_value"],
                "baseline_ratio": segment["baseline_ratio"],
                "duration_seconds": segment["duration_seconds"],
                "anomaly_score": segment["anomaly_score"],
                "spike_characteristics": {
                    "height": segment["peak_value"] - baseline_mean,
                    "sharpness": segment["anomaly_score"],
                    "duration_category": "short" if segment["duration_seconds"] < 60 else "medium"
                }
            })
        
        if AGGREGATE_MODE:
            return self.aggregate_metric_anomalies(results)
        else:
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
        end_time: datetime,
        baseline_df: pd.DataFrame
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
        end_time: datetime,
        baseline_df: pd.DataFrame
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
                # 使用正则表达式精确匹配错误码，避免误判
                def match_error_codes(text: str) -> list[str]:
                    matched_codes = []
                    text_str = str(text)
                    for code in self.error_codes:
                        # 使用正则表达式匹配，确保错误码是独立的数字
                        # 匹配模式：错误码前后不能是数字，但可以是空格、标点符号等
                        pattern = r'(?<!\d)' + re.escape(code) + r'(?!\d)'
                        if re.search(pattern, text_str):
                            matched_codes.append(code)
                    return matched_codes
                
                df["matched_codes"] = df[text_col].apply(match_error_codes)
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
# 日志量异常检测器
# =========================================
class LogVolumeAnomalyDetector:
    """
    检测日志量异常：基于基线数据，检测某个服务的日志量是否明显减少或增多
    """
    def __init__(
        self,
        volume_threshold: float = 0.5,  # 日志量变化阈值（50%）
        min_baseline_count: int = 10    # 基线数据最小日志数量
    ):
        self.volume_threshold = volume_threshold
        self.min_baseline_count = min_baseline_count

    def detect(
        self,
        log_df: pd.DataFrame,
        start_time: datetime,
        end_time: datetime,
        baseline_df: pd.DataFrame
    ) -> list[dict]:
        results: list[dict] = []
        
        if log_df.empty or baseline_df.empty:
            return results

        # 必须有 cmdb_id
        if "cmdb_id" not in log_df.columns or "cmdb_id" not in baseline_df.columns:
            return results

        # 时间过滤
        current_df = log_df.loc[(log_df["timestamp"] >= start_time) & (log_df["timestamp"] <= end_time)].copy()
        if current_df.empty:
            return results

        # 按 cmdb_id 分组检测
        for cmdb_id, current_group in current_df.groupby("cmdb_id"):
            baseline_group = baseline_df[baseline_df["cmdb_id"] == cmdb_id]
            if baseline_group.empty:
                continue

            # 重采样并计算日志量
            current_volume = self._calculate_log_volume(pd.DataFrame(current_group), start_time, end_time)
            baseline_volume = self._calculate_log_volume(pd.DataFrame(baseline_group), start_time, end_time)

            if baseline_volume < self.min_baseline_count:
                continue

            # 计算变化率
            if baseline_volume > 0:
                change_ratio = (current_volume - baseline_volume) / baseline_volume
                
                # 检测异常
                if abs(change_ratio) >= self.volume_threshold:
                    anomaly_type = "LogVolumeIncrease" if change_ratio > 0 else "LogVolumeDecrease"
                    
                    results.append({
                        "cmdb_id": cmdb_id,
                        "pattern": anomaly_type,
                        "change_ratio": f"{change_ratio:.2%}",
                        "start": start_time,
                        "end": end_time
                    })

        return results

    def _calculate_log_volume(self, df: pd.DataFrame, start_time: datetime, end_time: datetime) -> float:
        """计算指定时间窗口内的日志量"""
        if df.empty:
            return 0.0
        
        # 时间过滤
        time_filtered = df.loc[(df["timestamp"] >= start_time) & (df["timestamp"] <= end_time)]
        if time_filtered.empty:
            return 0.0
        
        # 直接计算总数，不需要重采样
        return float(len(time_filtered))


# =========================================
# 日志数据管理类
# =========================================
class LogAnalyzer:
    def __init__(
        self, 
        log_root: Path,
        baseline_start_tod: time = time(0, 15, 0),  # 00:15
        baseline_duration: timedelta = timedelta(minutes=30)  # 30分钟，到00:45
    ):
        self.log_root = log_root
        self.baseline_start_tod = baseline_start_tod
        self.baseline_duration = baseline_duration
        self.detectors: list[LogAnomalyDetector] = []
        self.loaded_log: dict[str, pd.DataFrame] = {}  # key: hour_key -> df
        self.max_cache_hours = 1  # 最多缓存几个小时的数据，1表示只缓存当前小时

    def _hour_key(self, dt: datetime) -> str:
        return dt.strftime("%Y-%m-%d_%H-00-00")

    def _baseline_df_for(self, df: pd.DataFrame) -> pd.DataFrame:
        """按每日固定时刻窗口抽取数据作为baseline（不跨天）"""
        return _ensure_time_window(df, self.baseline_start_tod, self.baseline_duration)

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

        # 删除缺失值
        df = df.dropna(subset=["timestamp", "cmdb_id", "message"])
        df = df.sort_values("timestamp")

        # 归一化 cmdb_id：去掉 pod 层级，只保留 service
        def normalize_cmdb_id(cmdb_id: str) -> str:
            if not isinstance(cmdb_id, str):
                return cmdb_id
            return cmdb_id.rsplit("-", 1)[0]

        if AGGREGATE_MODE:
            df["cmdb_id"] = df["cmdb_id"].apply(normalize_cmdb_id)

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
                # 计算当前小时的baseline
                baseline_df = self._baseline_df_for(log_df)
                for cmdb_id, g in log_df.groupby("cmdb_id"):
                    for detector in self.detectors:
                        results.extend(detector.detect(g, start_time, end_time, baseline_df))
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



class TraceTopologyChangeDetector:
    def _build_chains(self, df: pd.DataFrame) -> set[tuple]:
        """
        遍历 trace_df 构建调用链集合
        假设 df 至少包含以下列: trace_id, span_id, parent_span_id, service, operation, start_time
        """
        chains = set()

        # 按 trace_id 分组，避免不同 trace 混淆
        for trace_id, group in df.groupby("trace_id"):
            span_map = {row["span_id"]: row for _, row in group.iterrows()}

            for span_id, span_data in span_map.items():
                chain = []
                current_id = span_id

                while current_id in span_map:
                    node = span_map[current_id]
                    chain.append(f"{node['cmdb_id']}:{node['operation_name']}")
                    parent_id = node.get("parent_span")
                    if pd.isna(parent_id):
                        break
                    current_id = parent_id

                # 反转链路，保证从根到叶
                chain = tuple(reversed(chain))
                chains.add(chain)

        return chains

    def detect(
        self,
        trace_df: pd.DataFrame,
        baseline_df: pd.DataFrame,
        start_time: datetime,
        end_time: datetime
    ) -> list[dict]:
        # 基线调用链集合
        baseline_chains = self._build_chains(baseline_df)

        # 过滤 query 时间范围内的数据
        query_df = trace_df[
            (trace_df["timestamp"] >= start_time) &
            (trace_df["timestamp"] <= end_time)
        ]
        query_chains = self._build_chains(query_df)

        anomalies = []

        # 断链: baseline 有但 query 没有
        missing_chains = baseline_chains - query_chains
        for chain in missing_chains:
            anomalies.append({
                "type": "missing_chain",
                "chain": chain
            })

        # 新链: query 有但 baseline 没有
        new_chains = query_chains - baseline_chains
        for chain in new_chains:
            anomalies.append({
                "type": "new_chain",
                "chain": chain
            })

        return anomalies


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


def generate_batch_cases(
    start_time: datetime,
    end_time: datetime,
    metric_analyzer: MetricAnalyzer,
    log_analyzer: LogAnalyzer,
    trace_analyzer: TraceAnalyzer,
    ground_truth_extractor: GroundTruthExtractor,
    data_root_path: Path,
    output_folder: Path,
    time_window_minutes: int = 30
):
    """
    批量生成case，按指定时间窗口划分
    
    Args:
        start_time: 开始时间（带时区）
        end_time: 结束时间（带时区）
        metric_analyzer: 指标分析器
        log_analyzer: 日志分析器
        trace_analyzer: 链路分析器
        ground_truth_extractor: 真实标签提取器
        data_root_path: 数据根路径
        output_folder: 输出文件夹
        time_window_minutes: 时间窗口大小（分钟），默认30分钟
    """
    # 确保时区一致
    if start_time.tzinfo is None:
        start_time = start_time.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    
    # 生成时间窗口列表
    time_windows = []
    current_time = start_time
    while current_time < end_time:
        window_end = min(current_time + timedelta(minutes=time_window_minutes), end_time)
        time_windows.append((current_time, window_end))
        current_time = window_end
    
    print(f"将生成 {len(time_windows)} 个case，时间窗口: {time_window_minutes}分钟")
    
    # 为每个时间窗口生成case
    for i, (window_start, window_end) in enumerate(time_windows):
        print(f"正在处理第 {i+1}/{len(time_windows)} 个时间窗口: {window_start.strftime('%Y-%m-%d %H:%M:%S')} ~ {window_end.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 获取日期字符串用于数据加载
        date = window_start.strftime("%Y-%m-%d")
        
        # 加载对应日期的数据
        metric_analyzer.load_metrics_from_folder(data_root_path / date / "metric" / "container")
        trace_analyzer.load_traces_from_folder(data_root_path / date / "trace")
        
        # 查询异常信息
        metric_anomalies = metric_analyzer.query_anomalies(window_start, window_end)
        log_anomalies = log_analyzer.query_anomalies(window_start, window_end)
        trace_anomalies = trace_analyzer.query_anomalies(window_start, window_end)
        
        anomaly_information = metric_anomalies + log_anomalies + trace_anomalies
        
        # 查询真实标签
        ground_truth = ground_truth_extractor.query_injection(window_start, window_end)
        
        # 生成case
        time_range_str = f"{window_start.isoformat()} ~ {window_end.isoformat()}"
        
        case = {
            "fault_time_window": time_range_str,
            "anomaly_information": [serialize(a) for a in anomaly_information],
            "ground_truth": ground_truth
        }
        
        # 保存case文件
        output_file = output_folder / (f"Bravo_case_{window_start.strftime('%Y%m%d_%H-%M-%S')}.json")
        
        with output_file.open("w", encoding="utf-8") as f:
            json.dump(case, f, ensure_ascii=False, indent=4)
        
        print(f"已生成: {output_file.name}")


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
            inject_type = row.get("kind")
            experiment_dict = json.loads(row["experiment"])
            metadata_dict = json.loads(list(experiment_dict["metadata"]["annotations"].values())[0])
            spec_dict = dict(metadata_dict["spec"])
            
            if inject_type == "StressChaos":
                inject_sub_type = list(spec_dict["stressors"].keys())[0]
            elif inject_type == "HTTPChaos":
                inject_sub_type = list(spec_dict.keys())[0]
            else:
                raise ValueError(f"未知注入类型: {inject_type}")
            
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
                "inject_type": f'{inject_type}-{inject_sub_type}',
                "inject_component": spec_dict["selector"]["labelSelectors"]["app"]
            })

        if len(results) > 1:
            raise ValueError("查询结果超过 1 条，时间区间不应重叠！")
        return results[0] if results else None

    def close_connection(self):
        self.conn.close()


if __name__ == "__main__":
    data_root_path = Path("./multi_modal_data")
    output_folder = Path("./case")
    
    metric_analyzer = MetricAnalyzer()
    
    metric_analyzer.register_detector(SASDetector(n_sigma=3, min_duration_minutes=5))
    metric_analyzer.register_detector(SpikeDetector(spike_threshold=10, min_spike_duration_seconds=30, max_spike_duration_seconds=60, relative_threshold=8, resample_rule="15s"))
    
    log_analyzer = LogAnalyzer(data_root_path)
    log_analyzer.register_detector(
        KeywordAndCodeDetector(
            include_keywords=["error", "fail", "warn", "exception", "panic", "fatal"],
            exclude_keywords=["test", "ignore", "http://10.99.94.30:8200", "elastic-apm-node", "transaction_id"],
            error_codes=["400", "401", "403", "404", "500", "501", "502", "503", "504"],
            min_count=3
        )
    )
    
    # 注册日志量异常检测器
    log_analyzer.register_detector(
        LogVolumeAnomalyDetector(
            volume_threshold=0.066,  # 日志量变化6.66%以上视为异常
            min_baseline_count=100   # 基线数据至少需要100条日志
        )
    )
    
    trace_analyzer = TraceAnalyzer(data_root_path)
    trace_analyzer.register_detector(TraceLatencyDetector())
    trace_analyzer.register_detector(TraceErrorRateDetector())
    trace_analyzer.register_detector(TraceTopologyChangeDetector())
    
    ground_truth_extractor = GroundTruthExtractor(
        host="172.17.0.2",
        port=3306,
        user="root",
        password="elastic",
        database="chaos_mesh"
    )
    
    # 设置时间范围（每半小时一个故障注入）
    tz = "Asia/Shanghai"
    start = pd.Timestamp(datetime(2025, 8, 19, 1, 0, 0), tz=tz)
    end   = pd.Timestamp(datetime(2025, 8, 19, 23, 59, 59), tz=tz)
    
    # 使用批量化处理函数生成多个case
    generate_batch_cases(
        start_time=start.to_pydatetime(),
        end_time=end.to_pydatetime(),
        metric_analyzer=metric_analyzer,
        log_analyzer=log_analyzer,
        trace_analyzer=trace_analyzer,
        ground_truth_extractor=ground_truth_extractor,
        data_root_path=data_root_path,
        output_folder=output_folder,
        time_window_minutes=30  # 30分钟时间窗口
    )
  
    ground_truth_extractor.close_connection()
