#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spike检测分析脚本
分析adservice的container_cpu_system_seconds为什么符合spike标准
"""

import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from typing import Dict, List, Tuple
import warnings
warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

class SpikeAnalysisDetector:
    """
    Spike检测分析器 - 基于multi_modal_data_analyzer.py中的SpikeDetector逻辑
    """
    
    def __init__(
        self,
        spike_threshold: float = 5.0,           # 峰值检测阈值（相对于基线的倍数）
        min_spike_duration_seconds: int = 10,   # 最小峰值持续时间（秒）
        max_spike_duration_seconds: int = 300,  # 最大峰值持续时间（秒）
        relative_threshold: float = 3.0,        # 相对阈值（相对于周围数据的倍数）
        resample_rule: str = "10s",             # 重采样规则
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
        
    def to_datetime(self, val):
        """转换时间格式"""
        if isinstance(val, datetime):
            return val
        return datetime.fromisoformat(val)

    def _resample(self, df: pd.DataFrame, rule: str) -> pd.DataFrame:
        """重采样数据"""
        if df.empty:
            return df
        
        df_resampled = df.set_index('timestamp').resample(rule).agg({
            'value': 'mean'
        }).reset_index()
        
        return df_resampled

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

    def _extract_spike_segments(self, query_df: pd.DataFrame) -> List[Dict]:
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

    def generate_synthetic_data(self, start_time: str, end_time: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        生成模拟的adservice container_cpu_system_seconds数据
        基于案例文件中的时间范围和异常分数
        """
        start_dt = self.to_datetime(start_time)
        end_dt = self.to_datetime(end_time)
        
        # 生成时间序列（每10秒一个点）
        time_range = pd.date_range(start=start_dt, end=end_dt, freq='10s')
        
        # 基线数据（正常情况下的CPU系统时间）
        baseline_mean = 0.1  # 正常情况下的平均值
        baseline_std = 0.02  # 正常情况下的标准差
        
        # 生成基线数据
        baseline_data = []
        for t in time_range:
            # 添加一些随机波动
            value = np.random.normal(baseline_mean, baseline_std)
            baseline_data.append({
                'timestamp': t,
                'value': max(0, value)  # 确保非负
            })
        
        baseline_df = pd.DataFrame(baseline_data)
        
        # 生成查询数据（包含spike）
        query_data = []
        
        # 根据案例文件，spike发生在 09:06:15 到 09:24:00
        spike_start = self.to_datetime("2022-03-20T09:06:15+08:00")
        spike_end = self.to_datetime("2022-03-20T09:24:00+08:00")
        
        for t in time_range:
            if spike_start <= t <= spike_end:
                # 在spike期间，值显著增加
                # 根据异常分数2.81661735657695，峰值应该是基线的约2.8倍
                spike_multiplier = 2.8 + np.random.normal(0, 0.2)  # 添加一些变化
                value = baseline_mean * spike_multiplier
            else:
                # 正常情况
                value = np.random.normal(baseline_mean, baseline_std)
            
            query_data.append({
                'timestamp': t,
                'value': max(0, value)
            })
        
        query_df = pd.DataFrame(query_data)
        
        return baseline_df, query_df

    def analyze_spike_detection(
        self, 
        cmdb_id: str, 
        metric_name: str, 
        start_time: str, 
        end_time: str
    ) -> Dict:
        """
        分析spike检测过程
        """
        print(f"🔍 开始分析 {cmdb_id} 的 {metric_name} 指标...")
        print(f"📅 时间范围: {start_time} 到 {end_time}")
        print(f"⚙️  检测参数:")
        print(f"   - spike_threshold: {self.spike_threshold}")
        print(f"   - relative_threshold: {self.relative_threshold}")
        print(f"   - min_spike_duration: {self.min_spike_duration_seconds}s")
        print(f"   - max_spike_duration: {self.max_spike_duration_seconds}s")
        print(f"   - derivative_threshold: {self.derivative_threshold}")
        print(f"   - min_peak_height: {self.min_peak_height}")
        print()
        
        # 生成模拟数据
        baseline_df, query_df = self.generate_synthetic_data(start_time, end_time)
        
        print(f"📊 数据统计:")
        print(f"   - 基线数据点数: {len(baseline_df)}")
        print(f"   - 查询数据点数: {len(query_df)}")
        print(f"   - 基线平均值: {baseline_df['value'].mean():.4f}")
        print(f"   - 基线标准差: {baseline_df['value'].std():.4f}")
        print(f"   - 查询数据最大值: {query_df['value'].max():.4f}")
        print(f"   - 查询数据平均值: {query_df['value'].mean():.4f}")
        print()
        
        # 重采样数据
        base = self._resample(baseline_df, self.resample_rule)
        query_resampled = self._resample(query_df, self.resample_rule).sort_values("timestamp")
        
        print(f"🔄 重采样后数据统计:")
        print(f"   - 基线重采样点数: {len(base)}")
        print(f"   - 查询重采样点数: {len(query_resampled)}")
        print()
        
        # 计算基线统计信息
        baseline_mean = float(base["value"].mean())
        baseline_std = float(base["value"].std(ddof=0)) if len(base) > 1 else 0.0
        
        print(f"📈 基线统计信息:")
        print(f"   - 基线均值: {baseline_mean:.4f}")
        print(f"   - 基线标准差: {baseline_std:.4f}")
        print()
        
        # 多种峰值检测方法
        print("🔍 开始峰值检测...")
        
        # 1. 基于阈值的检测
        query_resampled = self._detect_spikes_by_threshold(query_resampled, baseline_mean, baseline_std)
        threshold_spikes = query_resampled["is_spike"].sum()
        print(f"   1️⃣ 基于阈值检测: 发现 {threshold_spikes} 个峰值点")
        
        # 2. 基于导数的检测
        query_resampled = self._detect_spikes_by_derivative(query_resampled)
        derivative_spikes = query_resampled["is_derivative_spike"].sum()
        print(f"   2️⃣ 基于导数检测: 发现 {derivative_spikes} 个峰值点")
        
        # 3. 基于模式的检测
        query_resampled = self._detect_spikes_by_pattern(query_resampled)
        pattern_spikes = query_resampled["is_pattern_spike"].sum()
        print(f"   3️⃣ 基于模式检测: 发现 {pattern_spikes} 个峰值点")
        
        # 4. 合并检测结果
        query_resampled = self._merge_spike_detections(query_resampled)
        final_spikes = query_resampled["is_spike_final"].sum()
        print(f"   4️⃣ 合并检测结果: 发现 {final_spikes} 个峰值点")
        print()
        
        # 提取峰值段
        spike_segments = self._extract_spike_segments(query_resampled)
        print(f"📊 峰值段分析:")
        print(f"   - 发现 {len(spike_segments)} 个有效峰值段")
        
        results = []
        for i, segment in enumerate(spike_segments):
            print(f"\n   🎯 峰值段 {i+1}:")
            print(f"      - 开始时间: {segment['start']}")
            print(f"      - 结束时间: {segment['end']}")
            print(f"      - 峰值时间: {segment['peak_time']}")
            print(f"      - 峰值数值: {segment['peak_value']:.4f}")
            print(f"      - 基线倍数: {segment['baseline_ratio']:.4f}")
            print(f"      - 持续时间: {segment['duration_seconds']:.1f}秒")
            print(f"      - 异常分数: {segment['anomaly_score']:.4f}")
            
            # 分析检测原因
            seg_data = segment['segment_data']
            print(f"      - 检测原因分析:")
            
            # 检查各种检测方法
            threshold_detected = seg_data["is_spike"].any()
            derivative_detected = seg_data["is_derivative_spike"].any()
            pattern_detected = seg_data["is_pattern_spike"].any()
            
            if threshold_detected:
                max_baseline_ratio = seg_data["baseline_ratio"].max()
                max_local_ratio = seg_data["local_ratio"].max()
                print(f"        ✅ 阈值检测触发:")
                print(f"           - 最大基线倍数: {max_baseline_ratio:.4f} (阈值: {self.spike_threshold})")
                print(f"           - 最大局部倍数: {max_local_ratio:.4f} (阈值: {self.relative_threshold})")
            
            if derivative_detected:
                max_derivative_zscore = seg_data["derivative_zscore"].max()
                print(f"        ✅ 导数检测触发:")
                print(f"           - 最大导数Z-score: {max_derivative_zscore:.4f} (阈值: {self.derivative_threshold})")
            
            if pattern_detected:
                print(f"        ✅ 模式检测触发: 检测到尖峰模式")
            
            results.append({
                "cmdb_id": cmdb_id,
                "pattern": "Spike",
                "metric_name": metric_name,
                "start": segment["start"],
                "end": segment["end"],
                "anomaly_score": segment["anomaly_score"],
                "detection_reasons": {
                    "threshold_detected": threshold_detected,
                    "derivative_detected": derivative_detected,
                    "pattern_detected": pattern_detected,
                    "max_baseline_ratio": seg_data["baseline_ratio"].max() if threshold_detected else 0,
                    "max_local_ratio": seg_data["local_ratio"].max() if threshold_detected else 0,
                    "max_derivative_zscore": seg_data["derivative_zscore"].max() if derivative_detected else 0
                }
            })
        
        return {
            "results": results,
            "baseline_df": baseline_df,
            "query_df": query_df,
            "query_resampled": query_resampled,
            "baseline_mean": baseline_mean,
            "baseline_std": baseline_std
        }

    def plot_analysis(self, analysis_result: Dict, save_path: str = None):
        """绘制分析结果"""
        baseline_df = analysis_result["baseline_df"]
        query_df = analysis_result["query_df"]
        query_resampled = analysis_result["query_resampled"]
        baseline_mean = analysis_result["baseline_mean"]
        
        fig, axes = plt.subplots(3, 1, figsize=(15, 12))
        
        # 1. 原始数据对比
        axes[0].plot(baseline_df['timestamp'], baseline_df['value'], 
                    label='基线数据', alpha=0.7, color='blue')
        axes[0].plot(query_df['timestamp'], query_df['value'], 
                    label='查询数据', alpha=0.7, color='red')
        axes[0].axhline(y=baseline_mean, color='green', linestyle='--', 
                       label=f'基线均值 ({baseline_mean:.4f})')
        axes[0].set_title('原始数据对比')
        axes[0].set_ylabel('CPU系统时间')
        axes[0].legend()
        axes[0].grid(True, alpha=0.3)
        
        # 2. 重采样后的检测结果
        axes[1].plot(query_resampled['timestamp'], query_resampled['value'], 
                    label='重采样数据', color='red', alpha=0.7)
        axes[1].scatter(query_resampled[query_resampled['is_spike']]['timestamp'],
                       query_resampled[query_resampled['is_spike']]['value'],
                       color='orange', s=50, label='阈值检测', marker='o')
        axes[1].scatter(query_resampled[query_resampled['is_derivative_spike']]['timestamp'],
                       query_resampled[query_resampled['is_derivative_spike']]['value'],
                       color='purple', s=50, label='导数检测', marker='s')
        axes[1].scatter(query_resampled[query_resampled['is_pattern_spike']]['timestamp'],
                       query_resampled[query_resampled['is_pattern_spike']]['value'],
                       color='brown', s=50, label='模式检测', marker='^')
        axes[1].axhline(y=baseline_mean, color='green', linestyle='--', 
                       label=f'基线均值 ({baseline_mean:.4f})')
        axes[1].set_title('峰值检测结果')
        axes[1].set_ylabel('CPU系统时间')
        axes[1].legend()
        axes[1].grid(True, alpha=0.3)
        
        # 3. 检测指标
        axes[2].plot(query_resampled['timestamp'], query_resampled['baseline_ratio'], 
                    label='基线倍数', color='blue', alpha=0.7)
        axes[2].plot(query_resampled['timestamp'], query_resampled['local_ratio'], 
                    label='局部倍数', color='red', alpha=0.7)
        if 'derivative_zscore' in query_resampled.columns:
            axes[2].plot(query_resampled['timestamp'], query_resampled['derivative_zscore'], 
                        label='导数Z-score', color='green', alpha=0.7)
        axes[2].axhline(y=self.spike_threshold, color='orange', linestyle='--', 
                       label=f'Spike阈值 ({self.spike_threshold})')
        axes[2].axhline(y=self.relative_threshold, color='red', linestyle='--', 
                       label=f'相对阈值 ({self.relative_threshold})')
        axes[2].axhline(y=self.derivative_threshold, color='green', linestyle='--', 
                       label=f'导数阈值 ({self.derivative_threshold})')
        axes[2].set_title('检测指标')
        axes[2].set_xlabel('时间')
        axes[2].set_ylabel('检测指标值')
        axes[2].legend()
        axes[2].grid(True, alpha=0.3)
        
        # 格式化x轴时间显示
        for ax in axes:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            ax.xaxis.set_major_locator(mdates.MinuteLocator(interval=5))
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"📊 分析图表已保存到: {save_path}")
        
        plt.show()

def main():
    """主函数"""
    print("🚀 开始Spike检测分析...")
    print("=" * 60)
    
    # 创建分析器
    detector = SpikeAnalysisDetector(
        spike_threshold=5.0,
        relative_threshold=3.0,
        min_spike_duration_seconds=10,
        max_spike_duration_seconds=300,
        derivative_threshold=2.0,
        min_peak_height=0.1
    )
    
    # 分析adservice的container_cpu_system_seconds
    analysis_result = detector.analyze_spike_detection(
        cmdb_id="adservice",
        metric_name="container_cpu_system_seconds",
        start_time="2022-03-20T09:00:00+08:00",
        end_time="2022-03-20T09:30:00+08:00"
    )
    
    print("\n" + "=" * 60)
    print("📋 分析总结:")
    print(f"✅ 成功检测到 {len(analysis_result['results'])} 个Spike异常")
    
    for i, result in enumerate(analysis_result['results']):
        print(f"\n🎯 Spike {i+1}:")
        print(f"   - 时间范围: {result['start']} 到 {result['end']}")
        print(f"   - 异常分数: {result['anomaly_score']:.4f}")
        print(f"   - 检测原因:")
        reasons = result['detection_reasons']
        if reasons['threshold_detected']:
            print(f"     ✅ 阈值检测 (基线倍数: {reasons['max_baseline_ratio']:.4f}, 局部倍数: {reasons['max_local_ratio']:.4f})")
        if reasons['derivative_detected']:
            print(f"     ✅ 导数检测 (Z-score: {reasons['max_derivative_zscore']:.4f})")
        if reasons['pattern_detected']:
            print(f"     ✅ 模式检测")
    
    # 绘制分析图表
    print("\n📊 生成分析图表...")
    detector.plot_analysis(analysis_result, "spike_analysis_result.png")
    
    print("\n🎉 分析完成！")

if __name__ == "__main__":
    main()
