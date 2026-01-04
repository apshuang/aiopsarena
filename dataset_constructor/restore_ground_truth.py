#!/usr/bin/env python3
"""
故障注入ground_truth恢复脚本

用于从yaml文件中恢复case文件丢失的ground_truth信息。
支持单个文件处理和批处理两种模式。

使用方法：
1. 修改下面的配置区域
2. 运行脚本: python3 restore_ground_truth.py
"""

import json
import re
import yaml
from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any
import glob

# =============================
# 配置区域 - 请根据需要修改以下配置
# =============================

# 运行模式: 'single' 或 'batch'
# - 'single': 处理单个case文件
# - 'batch': 批量处理目录下所有case文件
MODE = 'batch'  # 可选: 'single' 或 'batch'

# 单个文件处理模式配置（当 MODE='single' 时使用）
CASE_FILE = 'case/Bravo_case_20251022_01-00-00.json'  # case文件路径（相对或绝对路径）

# 批处理模式配置（当 MODE='batch' 时使用）
CASE_DIR = 'case'  # case文件所在目录（相对或绝对路径）

# yaml文件所在目录
YAML_DIR = '/home/ubuntu/ls/aiopsarena-main/platform-backend-master/chaosmesh_app/injection'

# case文件的时间窗口长度（分钟）
TIME_WINDOW_MINUTES = 20  # 默认20分钟，根据实际情况修改

# 是否启用干运行模式（True: 只打印信息不实际修改文件, False: 实际修改文件）
DRY_RUN = False  # 可选: True 或 False

# =============================
# 配置区域结束
# =============================


def parse_duration_str(duration_str: str) -> int:
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


def parse_time_window(time_window_str: str) -> tuple[datetime, datetime]:
    """
    解析时间窗口字符串，返回开始和结束时间
    
    Args:
        time_window_str: 格式如 "2025-10-22T01:00:00+08:00 ~ 2025-10-22T01:20:00+08:00"
    
    Returns:
        (start_time, end_time) 两个datetime对象
    """
    parts = time_window_str.split(" ~ ")
    if len(parts) != 2:
        raise ValueError(f"时间窗口格式错误: {time_window_str}")
    
    start_str = parts[0].strip()
    end_str = parts[1].strip()
    
    start_time = datetime.fromisoformat(start_str)
    end_time = datetime.fromisoformat(end_str)
    
    return start_time, end_time


def find_matching_yaml_file(window_start: datetime, window_end: datetime, yaml_dir: Path) -> Optional[Path]:
    """
    根据时间窗口找到匹配的yaml文件
    
    验证yaml文件中的故障注入时间是否在case的时间窗口内
    
    Args:
        window_start: case时间窗口开始时间（带时区）
        window_end: case时间窗口结束时间（带时区）
        yaml_dir: yaml文件所在目录
    
    Returns:
        匹配的yaml文件路径，如果未找到则返回None
    """
    # 将时间转换为UTC+8时区（如果还没有时区信息）
    if window_start.tzinfo is None:
        window_start = window_start.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    else:
        window_start = window_start.astimezone(ZoneInfo("Asia/Shanghai"))
    
    if window_end.tzinfo is None:
        window_end = window_end.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    else:
        window_end = window_end.astimezone(ZoneInfo("Asia/Shanghai"))
    
    # 提取日期
    date_str = window_start.strftime("%Y%m%d")
    
    # 获取该日期的所有yaml文件
    pattern = f"{date_str}-*.yaml"
    yaml_files = list(yaml_dir.glob(pattern))
    
    if not yaml_files:
        print(f"  警告: 未找到日期 {date_str} 的yaml文件")
        return None
    
    # 查找时间窗口内的yaml文件
    matching_files = []
    
    for yaml_file in yaml_files:
        # 从文件名提取时间：YYYYMMDD-HHMM.yaml
        match = re.match(r'(\d{8})-(\d{2})(\d{2})\.yaml', yaml_file.name)
        if not match:
            continue
        
        file_date = match.group(1)
        file_hour = int(match.group(2))
        file_minute = int(match.group(3))
        
        # 只考虑同一天的文件
        if file_date != date_str:
            continue
        
        # 构建yaml文件对应的故障注入时间
        inject_time = datetime(
            int(file_date[:4]), int(file_date[4:6]), int(file_date[6:8]),
            file_hour, file_minute, 0,
            tzinfo=ZoneInfo("Asia/Shanghai")
        )
        
        # 验证故障注入时间是否在时间窗口内
        if window_start <= inject_time <= window_end:
            matching_files.append((yaml_file, inject_time))
    
    if not matching_files:
        print(f"  警告: 未找到时间窗口内的yaml文件")
        print(f"    时间窗口: {window_start.strftime('%Y-%m-%d %H:%M:%S')} ~ {window_end.strftime('%Y-%m-%d %H:%M:%S')}")
        return None
    
    if len(matching_files) > 1:
        print(f"  警告: 找到 {len(matching_files)} 个时间窗口内的yaml文件，使用第一个")
        for yf, it in matching_files:
            print(f"    - {yf.name} (故障注入时间: {it.strftime('%Y-%m-%d %H:%M:%S')})")
    
    best_file, inject_time = matching_files[0]
    print(f"  找到匹配的yaml文件: {best_file.name}")
    print(f"    故障注入时间: {inject_time.strftime('%Y-%m-%d %H:%M:%S')} (在时间窗口内)")
    
    return best_file


def extract_ground_truth_from_yaml(yaml_file: Path, inject_start_time: datetime) -> Optional[Dict[str, Any]]:
    """
    从yaml文件中提取ground_truth信息
    
    Args:
        yaml_file: yaml文件路径
        inject_start_time: 故障注入开始时间（用于计算recover_time）
    
    Returns:
        ground_truth字典，格式符合AIOps-Bravo规范
    """
    try:
        with open(yaml_file, 'r', encoding='utf-8') as f:
            yaml_data = yaml.safe_load(f)
    except Exception as e:
        print(f"  错误: 无法读取yaml文件 {yaml_file}: {e}")
        return None
    
    # 提取kind
    kind = yaml_data.get('kind', '')
    if kind not in ['StressChaos', 'HTTPChaos']:
        print(f"  警告: 未知的故障注入类型: {kind}")
        return None
    
    # 提取spec
    spec = yaml_data.get('spec', {})
    if not spec:
        print(f"  错误: yaml文件中缺少spec字段")
        return None
    
    # 提取duration
    duration_str = spec.get('duration', '')
    if not duration_str:
        print(f"  错误: yaml文件中缺少duration字段")
        return None
    
    try:
        duration_sec = parse_duration_str(duration_str)
    except Exception as e:
        print(f"  错误: 无法解析duration: {e}")
        return None
    
    # 提取inject_component
    selector = spec.get('selector', {})
    label_selectors = selector.get('labelSelectors', {})
    inject_component = label_selectors.get('app', '')
    if not inject_component:
        print(f"  错误: yaml文件中缺少selector.labelSelectors.app字段")
        return None
    
    # 提取inject_sub_type
    if kind == 'StressChaos':
        stressors = spec.get('stressors', {})
        if not stressors:
            print(f"  错误: StressChaos类型缺少stressors字段")
            return None
        # 获取第一个stressor类型（通常是memory或cpu）
        inject_sub_type = list(stressors.keys())[0]
    elif kind == 'HTTPChaos':
        # HTTPChaos的sub_type可能是delay、abort、replace等
        # 检查spec中的字段
        if 'delay' in spec:
            inject_sub_type = 'delay'
        elif 'abort' in spec:
            inject_sub_type = 'abort'
        elif 'replace' in spec:
            inject_sub_type = 'replace'
        else:
            print(f"  警告: HTTPChaos类型无法确定sub_type，使用默认值'unknown'")
            inject_sub_type = 'unknown'
    else:
        inject_sub_type = 'unknown'
    
    # 构建inject_type
    inject_type = f'{kind}-{inject_sub_type}'
    
    # 计算recover_time
    # 确保inject_start_time有时区信息
    if inject_start_time.tzinfo is None:
        inject_start_time = inject_start_time.replace(tzinfo=ZoneInfo("Asia/Shanghai"))
    else:
        inject_start_time = inject_start_time.astimezone(ZoneInfo("Asia/Shanghai"))
    
    inject_time = inject_start_time
    recover_time = inject_start_time + timedelta(seconds=duration_sec)
    
    # 构建ground_truth
    ground_truth = {
        "inject_time": inject_time.isoformat(),
        "recover_time": recover_time.isoformat(),
        "inject_type": inject_type,
        "inject_component": inject_component
    }
    
    return ground_truth


def restore_single_case(case_file: Path, yaml_dir: Path, dry_run: bool = False) -> bool:
    """
    恢复单个case文件的ground_truth
    
    Args:
        case_file: case文件路径
        yaml_dir: yaml文件所在目录
        dry_run: 如果为True，只打印信息不实际修改文件
    
    Returns:
        是否成功恢复
    """
    print(f"\n处理文件: {case_file.name}")
    
    # 读取case文件
    try:
        with open(case_file, 'r', encoding='utf-8') as f:
            case_data = json.load(f)
    except Exception as e:
        print(f"  错误: 无法读取case文件: {e}")
        return False
    
    # 检查ground_truth是否已存在
    if case_data.get('ground_truth') is not None:
        print(f"  提示: 该case文件已有ground_truth，跳过")
        return True
    
    # 提取时间窗口
    time_window_str = case_data.get('fault_time_window', '')
    if not time_window_str:
        print(f"  错误: case文件中缺少fault_time_window字段")
        return False
    
    try:
        start_time, end_time = parse_time_window(time_window_str)
    except Exception as e:
        print(f"  错误: 无法解析时间窗口: {e}")
        return False
    
    print(f"  时间窗口: {start_time.strftime('%Y-%m-%d %H:%M:%S')} ~ {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 验证时间窗口长度
    window_duration = (end_time - start_time).total_seconds() / 60.0
    if abs(window_duration - TIME_WINDOW_MINUTES) > 1.0:  # 允许1分钟的误差
        print(f"  警告: 时间窗口长度 ({window_duration:.1f}分钟) 与配置的 {TIME_WINDOW_MINUTES} 分钟不一致")
    
    # 找到匹配的yaml文件（必须在时间窗口内）
    yaml_file = find_matching_yaml_file(start_time, end_time, yaml_dir)
    if not yaml_file:
        print(f"  失败: 未找到时间窗口内的yaml文件")
        return False
    
    # 从yaml文件名提取故障注入时间
    match = re.match(r'(\d{8})-(\d{2})(\d{2})\.yaml', yaml_file.name)
    if not match:
        print(f"  错误: 无法从yaml文件名提取时间: {yaml_file.name}")
        return False
    
    file_date = match.group(1)
    file_hour = int(match.group(2))
    file_minute = int(match.group(3))
    # 构建inject_start_time（使用yaml文件名中的时间，这个时间已经在时间窗口内）
    inject_start_time = datetime(
        int(file_date[:4]), int(file_date[4:6]), int(file_date[6:8]),
        file_hour, file_minute, 0,
        tzinfo=ZoneInfo("Asia/Shanghai")
    )
    
    ground_truth = extract_ground_truth_from_yaml(yaml_file, inject_start_time)
    if not ground_truth:
        print(f"  失败: 无法从yaml文件提取ground_truth")
        return False
    
    print(f"  提取的ground_truth:")
    print(f"    inject_time: {ground_truth['inject_time']}")
    print(f"    recover_time: {ground_truth['recover_time']}")
    print(f"    inject_type: {ground_truth['inject_type']}")
    print(f"    inject_component: {ground_truth['inject_component']}")
    
    # 更新case文件
    if not dry_run:
        case_data['ground_truth'] = ground_truth
        try:
            with open(case_file, 'w', encoding='utf-8') as f:
                json.dump(case_data, f, indent=4, ensure_ascii=False)
            print(f"  成功: 已更新case文件")
        except Exception as e:
            print(f"  错误: 无法写入case文件: {e}")
            return False
    else:
        print(f"  [DRY RUN] 将更新ground_truth")
    
    return True


def restore_batch_cases(case_dir: Path, yaml_dir: Path, dry_run: bool = False) -> Dict[str, int]:
    """
    批量恢复case文件的ground_truth
    
    Args:
        case_dir: case文件所在目录
        yaml_dir: yaml文件所在目录
        dry_run: 如果为True，只打印信息不实际修改文件
    
    Returns:
        统计信息字典，包含成功、失败、跳过的数量
    """
    print(f"\n开始批量处理...")
    print(f"Case目录: {case_dir}")
    print(f"Yaml目录: {yaml_dir}")
    if dry_run:
        print(f"[DRY RUN模式]")
    
    # 查找所有case文件
    case_files = list(case_dir.glob("Bravo_case_*.json"))
    if not case_files:
        print(f"  错误: 未找到case文件（模式: Bravo_case_*.json）")
        return {"success": 0, "failed": 0, "skipped": 0, "total": 0}
    
    print(f"  找到 {len(case_files)} 个case文件")
    
    stats = {"success": 0, "failed": 0, "skipped": 0, "total": len(case_files)}
    
    for case_file in case_files:
        try:
            # 读取case文件检查ground_truth
            with open(case_file, 'r', encoding='utf-8') as f:
                case_data = json.load(f)
            
            if case_data.get('ground_truth') is not None:
                stats["skipped"] += 1
                continue
            
            # 尝试恢复
            if restore_single_case(case_file, yaml_dir, dry_run):
                stats["success"] += 1
            else:
                stats["failed"] += 1
        except Exception as e:
            print(f"  错误: 处理文件 {case_file.name} 时出错: {e}")
            stats["failed"] += 1
    
    print(f"\n批量处理完成:")
    print(f"  总计: {stats['total']}")
    print(f"  成功: {stats['success']}")
    print(f"  失败: {stats['failed']}")
    print(f"  跳过: {stats['skipped']}")
    
    return stats


def main():
    """主函数"""
    # 检查yaml目录
    yaml_dir = Path(YAML_DIR)
    if not yaml_dir.exists():
        print(f"错误: yaml目录不存在: {yaml_dir}")
        print(f"请检查配置中的 YAML_DIR 路径是否正确")
        return
    
    # 根据模式执行相应操作
    if MODE == 'single':
        # 单文件处理模式
        case_file = Path(CASE_FILE)
        # 如果是相对路径，尝试从脚本所在目录解析
        if not case_file.is_absolute():
            script_dir = Path(__file__).parent
            case_file = script_dir / case_file
        
        if not case_file.exists():
            print(f"错误: case文件不存在: {case_file}")
            print(f"请检查配置中的 CASE_FILE 路径是否正确")
            return
        
        print(f"运行模式: 单个文件处理")
        print(f"Case文件: {case_file}")
        print(f"Yaml目录: {yaml_dir}")
        if DRY_RUN:
            print(f"模式: 干运行（不会实际修改文件）")
        else:
            print(f"模式: 实际执行（将修改文件）")
        
        restore_single_case(case_file, yaml_dir, DRY_RUN)
        
    elif MODE == 'batch':
        # 批处理模式
        case_dir = Path(CASE_DIR)
        # 如果是相对路径，尝试从脚本所在目录解析
        if not case_dir.is_absolute():
            script_dir = Path(__file__).parent
            case_dir = script_dir / case_dir
        
        if not case_dir.exists():
            print(f"错误: case目录不存在: {case_dir}")
            print(f"请检查配置中的 CASE_DIR 路径是否正确")
            return
        
        print(f"运行模式: 批量处理")
        print(f"Case目录: {case_dir}")
        print(f"Yaml目录: {yaml_dir}")
        if DRY_RUN:
            print(f"模式: 干运行（不会实际修改文件）")
        else:
            print(f"模式: 实际执行（将修改文件）")
        
        restore_batch_cases(case_dir, yaml_dir, DRY_RUN)
        
    else:
        print(f"错误: 未知的运行模式: {MODE}")
        print(f"请将配置中的 MODE 设置为 'single' 或 'batch'")
        return


if __name__ == '__main__':
    main()

