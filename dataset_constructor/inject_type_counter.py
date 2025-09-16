#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
脚本用于统计case文件夹中各个case的ground_truth的inject_type数量
"""

import json
import os
from collections import Counter
import glob

def count_inject_types(case_dir):
    """
    统计case文件夹中所有case的inject_type数量
    
    Args:
        case_dir (str): case文件夹路径
    
    Returns:
        dict: inject_type统计结果
    """
    inject_type_counter = Counter()
    case_files = []
    error_files = []
    
    # 获取所有JSON文件
    json_files = glob.glob(os.path.join(case_dir, "*.json"))
    
    print(f"找到 {len(json_files)} 个JSON文件")
    
    for json_file in json_files:
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 检查是否有ground_truth字段
            if 'ground_truth' in data and 'inject_type' in data['ground_truth']:
                inject_type = data['ground_truth']['inject_type']
                inject_type_counter[inject_type] += 1
                case_files.append({
                    'file': os.path.basename(json_file),
                    'inject_type': inject_type,
                    'inject_component': data['ground_truth'].get('inject_component', 'N/A')
                })
            else:
                print(f"警告: {json_file} 中没有找到ground_truth或inject_type字段")
                error_files.append(json_file)
                
        except Exception as e:
            print(f"错误: 无法读取文件 {json_file}: {str(e)}")
            error_files.append(json_file)
    
    return inject_type_counter, case_files, error_files

def print_results(inject_type_counter, case_files, error_files):
    """
    打印统计结果
    
    Args:
        inject_type_counter (Counter): inject_type计数器
        case_files (list): case文件列表
        error_files (list): 错误文件列表
    """
    print("\n" + "="*80)
    print("INJECT_TYPE 统计结果")
    print("="*80)
    
    # 按数量排序显示
    sorted_types = inject_type_counter.most_common()
    
    print(f"\n总计发现 {len(inject_type_counter)} 种不同的inject_type:")
    print("-" * 60)
    
    for inject_type, count in sorted_types:
        print(f"{inject_type:<50} : {count:>3} 个case")
    
    print(f"\n总计处理了 {len(case_files)} 个case文件")
    if error_files:
        print(f"处理失败的文件: {len(error_files)} 个")
        for error_file in error_files:
            print(f"  - {error_file}")
    
    # 显示详细信息（可选）
    print(f"\n详细信息 (前20个case):")
    print("-" * 80)
    print(f"{'文件名':<40} {'Inject Type':<30} {'Component':<20}")
    print("-" * 80)
    
    for i, case in enumerate(case_files[:20]):
        filename = case['file'][:37] + "..." if len(case['file']) > 40 else case['file']
        inject_type = case['inject_type'][:27] + "..." if len(case['inject_type']) > 30 else case['inject_type']
        component = case['inject_component'][:17] + "..." if len(case['inject_component']) > 20 else case['inject_component']
        print(f"{filename:<40} {inject_type:<30} {component:<20}")
    
    if len(case_files) > 20:
        print(f"... 还有 {len(case_files) - 20} 个case文件")

def main():
    """主函数"""
    # case文件夹路径
    case_dir = "/home/ubuntu/ls/aiopsarena-main/dataset_constructor/case"
    
    if not os.path.exists(case_dir):
        print(f"错误: case文件夹不存在: {case_dir}")
        return
    
    print("开始统计inject_type...")
    print(f"扫描文件夹: {case_dir}")
    
    # 统计inject_type
    inject_type_counter, case_files, error_files = count_inject_types(case_dir)
    
    # 打印结果
    print_results(inject_type_counter, case_files, error_files)
    
    # 保存结果到文件
    output_file = "/home/ubuntu/ls/aiopsarena-main/dataset_constructor/inject_type_statistics.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("INJECT_TYPE 统计结果\n")
        f.write("="*80 + "\n\n")
        
        f.write(f"总计发现 {len(inject_type_counter)} 种不同的inject_type:\n")
        f.write("-" * 60 + "\n")
        
        for inject_type, count in inject_type_counter.most_common():
            f.write(f"{inject_type:<50} : {count:>3} 个case\n")
        
        f.write(f"\n总计处理了 {len(case_files)} 个case文件\n")
        if error_files:
            f.write(f"处理失败的文件: {len(error_files)} 个\n")
            for error_file in error_files:
                f.write(f"  - {error_file}\n")
    
    print(f"\n结果已保存到: {output_file}")

if __name__ == "__main__":
    main()


