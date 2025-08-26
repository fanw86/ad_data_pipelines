import pandas as pd
import numpy as np
import datetime
import os
import sqlite3
from pathlib import Path
import pyarrow.parquet as pq
import gc
import psutil
import math
import hyperloglog

def log_memory_usage(step_name):
    process = psutil.Process(os.getpid())
    mem = process.memory_info().rss / (1024 * 1024)  # MB
    print(f"{step_name} - 内存使用: {mem:.2f} MB")

def read_afc_data(file_path, chunk_size=None):
    """读取AFC数据（支持CSV/Parquet格式），可进行分块处理"""
    cols = ['journey_id','leg_id', 'start_time', 'end_time', 'route', 'distance', 'uid']
    dtypes = {'leg_id': 'int8', 'route': 'str', 'distance': 'float32','uid': 'str'}
    
    if file_path.endswith('.parquet'):
        if chunk_size:
            return pq.ParquetFile(file_path).iter_batches(batch_size=chunk_size, columns=cols)
        else:
            return pd.read_parquet(file_path, columns=cols)
    else:  # CSV 格式
        if chunk_size:
            return pd.read_csv(
                file_path,
                usecols=cols,
                dtype=dtypes,
                parse_dates=['start_time', 'end_time'],
                infer_datetime_format=True,
                chunksize=chunk_size
            )
        else:
            return pd.read_csv(
                file_path,
                usecols=cols,
                dtype=dtypes,
                parse_dates=['start_time', 'end_time'],
                infer_datetime_format=True
            )

def read_line_data(folder, filename):
    """
    读取VDV数据（.x10格式）中的LINE Data，提取LINE_ABBR和OP_DEP_NO列
    """
    filepath = os.path.join(folder, filename)
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    
    data = []
    in_table = False
    try:
        with open(filepath, 'r', encoding='ISO-8859-1') as f:
            for line in f:
                # 检测表开始标记
                if line.startswith('tbl; LINE'):
                    in_table = True
                    continue
                # 检测表结束标记
                if line.startswith('end;'):
                    in_table = False
                    break
                # 仅处理数据行
                if in_table and line.startswith('rec;'):
                    parts = line.strip().split('; ')
                    if len(parts) >= 8:
                        # 提取LINE_ABBR (索引7) 和 OP_DEP_NO (索引6)
                        data.append({
                            'LINE_ABBR': parts[7].strip(),
                            'OP_DEP_NO': parts[6].strip()
                        })
    
        return pd.DataFrame(data)
    
    except Exception as e:
        raise Exception(f"Error loading {filepath}: {str(e)}")

def read_operating_department(folder, filename):
    """
    读取VDV数据（.x10格式）中OPERATING_DEPARTMENT表数据，提取OP_DEP_NO和OP_DEP_ABBR
    """
    filepath = os.path.join(folder, filename)
    
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"File not found: {filepath}")
    
    data = []
    in_table = False
    try:
        with open(filepath, 'r', encoding='ISO-8859-1') as f:
            for line in f:
                # 检测表开始标记
                if line.startswith('tbl; OPERATING_DEPARTMENT'):
                    in_table = True
                    continue
                # 检测表结束标记
                if line.startswith('end;'):
                    in_table = False
                    break
                 # 仅处理数据行
                if in_table and line.startswith('rec;'):
                    parts = line.strip().split('; ')
                    if len(parts) >= 5:
                        # 提取OP_DEP_NO (索引2) 和 OP_DEP_ABBR (索引3)
                        data.append({
                           'OP_DEP_NO': parts[2].strip(),
                           'OP_DEP_ABBR': parts[3].strip().replace('"', '')
                        })
                    
        return pd.DataFrame(data)
    
    except Exception as e:
        raise Exception(f"Error loading {filepath}: {str(e)}")

def map_region_id(op_dep_abbr):
    """
    根据OP_DEP_ABBR的前两个字母映射region_id
    """
    if op_dep_abbr.startswith('AD'):
        return 'Abu Dhabi'
    elif op_dep_abbr.startswith('ER'):
        return 'Al Ain'
    elif op_dep_abbr.startswith('WR'):
        return 'Al Dhafra'
    else:
        return 'Unknown'

def merge_data(afc_df, vdv_df, op_dept_df):
    """
    合并AFC、VDV和OPERATING_DEPARTMENT数据
    1. 通过route与LINE_ABBR连接获取OP_DEP_NO
    2. 通过OP_DEP_NO连接OPERATING_DEPARTMENT获取OP_DEP_ABBR
    3. 根据OP_DEP_ABBR映射region_id
    """
    # 清洗VDV数据 - 去除重复
    vdv_df = vdv_df.drop_duplicates(subset='LINE_ABBR', keep='first')
    
    # 清洗键值
    def deep_clean(s):
        s = str(s).strip()  # 去除两端空白
        s = ''.join(filter(str.isalnum, s))  # 只保留字母数字
        return s.upper()  # 统一大写
    
    afc_df['route_clean'] = afc_df['route'].apply(deep_clean)
    vdv_df['LINE_ABBR_clean'] = vdv_df['LINE_ABBR'].apply(deep_clean)
    
    # 合并数据
    merged_df = pd.merge(
        afc_df,
        vdv_df,
        left_on='route_clean',
        right_on='LINE_ABBR_clean',
        how='left'
    )
    
    # 第二次合并：添加OPERATING_DEPARTMENT信息   
    # 添加区域映射
    op_dept_df['region_id'] = op_dept_df['OP_DEP_ABBR'].apply(map_region_id)
    
    # 合并区域信息
    merged_df = pd.merge(
        merged_df,
        op_dept_df[['OP_DEP_NO', 'region_id']],
        on='OP_DEP_NO',
        how='left'
    )
    
    # 清理不需要的列
    merged_df = merged_df.drop(columns=['LINE_ABBR', 'route_clean', 'LINE_ABBR_clean'])
    
    return merged_df

def calculate_transfer_time(df):
    
    # 确保必要字段存在
    required_cols = ['uid', 'journey_id', 'leg_id', 'start_time', 'end_time']
    for col in required_cols:
        if col not in df.columns:
            raise ValueError(f"缺少必要列: {col}")
    
    # 创建唯一行程标识
    df['journey_key'] = df['uid'].astype(str) + '_' + df['journey_id'].astype(str)

    # 确保时间列是datetime类型
    df['start_time'] = pd.to_datetime(df['start_time'])
    df['end_time'] = pd.to_datetime(df['end_time'])

    # 按行程分组并按leg_id排序
    df = df.sort_values(['journey_key', 'leg_id'])
    
    # 计算下一leg的开始时间
    df['next_start'] = df.groupby('journey_key')['start_time'].shift(-1)
    
    # 计算换乘时间（分钟）
    df['transfer_time'] = (df['next_start'] - df['end_time']).dt.total_seconds() / 60.0

    # 处理异常值
    df.loc[df['transfer_time'] < 0, 'transfer_time'] = 0  # 负时间
    df.loc[df['transfer_time'] > 240, 'transfer_time'] = 0  # 超过4小时
    
    # 处理行程边界
    # 标记行程内最后一个leg（没有下一leg）
    last_leg_mask = df.groupby('journey_key')['leg_id'].transform('max') == df['leg_id']
    df.loc[last_leg_mask, 'transfer_time'] = 0
    
    # 清理临时列
    df = df.drop(columns=['next_start', 'journey_key'])
    
    return df

def process_chunk(chunk, vdv_line_data, vdv_op_dept_data):
    """处理单个数据块并返回聚合结果"""
    # 合并数据
    merged_chunk = merge_data(chunk, vdv_line_data, vdv_op_dept_data)
    
    # 过滤掉Unknown区域
    merged_chunk = merged_chunk[merged_chunk['region_id'] != 'Unknown']
    
    # 强制转换时间列为datetime
    merged_chunk['start_time'] = pd.to_datetime(merged_chunk['start_time'])
    merged_chunk['end_time'] = pd.to_datetime(merged_chunk['end_time'])
    
    # 转换距离单位：米 -> 千米
    merged_chunk['distance_km'] = merged_chunk['distance'] / 1000.0
    
    # 计算行程时间（Minutes）
    merged_chunk['trip_time'] = (merged_chunk['end_time'] - merged_chunk['start_time']).dt.total_seconds()/60.0
    
    # 计算换乘时间
    merged_chunk = calculate_transfer_time(merged_chunk)
    
    # 提取年月部分
    merged_chunk['year_month'] = merged_chunk['start_time'].dt.to_period('M')
    
    # 返回处理后的数据块
    return merged_chunk

def initialize_aggregator():
    """初始化聚合数据结构"""
    return {
        'region_daily': {},  # (date_str, region_id) -> {trip_count, uid_counter}
        'overall_daily': {}, # date_str -> {trip_count, uid_counter}
        'region': {},        # (year_month, region_id) -> 原有结构
        'overall': {}        # year_month -> 原有结构
    }

def update_aggregator(aggregator, df):
    """使用数据块更新聚合器"""
    #==按天聚合==#
    # 添加日期字符串列 (YYYY-MM-DD)
    df['date_str'] = df['start_time'].dt.strftime('%Y-%m-%d')

    # 区域按天聚合
    region_daily_group = df.groupby(['date_str', 'region_id'])
    for (date_str, region_id), group in region_daily_group:
        key = (date_str, region_id)
        if key not in aggregator['region_daily']:
            aggregator['region_daily'][key] = {
                'trip_count': 0,
                'uid_counter': hyperloglog.HyperLogLog(0.01)
            }
        aggregator['region_daily'][key]['trip_count'] += len(group)
        for uid in group['uid'].unique():
            aggregator['region_daily'][key]['uid_counter'].add(uid)
    
    # 整体按天聚合
    overall_daily_group = df.groupby('date_str')
    for date_str, group in overall_daily_group:
        if date_str not in aggregator['overall_daily']:
            aggregator['overall_daily'][date_str] = {
                'trip_count': 0,
                'uid_counter': hyperloglog.HyperLogLog(0.01)
            }
        aggregator['overall_daily'][date_str]['trip_count'] += len(group)
        for uid in group['uid'].unique():
            aggregator['overall_daily'][date_str]['uid_counter'].add(uid)
    
    
    #==按月聚合==#
    # 处理区域数据
    region_grouped = df.groupby(['year_month', 'region_id'])
    for (year_month, region_id), group_df in region_grouped:
        key = (year_month, region_id)
        
        # 初始化区域存储
        if key not in aggregator['region']:
            aggregator['region'][key] = {
                'passenger_trips_num': 0,
                'journey_num': 0,
                'transfer_num': 0,
                'travel_distance': 0.0,
                'travel_time': 0.0,
                'transfer_time': 0.0,
                'uid_counter': hyperloglog.HyperLogLog(0.01)  # 1%误差
            }
        
        # 更新指标
        stats = aggregator['region'][key]
        stats['passenger_trips_num'] += len(group_df)
        stats['journey_num'] += (group_df['leg_id'] == 1).sum()
        stats['transfer_num'] += (group_df['leg_id'] != 1).sum()
        stats['travel_distance'] += group_df['distance_km'].sum()
        stats['travel_time'] += group_df['trip_time'].sum()
        stats['transfer_time'] += group_df['transfer_time'].sum()
        
        # 更新唯一乘客计数
        for uid in group_df['uid'].unique():
            stats['uid_counter'].add(uid)
    
    # 处理整体数据（忽略区域）
    overall_grouped = df.groupby('year_month')
    for year_month, group_df in overall_grouped:
        # 初始化整体存储
        if year_month not in aggregator['overall']:
            aggregator['overall'][year_month] = {
                'passenger_trips_num': 0,
                'journey_num': 0,
                'transfer_num': 0,
                'travel_distance': 0.0,
                'travel_time': 0.0,
                'transfer_time': 0.0,
                'uid_counter': hyperloglog.HyperLogLog(0.01)  # 1%误差
            }
        
        # 更新指标
        stats = aggregator['overall'][year_month]
        stats['passenger_trips_num'] += len(group_df)
        stats['journey_num'] += (group_df['leg_id'] == 1).sum()
        stats['transfer_num'] += (group_df['leg_id'] != 1).sum()
        stats['travel_distance'] += group_df['distance_km'].sum()
        stats['travel_time'] += group_df['trip_time'].sum()
        stats['transfer_time'] += group_df['transfer_time'].sum()
        
        # 更新唯一乘客计数
        for uid in group_df['uid'].unique():
            stats['uid_counter'].add(uid)
    
    return aggregator

def get_days_in_month(year_month):
    """
    计算指定年月的天数
    year_month: Period对象 (如 '2025-01')
    """
    # 转换为datetime对象
    date_obj = year_month.to_timestamp()
    # 获取下个月的第一天
    if date_obj.month == 12:
        next_month = datetime.datetime(date_obj.year + 1, 1, 1)
    else:
        next_month = datetime.datetime(date_obj.year, date_obj.month + 1, 1)
    
    # 计算本月的最后一天
    last_day = next_month - datetime.timedelta(days=1)
    return last_day.day


def process_and_export(afc_file, vdv_folder, output_path, chunk_size=200000):
    """处理数据并导出CSV文件（内存优化版）"""
    # 加载VDV数据
    print("Loading VDV data...")
    vdv_line_data = read_line_data(vdv_folder, "i2263440.x10")
    vdv_op_dept_data = read_operating_department(vdv_folder, "i3333440.x10")
    
    # 初始化聚合器
    aggregator = initialize_aggregator()
    
    # 分块处理AFC数据
    print("Processing AFC data in chunks...")
    afc_reader = read_afc_data(afc_file, chunk_size=chunk_size)
    total_chunks = 0
    
    if isinstance(afc_reader, pd.DataFrame):  # 单块数据
        processed = process_chunk(afc_reader, vdv_line_data, vdv_op_dept_data)
        aggregator = update_aggregator(aggregator, processed)
        total_chunks = 1
    else:  # 分块处理
        for i, chunk in enumerate(afc_reader):
            total_chunks += 1
            
            if isinstance(chunk, pd.core.frame.DataFrame):
                df_chunk = chunk
            else:  # Parquet批次
                df_chunk = chunk.to_pandas()
            
            print(f"Processing chunk {i+1}...")
            processed = process_chunk(df_chunk, vdv_line_data, vdv_op_dept_data)
            
            # 更新聚合器
            aggregator = update_aggregator(aggregator, processed)
            
            # 主动释放内存
            del df_chunk, processed
            gc.collect()
            log_memory_usage(f"处理块 {i+1} 后")
    
    print(f"共处理 {total_chunks} 个数据块")
    log_memory_usage("所有块处理后")
    
    # 准备最终结果
    results = []
    current_time = datetime.datetime.now()
    
    # 处理区域结果
    for key, stats in aggregator['region'].items():
        year_month, region_id = key
        days_in_month = get_days_in_month(year_month)
        
        # === 计算日均出行次数 ===
        total_daily_avg = 0.0
        # 遍历该月每一天
        start_date = year_month.start_time
        end_date = year_month.end_time
        dates = pd.date_range(start_date, end_date)
        
        for d in dates:
            date_str = d.strftime('%Y-%m-%d')
            daily_key = (date_str, region_id)
            
            if daily_key in aggregator['region_daily']:
                daily_data = aggregator['region_daily'][daily_key]
                daily_trips = daily_data['trip_count']
                daily_users = len(daily_data['uid_counter'])
                
                # 计算当天的平均出行次数
                if daily_users > 0:
                    total_daily_avg += daily_trips / daily_users

        # 月平均值 = 每日平均值之和 / 当月天数
        avg_daily_trips_per_passenger = total_daily_avg / days_in_month
        
        # 计算平均值指标
        passenger_trips = stats['passenger_trips_num']
        journeys = stats['journey_num']
        
        avg_travel_time_by_trips = stats['travel_time'] / passenger_trips if passenger_trips > 0 else 0
        avg_travel_time_by_journey = stats['travel_time'] / journeys if journeys > 0 else 0
        avg_travel_distance_by_trips = stats['travel_distance'] / passenger_trips if passenger_trips > 0 else 0
        avg_travel_distance_by_journey = stats['travel_distance'] / journeys if journeys > 0 else 0
        
        results.append({
            't_date': year_month.strftime('%Y-%m'),
            'date_type': 'month',
            'region_id': region_id,
            'transfer_num': stats['transfer_num'],
            'journey_num': stats['journey_num'],
            'passenger_trips_num': passenger_trips,
            'travel_distance': stats['travel_distance'],
            'travel_time': stats['travel_time'],
            'transfer_time': stats['transfer_time'],
            'avg_daily_trips_per_passenger': avg_daily_trips_per_passenger,
            'avg_travel_time_by_trips': avg_travel_time_by_trips,
            'avg_travel_time_by_journey': avg_travel_time_by_journey,
            'avg_travel_distance_by_trips': avg_travel_distance_by_trips,
            'avg_travel_distance_by_journey': avg_travel_distance_by_journey,
            'create_by': 'system',
            'update_by': 'system',
            'create_time': current_time.strftime('%Y-%m-%d %H:%M:%S'),
            'update_time': current_time.strftime('%Y-%m-%d %H:%M:%S')
        })
    
    # 处理整体结果
    for year_month, stats in aggregator['overall'].items():
        days_in_month = get_days_in_month(year_month)
        total_daily_avg = 0.0
        start_date = year_month.start_time
        end_date = year_month.end_time
        dates = pd.date_range(start_date, end_date)
        
        for d in dates:
            date_str = d.strftime('%Y-%m-%d')
            if date_str in aggregator['overall_daily']:
                daily_data = aggregator['overall_daily'][date_str]
                daily_trips = daily_data['trip_count']
                daily_users = len(daily_data['uid_counter'])
                
                if daily_users > 0:
                    total_daily_avg += daily_trips / daily_users
        
        avg_daily_trips_per_passenger = total_daily_avg / days_in_month
        
        # 计算平均值指标
        passenger_trips = stats['passenger_trips_num']
        journeys = stats['journey_num']
        
        avg_travel_time_by_trips = stats['travel_time'] / passenger_trips if passenger_trips > 0 else 0
        avg_travel_time_by_journey = stats['travel_time'] / journeys if journeys > 0 else 0
        avg_travel_distance_by_trips = stats['travel_distance'] / passenger_trips if passenger_trips > 0 else 0
        avg_travel_distance_by_journey = stats['travel_distance'] / journeys if journeys > 0 else 0
        
        results.append({
            't_date': year_month.strftime('%Y-%m'),
            'date_type': 'month',
            'region_id': 'overall',
            'transfer_num': stats['transfer_num'],
            'journey_num': stats['journey_num'],
            'passenger_trips_num': passenger_trips,
            'travel_distance': stats['travel_distance'],
            'travel_time': stats['travel_time'],
            'transfer_time': stats['transfer_time'],
            'avg_daily_trips_per_passenger': avg_daily_trips_per_passenger,
            'avg_travel_time_by_trips': avg_travel_time_by_trips,
            'avg_travel_time_by_journey': avg_travel_time_by_journey,
            'avg_travel_distance_by_trips': avg_travel_distance_by_trips,
            'avg_travel_distance_by_journey': avg_travel_distance_by_journey,
            'create_by': 'system',
            'update_by': 'system',
            'create_time': current_time.strftime('%Y-%m-%d %H:%M:%S'),
            'update_time': current_time.strftime('%Y-%m-%d %H:%M:%S')
        })
    
    # 转换为DataFrame并导出
    result_df = pd.DataFrame(results)
    result_df.to_csv(output_path, index=False)
    print(f"Results exported to {output_path}")
    log_memory_usage("最终导出后")
    
    return result_df

# 主函数
def main():
    # Configuration
    vdv_folder = "../data/input/VDV_data"
    output_file = '../data/output/ads_passenger_travel_info_mi0811.csv'
    afc_file = '../data/input/2025_H1_afc_data_endstop_imputed.parquet'  # 新的Parquet文件路径
    
    # 减小块大小以适应内存限制
    process_and_export(
        afc_file=afc_file,
        vdv_folder=vdv_folder,
        output_path=output_file,
        chunk_size=200000 
    )

if __name__ == "__main__":
    main()