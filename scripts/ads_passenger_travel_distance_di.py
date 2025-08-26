import pandas as pd
import numpy as np
import datetime
import os
import sqlite3
from pathlib import Path
import pyarrow.parquet as pq

DATABASE_PATH = 'passenger_travel_distance_creation_times.db'

def initialize_database():
    """初始化数据库"""
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS passenger_travel_distance_creation_times (
                t_date TEXT,
                region_id TEXT,
                distance_type TEXT,
                first_creation_time TIMESTAMP,
                PRIMARY KEY (t_date, region_id, distance_type)
            )
        ''')
        conn.commit()

def get_or_create_timestamp(t_date, region_id, distance_type):
    """获取或创建首次创建时间戳"""
    with sqlite3.connect(DATABASE_PATH) as conn:
        cursor = conn.cursor()
        
        # 尝试获取现有记录
        cursor.execute('''
            SELECT first_creation_time
            FROM passenger_travel_distance_creation_times
            WHERE t_date = ? AND region_id = ? AND distance_type = ?
        ''', (t_date, region_id, distance_type))
        
        result = cursor.fetchone()
        
        current_time = datetime.datetime.now()
        
        if result:
            # 记录已存在，返回存储的时间戳
            return datetime.datetime.fromisoformat(result[0])
        else:
            # 记录不存在，创建新记录
            cursor.execute('''
                INSERT INTO passenger_travel_distance_creation_times (t_date, region_id, distance_type, first_creation_time)
                VALUES (?, ?, ?, ?)
            ''', (t_date, region_id, distance_type, current_time.isoformat()))
            conn.commit()
            return current_time
        
def read_afc_data_in_chunks(file_path, chunk_size=500_000):
    """
    分块读取大型 Parquet 文件，返回 DataFrame 生成器
    """
    cols = ['leg_id', 'start_time', 'route', 'distance']
    parquet_file = pq.ParquetFile(file_path)
    for batch in parquet_file.iter_batches(batch_size=chunk_size, columns=cols):
        yield batch.to_pandas()

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

def process_and_export(merged_df, output_path=None, add_overall=False):
    """
    处理数据并导出CSV文件
    """
    initialize_database()

    # 确保数据不为空
    if merged_df.empty:
        print("警告：合并后的数据为空！")
        return None
    
    # 转换距离单位：米 -> 千米
    merged_df['distance_km'] = merged_df['distance'] / 1000.0
    
    # 定义距离区间
    distance_bins = [0, 5, 10, 15, 20, float('inf')]
    distance_labels = ['0-5km', '5-10km', '10-15km', '15-20km', '>20km']
    
    # 将距离分配到区间
    merged_df['distance_type'] = pd.cut(
        merged_df['distance_km'],
        bins=distance_bins,
        labels=distance_labels,
        right=True,
        include_lowest=True
    )
    
    # 提取日期部分
    merged_df['t_date'] = pd.to_datetime(merged_df['start_time']).dt.date
    
    # 按日期和region_id分组
    grouped = merged_df.groupby(['t_date', 'region_id', 'distance_type'])
    
    # 准备结果数据集
    results = []
    current_time = datetime.datetime.now()
    
    # 遍历每个分组
    for (t_date, region_id, dist_type), group in grouped:
        # 计算总出行量（passenger_trips_num）
        passenger_trips_num = len(group)
        
        # 计算行程量（journey_num） - leg_id=1的数量
        journey_num = len(group[group['leg_id'] == 1])
        
        # 计算换乘量（transfer_num） - leg_id!=1的数量
        transfer_num = len(group[group['leg_id'] != 1])
        
        # 获取或创建首次创建时间
        date_str = t_date.strftime('%Y-%m-%d')  # 转换为字符串格式
        create_time = get_or_create_timestamp(date_str, region_id, dist_type)

        # 添加到结果集
        results.append({
            't_date': t_date,
            'date_type': 'day',
            'region_id': region_id,
            'distance_type': dist_type,
            'transfer_num': transfer_num,
            'journey_num': journey_num,
            'passenger_trips_num': passenger_trips_num,
            'create_by': 'system',
            'update_by': 'system',
            'create_time': create_time,
            'update_time': current_time
        })
    
    # 统计 overall
    if add_overall:
        grouped_overall = merged_df.groupby(['t_date', 'distance_type'])
        for (t_date, dist_type), group in grouped_overall:
            passenger_trips_num = len(group)
            journey_num = len(group[group['leg_id'] == 1])
            transfer_num = len(group[group['leg_id'] != 1])
            date_str = t_date.strftime('%Y-%m-%d')
            create_time = get_or_create_timestamp(date_str, 'overall', dist_type)
            results.append({
                't_date': t_date,
                'date_type': 'day',
                'region_id': 'overall',
                'distance_type': dist_type,
                'transfer_num': transfer_num,
                'journey_num': journey_num,
                'passenger_trips_num': passenger_trips_num,
                'create_by': 'system',
                'update_by': 'system',
                'create_time': create_time,
                'update_time': current_time
            })

    # 转换为DataFrame
    if results:
        result_df = pd.DataFrame(results)
        # 转换日期格式
        result_df['t_date'] = pd.to_datetime(result_df['t_date']).dt.strftime('%Y-%m-%d')

        # 转换时间格式
        result_df['update_time'] = pd.to_datetime(result_df['update_time'])
        result_df['create_time'] = pd.to_datetime(result_df['create_time'])

        # 格式化时间列为ISO格式
        result_df['update_time'] = result_df['update_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        result_df['create_time'] = result_df['create_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        if output_path:
            result_df.to_csv(output_path, index=False)
        return result_df
    else:
        print("警告：没有生成任何结果记录！")
        return None

def main():
    folder = "../data/input/VDV_data"
    output_file = '../data/output/ads_passenger_travel_distance_di0811.csv'
    afc_parquet_file = '../data/input/2025_H1_afc_data_endstop_imputed.parquet'

    print("Loading VDV LINE data...")
    vdv_line_data = read_line_data(folder, "i2261280.x10")
    print(f"读取到 {len(vdv_line_data)} 条记录")

    print("Load VDV OPERATING_DEPARTMENT data...")
    vdv_op_dept_data = read_operating_department(folder, "i3331280.x10")
    print(f"读取到 {len(vdv_op_dept_data)} 条记录")

    # 清空输出文件并写入表头
    header_written = False

    print("Processing AFC data in chunks...")
    for i, chunk in enumerate(read_afc_data_in_chunks(afc_parquet_file)):
        print(f"Processing chunk {i+1} with {len(chunk)} rows")
        merged_data = merge_data(chunk, vdv_line_data, vdv_op_dept_data)
        # 过滤掉 Unknown 地区
        merged_data = merged_data[merged_data['region_id'] != 'Unknown']
        if not merged_data.empty:
            result_df = process_and_export(merged_data, None, add_overall=True)
            if result_df is not None and not result_df.empty:
                result_df.to_csv(output_file, mode='a', header=not header_written, index=False)
                header_written = True

    print(f"Processing complete. Results exported to {output_file}")
    
if __name__ == "__main__":
    main()