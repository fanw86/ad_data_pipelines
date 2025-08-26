import pandas as pd
import numpy as np
import datetime
import os
from pathlib import Path
import pyarrow.parquet as pq

def read_afc_data_parquet(file_path, chunksize=1000000):
    """
    读取Parquet格式的AFC数据（分块读取）
    返回一个生成器，每次产生一个包含leg_id, start_time, route的DataFrame
    """
    # 创建Parquet文件对象
    parquet_file = pq.ParquetFile(file_path)
    
    # 分块读取数据
    for batch in parquet_file.iter_batches(batch_size=chunksize, columns=['leg_id', 'start_time', 'route']):
        df = batch.to_pandas()
        df['start_time'] = pd.to_datetime(df['start_time'])
        yield df

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
    
    # 合并AFC和VDV LINE数据
    afc_df['route_clean'] = afc_df['route'].apply(deep_clean)
    vdv_df['LINE_ABBR_clean'] = vdv_df['LINE_ABBR'].apply(deep_clean)
        
    # 找出不匹配的键值
    afc_routes = set(afc_df['route_clean'].unique())
    vdv_routes = set(vdv_df['LINE_ABBR_clean'].unique())
    
    #AFC数据中存在route值为空的，因此无法匹配到region，在后续统计journey_num时也会被忽略
    #目前使用的AFC数据都存在这个情况，因此在其他两个代码文件中不再统计此项
    missing_in_vdv = afc_routes - vdv_routes
    print(f"\nVDV中缺失的route值 (前10个): {list(missing_in_vdv)[:10]}")
    
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

    # 检查合并结果，其他两个代码文件也不再统计
    match_count = merged_df['OP_DEP_NO'].notna().sum()
    print(f"\n合并结果: 总记录数={len(merged_df)}, 匹配记录数={match_count}")

    # 清理不需要的列
    merged_df = merged_df.drop(columns=['LINE_ABBR', 'route_clean', 'LINE_ABBR_clean'])
    
    # 过滤掉Unknown区域
    merged_df = merged_df[merged_df['region_id'] != 'Unknown']
    
    return merged_df

def accumulate_leg_counts(merged_df, accumulator):
    """
    按日期和区域累加leg_id计数
    同时添加overall区域统计
    """
    # 提取日期部分
    merged_df['t_date'] = merged_df['start_time'].dt.date
    
    # 按日期和region_id分组
    grouped = merged_df.groupby(['t_date', 'region_id'])
    
    # 遍历每个分组
    for (t_date, region_id), group in grouped:
        # 转换为字符串日期以便在字典中使用
        date_str = t_date.strftime('%Y-%m-%d')
        
        # 统计当前分组的leg_id计数
        leg_counts = group['leg_id'].value_counts().sort_index()
        
        # 更新特定区域的计数
        if (date_str, region_id) not in accumulator:
            accumulator[(date_str, region_id)] = leg_counts
        else:
            accumulator[(date_str, region_id)] = accumulator[(date_str, region_id)].add(leg_counts, fill_value=0)
        
        # 更新overall区域的计数
        if (date_str, 'overall') not in accumulator:
            accumulator[(date_str, 'overall')] = leg_counts
        else:
            accumulator[(date_str, 'overall')] = accumulator[(date_str, 'overall')].add(leg_counts, fill_value=0)
    
    return accumulator

def generate_final_result(accumulator):
    """
    根据累加的leg计数生成最终结果
    """
    results = []
    current_time = datetime.datetime.now()
    
    # 处理每个日期和区域组合
    for (t_date, region_id), leg_counts in accumulator.items():
        # 转换为字典形式，键为leg_id，值为计数
        counts_dict = leg_counts.to_dict()
        
        # 确定最大leg_id值（至少统计到6）
        if counts_dict:
            max_leg = max(6, max(counts_dict.keys()))
        else:
            max_leg = 6
        
        # 初始化计数数组（索引0不使用，从1开始）
        counts = [0] * (max_leg + 1)
        for i in range(1, max_leg + 1):
            counts[i] = counts_dict.get(i, 0)
        
        # 计算各transfer类型的journey_num
        for transfer_type in range(0, max_leg - 1):
            journey_num = counts[transfer_type + 1] - counts[transfer_type + 2]

            # 仅添加journey_num非负的记录
            if journey_num >= 0:
                results.append({
                    't_date': t_date,
                    'date_type': 'day',
                    'region_id': region_id,
                    'transfer_cnt_type': transfer_type,
                    'journey_num': journey_num,
                    'create_by': 'system',
                    'update_by': 'system',
                    'create_time': current_time,
                    'update_time': current_time
                })
    
    # 转换为DataFrame
    if results:
        result_df = pd.DataFrame(results)
        
        # 转换时间格式
        result_df['update_time'] = pd.to_datetime(result_df['update_time'])
        result_df['create_time'] = pd.to_datetime(result_df['create_time'])
        
        # 格式化时间列为ISO格式
        result_df['update_time'] = result_df['update_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        result_df['create_time'] = result_df['create_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        print("警告：没有生成任何结果记录！")
        # 创建空结果集但包含所有列
        result_df = pd.DataFrame(columns=[
            't_date', 'date_type', 'region_id', 'transfer_cnt_type','journey_num', 
            'create_by', 'update_by', 'create_time', 'update_time'
        ])
    
    return result_df

def process_and_export(accumulator, output_path):
    """
    处理累加的数据并导出CSV文件
    """
    
    # 生成最终结果
    result_df = generate_final_result(accumulator)
    
    # 导出CSV
    result_df.to_csv(output_path, index=False)
    
    return result_df

# 主函数
def main():
    # Configuration
    folder = "../data/input/VDV_data"
    output_file = '../data/output/ads_passenger_transfer_count_di0811.csv'
    afc_file = '../data/input/2025_H1_afc_data_endstop_imputed.parquet' 
    # 初始化累加器
    accumulator = {}
    
    # 一次性加载VDV数据（数据量小）
    print("Loading VDV LINE data...")
    vdv_line_data = read_line_data(folder, "i2261280.x10")
    print(f"读取到 {len(vdv_line_data)} 条记录")

    print("Load VDV OPERATING_DEPARTMENT data...")
    vdv_op_dept_data = read_operating_department(folder, "i3331280.x10")
    print(f"读取到 {len(vdv_op_dept_data)} 条记录")
    
    # 添加区域映射
    vdv_op_dept_data['region_id'] = vdv_op_dept_data['OP_DEP_ABBR'].apply(map_region_id)
    
    # 分块处理AFC数据
    print("Processing AFC data in chunks...")
    chunk_count = 0
    for chunk_df in read_afc_data_parquet(afc_file, chunksize=500000):
        chunk_count += 1
        print(f"Processing chunk {chunk_count} with {len(chunk_df)} records...")
        
        # 合并当前块的数据
        merged_chunk = merge_data(chunk_df, vdv_line_data, vdv_op_dept_data)
        
        # 累加当前块的计数
        accumulator = accumulate_leg_counts(merged_chunk, accumulator)
    
    # 处理并导出最终结果
    print("Generating final result...")
    result = process_and_export(accumulator, output_file)
    print(f"Processing complete and results exported to {output_file}")
    print(f"Total records processed: {sum(len(counts) for counts in accumulator.values())}")

if __name__ == "__main__":
    main()