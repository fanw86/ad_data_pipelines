import pyarrow.dataset as ds
import pandas as pd


# 1 读取数据
# 2 每1/4个月读取数据
# 3 trip_num 每条线路每天的记录数
# 4 leg1_num 每条线路每天 leg_id == 1 的数量
# 5 merge OP_DEP_NO
# 6 merge service_type
# 7 构建 service_type 列
# 8 聚合
# 9 构造目标表
# 10 主控程序，输出现在处理进度
# 11 拼接所有数据


# 1 读取数据
df_servicetype = pd.read_excel('E:/SUTPC/阿布扎比公交/样例数据/Service Type category for routes.xlsx')
def read_x10_table(filepath, table_name='LINE'):
    with open(filepath, 'r', encoding='ISO-8859-1') as f:
        lines = f.readlines()

    # 准备变量
    in_table = False
    col_names = []
    col_types = []
    records = []

    for i, line in enumerate(lines):
        line = line.strip()

        if line.startswith(f"tbl; {table_name}"):
            in_table = True
            continue

        if in_table:
            if line.startswith("atr;"):
                col_names = [x.strip() for x in line.split(';')[1:]]
            elif line.startswith("frm;"):
                col_types = [x.strip() for x in line.split(';')[1:]]
            elif line.startswith("rec;"):
                raw = line.split(';')[1:]
                parsed = [x.strip().strip('"') for x in raw]
                records.append(parsed)
            elif line.startswith("tbl;") and not line.startswith(f"tbl; {table_name}"):
                # 遇到下一个表，结束当前表
                break

    # 构建 DataFrame
    df = pd.DataFrame(records, columns=col_names)
    return df

df_line = read_x10_table('E:/SUTPC/阿布扎比公交/样例数据/7.DIVA VDV Data/VDV_data_AD_og/i2263440.x10', table_name='LINE')

# 2 每1/4个月读取数据
def process_quarter_month_ridership(parquet_path, month_str, part, df_line, df_servicetype):
    import pyarrow.dataset as ds

    # 划分日期范围
    month_start = pd.to_datetime(month_str + '-01')
    month_end = month_start + pd.offsets.MonthEnd(0)
    end_day = month_end.day

    if part == 1:
        start_date = f'{month_str}-01'
        end_date = f'{month_str}-08'
    elif part == 2:
        start_date = f'{month_str}-08'
        end_date = f'{month_str}-15'
    elif part == 3:
        start_date = f'{month_str}-15'
        end_date = f'{month_str}-22'
    elif part == 4:
        start_date = f'{month_str}-22'
        month_end_plus1 = month_end + pd.Timedelta(days=1)
        end_date = month_end_plus1.strftime("%Y-%m-%d")
    else:
        raise ValueError("part must be 1~4")

    # 读取 parquet 中该时间段数据
    dataset = ds.dataset(parquet_path, format="parquet")
    table = dataset.to_table(filter=(ds.field("start_time") >= start_date) & (ds.field("start_time") <= end_date))
    AFC = table.to_pandas()
    AFC['date'] = pd.to_datetime(AFC['start_time']).dt.date

# 3 trip_num 每条线路每天的记录数
    trip_counts = AFC.groupby(['date', 'route']).size().reset_index(name='trip_num')

# 4 leg1_num 每条线路每天 leg_id == 1 的数量
    leg1_counts = (
        AFC[AFC['leg_id'] == 1]
        .groupby(['date', 'route'])
        .size()
        .reset_index(name='leg1_num')
    )

    route_summary = trip_counts.merge(leg1_counts, on=['date', 'route'], how='left')
    route_summary['leg1_num'] = route_summary['leg1_num'].fillna(0).astype(int)
    route_summary['transfer_num'] = route_summary['trip_num'] - route_summary['leg1_num']

# 5 merge OP_DEP_NO
    route_summary['route'] = route_summary['route'].astype(str)
    df_line['LINE_ABBR'] = df_line['LINE_ABBR'].astype(str)
    df_line_subset = df_line[['LINE_ABBR', 'OP_DEP_NO']].drop_duplicates()

    route_summary_with_opdep = route_summary.merge(
        df_line_subset,
        left_on='route',
        right_on='LINE_ABBR',
        how='left'
    ).drop(columns=['LINE_ABBR'])

# 6 merge service_type
    df_servicetype['Route'] = df_servicetype['Route'].astype(str)
    merged = route_summary_with_opdep.merge(
        df_servicetype,
        left_on='route',
        right_on='Route',
        how='left'
    ).drop(columns=['Route'])

    # 手动处理 ADL 特殊情况
    merged.loc[merged['route'] == 'ADL', 'Region'] = 'Abu Dhabi'
    merged.loc[merged['route'] == 'ADL', 'Service Type'] = 'Link'
    merged = merged[merged['route'] != '-']

# 7 构建 service_type 列
    def get_service_type(row):
        r, s = row['Region'], row['Service Type']
        if r == 'Abu Dhabi' and s == 'Local':
            return 'AD Local'
        elif r == 'Abu Dhabi' and s == 'Regional':
            return 'AD Regional'
        elif r == 'Abu Dhabi' and s == 'Link':
            return 'AD Link'
        elif r == 'Al Ain' and s == 'Local':
            return 'AA Local'
        elif r == 'Al Ain' and s == 'Regional':
            return 'AA Regional'
        elif r == 'Al Dhafra' and s == 'Local':
            return 'DH Local'
        elif r == 'Al Dhafra' and s == 'Regional':
            return 'DH Regional'
        else:
            return 'Unknown'

    merged['service_type'] = merged.apply(get_service_type, axis=1)

# 8 聚合
    service_type_summary = (
        merged
        .groupby(['service_type', 'Region', 'date'], as_index=False)
        [['trip_num', 'leg1_num', 'transfer_num']]
        .sum()
    )

# 9 构造目标表
    now_time = pd.Timestamp.now()
    result_df = pd.DataFrame({
        'service_type': service_type_summary['service_type'],
        'region_id': service_type_summary['Region'],
        't_date': pd.to_datetime(service_type_summary['date']),
        'date_type': 'day',
        'passenger_trips_num': service_type_summary['trip_num'],
        'journey_num': service_type_summary['leg1_num'],
        'transfer_num': service_type_summary['transfer_num'],
        'update_time': now_time,
        'update_by': 'Ejane',
        'create_time': now_time,
        'create_by': 'Ejane'
    })

    result_df['t_date'] = result_df['t_date'].dt.strftime('%Y-%m-%d')
    return result_df

 # 10 主控程序，输出现在处理进度
parquet_file_path = 'E:/SUTPC/阿布扎比公交/2025_H1_afc_data_endstop_imputed/2025_H1_afc_data_endstop_imputed.parquet'
months = ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06']
all_ridership_data = []

for month in months:
    for part in range(1, 5):
        print(f"Processing ridership {month} - Part {part}/4")
        df_part = process_quarter_month_ridership(parquet_file_path, month, part, df_line, df_servicetype)
        all_ridership_data.append(df_part)


# 11 拼接所有数据
ads_ridership_passenger_trips_di = pd.concat(all_ridership_data).drop_duplicates()
ads_ridership_passenger_trips_di = ads_ridership_passenger_trips_di.reset_index(drop=True)

ads_ridership_passenger_trips_di.to_csv('E:/SUTPC/阿布扎比公交/0807/ads_ridership_passenger_trips_di.csv', index=False)