import pyarrow.parquet as pq
import pandas as pd
import pyarrow.dataset as ds


# 1 读取数据
# 2 每1/4个月读取数据
# 3 在AFC里计算group by 'date','route','tripdir'的passenger_trips_num（数据行数）
# 4 和df_line连接，AFC的route和df_line的LINE_NO
# 5 构建ads_route_passenger_trips_di
# 6 主控程序，输出现在处理进度
# 7 拼接所有的表，有全部数据的总表



# 1 读取数据
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
def process_quarter_month(parquet_path, month_str, part):
    # 日期区间划分
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

    # 用 Arrow dataset 方式按条件读取
    dataset = ds.dataset(parquet_path, format="parquet")
    filter_expr = (
        (ds.field("start_time") >= start_date) &
        (ds.field("start_time") <= end_date)
        )
    table = dataset.to_table(filter=filter_expr)
    AFC = table.to_pandas()

# 3 在AFC里计算group by 'date','route','tripdir'的passenger_trips_num（数据行数）
    AFC['date'] = pd.to_datetime(AFC['start_time']).dt.date
    passenger_trips = (
        AFC
        .groupby(['date', 'route', 'tripdir'])
        .size()
        .reset_index(name='passenger_trips_num')
    )

# 4 和df_line连接，AFC的route和df_line的LINE_NO
    passenger_trips['route'] = passenger_trips['route'].astype(str)
    passenger_trips_with_lineinfo = passenger_trips.merge(
        df_line,
        left_on='route',
        right_on='LINE_ABBR',
        how='left'
    ).drop_duplicates(subset=['date', 'route', 'tripdir'])

# 5 构建ads_route_passenger_trips_di
    now_time = pd.Timestamp.now()
    result_df = pd.DataFrame({
        'line_no': passenger_trips_with_lineinfo['LINE_NO'],
        'route_no': '',
        'direction': passenger_trips_with_lineinfo['tripdir'].astype(int),
        't_date': pd.to_datetime(passenger_trips_with_lineinfo['date'], errors='coerce').dt.strftime('%Y-%m-%d'),
        'date_type': 'day',
        'passenger_trips_num': passenger_trips_with_lineinfo['passenger_trips_num'],
        'create_time': now_time,
        'create_by': 'Ejane',
        'update_time': now_time,
        'update_by': 'Ejane'
    })

    return result_df



# 6 主控程序，输出现在处理进度
parquet_file_path = 'E:/SUTPC/阿布扎比公交/2025_H1_afc_data_endstop_imputed/2025_H1_afc_data_endstop_imputed.parquet'  # ← 替换为你的路径
months = ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06']
all_data = []

for month in months:
    for part in range(1, 5):
        print(f"Processing {month} - Part {part}/4")
        partial = process_quarter_month(parquet_file_path, month, part)
        all_data.append(partial)

# 7 拼接所有的表，有全部数据的总表
final_result = pd.concat(all_data, ignore_index=True)
print(final_result)
final_result.to_csv('E:/SUTPC/阿布扎比公交/0807/ads_route_passenger_trips_di.csv', index=False)