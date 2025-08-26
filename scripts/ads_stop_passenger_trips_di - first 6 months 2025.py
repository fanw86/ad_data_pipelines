import pyarrow.dataset as ds
import pandas as pd


# 1 每1/4个月读取数据
def process_quarter_month_stop_trips(parquet_path, month_str, part, AVM):
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
    filter_expr = (ds.field("start_time") >= start_date) & (ds.field("start_time") <= end_date)
    table = dataset.to_table(filter=filter_expr)
    AFC = table.to_pandas()

# 2 提取所有站点station_no
    AFC['date'] = pd.to_datetime(AFC['start_time']).dt.date
    AFC_dropped = AFC[(AFC['start_station_no'] != '-') & (AFC['end_station_no'] != '-')]

    # 所有出现过的站点
    start_pairs = AFC_dropped[['date', 'start_station_no']].rename(columns={'start_station_no': 'station_no'}).drop_duplicates()
    end_pairs = AFC_dropped[['date', 'end_station_no']].rename(columns={'end_station_no': 'station_no'}).drop_duplicates()
    station_date_pairs = pd.concat([start_pairs, end_pairs]).drop_duplicates()

# 3 计算一天内，作为start_station_no出现的次数（trip_board_num）
    trip_board_counts = AFC_dropped.groupby(['date', 'start_station_no']).size().reset_index(name='trip_board_num')
    trip_board_counts = trip_board_counts.rename(columns={'start_station_no': 'station_no'})

# 4 计算一天内，作为end_station_no出现的次数（trip_alight_num）
    trip_alight_counts = AFC_dropped.groupby(['date', 'end_station_no']).size().reset_index(name='trip_alight_num')
    trip_alight_counts = trip_alight_counts.rename(columns={'end_station_no': 'station_no'})

# 5 计算一天内，作为一个journey_id的leg=1的时候的start_station_no出现的次数（journey_board_num）
    leg1_df = AFC_dropped[AFC_dropped['leg_id'] == 1].copy()
    leg1_df['start_station_no'] = leg1_df['start_station_no'].fillna(-1).astype(int)
    journey_board_counts = leg1_df.groupby(['date', 'start_station_no']).size().reset_index(name='journey_board_num')
    journey_board_counts = journey_board_counts.rename(columns={'start_station_no': 'station_no'})

# 6 计算一天内，作为连续journeyid的最后一个leg时候的end_station_no出现的次数（journey_alight_num）
    AFC_endstation_nonull = AFC_dropped[AFC_dropped['end_station_no'] != '-'].copy()
    AFC_endstation_nonull = AFC_endstation_nonull.dropna(subset=['start_station_no','end_station_no'])
    AFC_endstation_nonull['end_station_no'] = AFC_endstation_nonull['end_station_no'].astype(int)
    # 标记每个乘客（uid）每个 journey 的最后一段 leg
    AFC_endstation_nonull['rank'] = AFC_endstation_nonull.groupby(['uid', 'journey_id'])['start_time'].rank(method='first', ascending=False)
    AFC_endstation_nonull['is_last_leg'] = AFC_endstation_nonull['rank'] == 1
    # 提取每个 journey 的 alight leg（下车站）
    last_legs = AFC_endstation_nonull[AFC_endstation_nonull['is_last_leg']].copy()
    # 按日期 + end_station_no 聚合下车人数
    journey_alight_counts = (
        last_legs
        .groupby(['date', 'end_station_no'])
        .size()
        .reset_index(name='journey_alight_num')
    )
    # 整理字段名
    journey_alight_counts = journey_alight_counts.rename(columns={'end_station_no': 'station_no'})

# 7 merge 合并所有指标
    result = station_date_pairs.merge(trip_board_counts, on=['date', 'station_no'], how='left') \
                               .merge(trip_alight_counts, on=['date', 'station_no'], how='left') \
                               .merge(journey_board_counts, on=['date', 'station_no'], how='left') \
                               .merge(journey_alight_counts, on=['date', 'station_no'], how='left') \
                               .fillna(0)

    # merge AVM 的日期类型
    AVM['date'] = pd.to_datetime(AVM['OPD_DATE']).dt.date
    result = result.merge(AVM[['date', 'DAY_TYPE']].drop_duplicates(subset=['date']), on='date', how='left')

# 8 构建ads_stop_passenger_trips_di
    now_time = pd.Timestamp.now()
    result['t_date'] = pd.to_datetime(result['date'], errors='coerce')
    result = result.drop_duplicates(subset=['t_date', 'station_no'])

    ads_stop_passenger_trips_di = pd.DataFrame({
        'stop_no': result['station_no'].astype(int),
        't_date': result['t_date'].dt.strftime('%Y-%m-%d'),
        'date_type': 'day',
        'trip_board_num': result['trip_board_num'].astype(int),
        'trip_alight_num': result['trip_alight_num'].astype(int),
        'journey_board_num': result['journey_board_num'].astype(int),
        'journey_alight_num': result['journey_alight_num'].astype(int),
        'create_time': now_time,
        'create_by': 'Ejane',
        'update_time': now_time,
        'update_by': 'Ejane'
    })

    return ads_stop_passenger_trips_di


# 9 主控程序，输出现在处理进度
parquet_path = 'E:/SUTPC/阿布扎比公交/2025_H1_afc_data_endstop_imputed/2025_H1_afc_data_endstop_imputed.parquet'
months = ['2025-01', '2025-02', '2025-03', '2025-04', '2025-05', '2025-06']
AVM = pd.read_csv('E:/SUTPC/阿布扎比公交/样例数据/1.AVM_Data/avm_trips_20250115-17/export.csv')

all_stop_data = []
for month in months:
    for part in range(1, 5):
        print(f'Processing {month} - Part {part}/4')
        part_df = process_quarter_month_stop_trips(parquet_path, month, part, AVM)
        all_stop_data.append(part_df)


# 10 主控程序，输出现在处理进度
final_df = pd.concat(all_stop_data, ignore_index=True)
print(final_df )
final_df.to_csv('E:/SUTPC/阿布扎比公交/0807/ads_stop_passenger_trips_di.csv', index=False)