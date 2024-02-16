import csv
import pytz
import datetime

#--------------------------------------------------------------------------------
# 現在日時文字列
#--------------------------------------------------------------------------------
def get_dt_str():
    tz = pytz.timezone('Asia/Tokyo')
    return datetime.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S.%f")

#--------------------------------------------------------------------------------
# 現在日時付きprint出力
#--------------------------------------------------------------------------------
def print_ex(message):
    print(get_dt_str() + ',' + message)

#--------------------------------------------------------------------------------
# CSVファイル→二次元配列
#--------------------------------------------------------------------------------
def csv_to_array(filepath):

    try:
        with open(filepath, mode='r', encoding='utf-8', newline='') as f:
            reader = csv.reader(f)

            data = []
            for i, row in enumerate(reader):
                if i > 0:
                    data.append(row)

    except Exception as e:
        print_ex(f'エラー発生: {e}')
        return None
    
    return data

