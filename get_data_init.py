#--------------------------------------------------------------------------------
# 初回データ取得処理
#--------------------------------------------------------------------------------
# ① 注文実績を取得する(最大900件)
# ② 出品データのリストを取得
# ③ 出品停止リスト追加
# ④ 出品データの詳細データを取得
# ⑤ カテゴリ抽出し市場データを取得する
#--------------------------------------------------------------------------------
from scraping import *
from datetime import datetime, timedelta
import pytz

def get_data_init():

    print_ex('初回データ取得 処理開始')
    
    try:
        # 外注管理リスト取得
        url_list = get_management_info()

        for i, row in enumerate(url_list):

            url_sheet = row['sheet_url']    # スプレッドシートURL
            url_order = row['order_url']    # 注文実績URL
            url_item = row['item_url']      # 出品データURL
            state = row['state']            # 状態
            ss_name = row['sheet_name']     # スプレッドシート名

            set_ss_value(url_sheet, "操作", 13, 3, get_dt_str())

            # 状態が未取得, 取得中, エラーの場合に処理する
            if (state != GetDataStep.INIT_NONE.value and state != GetDataStep.INIT_ERROR.value and
                state != GetDataStep.INIT_RUN_ORDER.value and state != GetDataStep.INIT_RUN_LIST.value and
                state != GetDataStep.INIT_RUN_ITEM.value and state != GetDataStep.INIT_RUN_MARKET.value):
                print_ex('処理不要なのでスキップ state=' + state)
                continue

            #--------------------------------------------------------------------------------
            # ① 注文実績を取得する(最大900件)
            #--------------------------------------------------------------------------------
            print_ex('[1]注文実績初回取得 開始 ' + ss_name)
            set_ss_value(MANAGE_SS_URL, MANAGE_SS_NAME, MANAGE_ROW_START + i, MANAGE_COL_STATE, GetDataStep.INIT_RUN_ORDER.value)
            set_ss_value(url_sheet, "操作", 12, 3, GetDataStep.INIT_RUN_ORDER.value)

            # 注文実績→CSVファイル
            if get_order_data(get_dt_str(), url_order) == False:
                set_ss_value(MANAGE_SS_URL, MANAGE_SS_NAME, MANAGE_ROW_START + i, MANAGE_COL_STATE, GetDataStep.INIT_ERROR.value)
                set_ss_value(url_sheet, "操作", 12, 3, GetDataStep.INIT_ERROR.value)
                continue
            
            # CSVファイルから読込み
            data = csv_to_array(FILE_PATH_ORDER)
            data_sorted = sorted(data, key=sort_key)

            # 前日までのデータとする
            tz = pytz.timezone('Asia/Tokyo')
            dt_today = datetime.now(tz).date()
            dt_yesterday = dt_today - timedelta(days=1)            
            
            data_filtered = []
            for r in data_sorted:
                date = datetime.strptime(r[6].split()[0], '%Y/%m/%d').date()
                if date <= dt_yesterday:
                    data_filtered.append(r)

            # No.列を振り直し
            for j, r in enumerate(data_filtered):
                r[0] = str(j + 1)
            
            # スプレッドシートへ書込み
            set_ss_all_values(url_sheet, ORDER_SHEET_NAME, data_filtered)

            print_ex('[1]注文実績初回取得 終了 ' + ss_name)

            #--------------------------------------------------------------------------------
            # ② 出品データのリストを取得
            #--------------------------------------------------------------------------------
            print_ex('[2]出品データ初回取得(リスト) 開始 ' + ss_name)
            set_ss_value(MANAGE_SS_URL, MANAGE_SS_NAME, MANAGE_ROW_START + i, MANAGE_COL_STATE, GetDataStep.INIT_RUN_LIST.value)
            set_ss_value(url_sheet, "操作", 12, 3, GetDataStep.INIT_RUN_LIST.value)

            # 商品データ(リスト)取得
            get_item_list(url_item, FILE_PATH_ITEM, ITEM_COLS)

            # スプレッドシート出力
            data = csv_to_array(FILE_PATH_ITEM)
            set_ss_all_values(url_sheet, ITEM_SHEET_NAME, data)

            print_ex('[2]出品データ初回取得(リスト) 終了 ' + ss_name)

            #--------------------------------------------------------------------------------
            # ③ 出品停止リスト追加
            #--------------------------------------------------------------------------------
            print_ex('[3]出品停止リスト追加 開始 ' + ss_name)

            # 注文実績にしか存在しない商品IDを出品データに追加
            get_compare_order_of_item(url_sheet)

            print_ex('[3]出品停止リスト追加 終了 ' + ss_name)

            #--------------------------------------------------------------------------------
            # ④ 出品データの詳細データを取得
            #--------------------------------------------------------------------------------
            print_ex('[4]出品データ初回取得(詳細) 開始 ' + ss_name)
            set_ss_value(MANAGE_SS_URL, MANAGE_SS_NAME, MANAGE_ROW_START + i, MANAGE_COL_STATE, GetDataStep.INIT_RUN_ITEM.value)
            set_ss_value(url_sheet, "操作", 12, 3, GetDataStep.INIT_RUN_ITEM.value)

            # 商品データ(詳細)取得
            result = False
            while not result:
                result = get_item_detail(url_sheet)
                print_ex( '[4]出品データ初回取得(詳細) 処理結果 ' + ('成功' if result else '失敗'))

            # スプレッドシート出力
            data = csv_to_array(FILE_PATH_ITEM)
            set_ss_all_values(url_sheet, ITEM_SHEET_NAME, data)

            print_ex('[4]出品データ初回取得(詳細) 終了 ' + ss_name)

            #--------------------------------------------------------------------------------
            # ⑤ カテゴリ抽出し市場データを取得する
            #--------------------------------------------------------------------------------
            print_ex('[5]市場データ初回取得 開始 ' + ss_name)
            set_ss_value(MANAGE_SS_URL, MANAGE_SS_NAME, MANAGE_ROW_START + i, MANAGE_COL_STATE, GetDataStep.INIT_RUN_MARKET.value)
            set_ss_value(url_sheet, "操作", 12, 3, GetDataStep.INIT_RUN_MARKET.value)

            # 出品データ→市場データ用リスト
            item_to_market(url_sheet)

            # 市場データの出品数取得
            result = False
            while not result:
                result = get_market_data()
                print_ex( '[5]市場データ初回取得処理 結果 ' + ('成功' if result else '失敗'))

                if result:
                    error_count = 0
                else:
                    error_count += 1
                
                if error_count >= MARKET_MAX_RETRY:
                    raise

            # スプレッドシート出力
            data = csv_to_array(MARKET_PATH_ORDER)
            set_ss_all_values(url_sheet, MARKET_SHEET_NAME, data)

            print_ex('[5]市場データ初回取得 終了 ' + ss_name)

    except Exception as e:
        print_ex(f'エラー発生: {e}')
        set_ss_value(MANAGE_SS_URL, MANAGE_SS_NAME, MANAGE_ROW_START + i, MANAGE_COL_STATE, GetDataStep.INIT_ERROR.value)
        set_ss_value(url_sheet, "操作", 12, 3, GetDataStep.INIT_ERROR.value)
        raise

    set_ss_value(MANAGE_SS_URL, MANAGE_SS_NAME, MANAGE_ROW_START + i, MANAGE_COL_STATE, GetDataStep.INIT_DONE.value)
    set_ss_value(url_sheet, "操作", 12, 3, GetDataStep.INIT_DONE.value)
    set_ss_value(MANAGE_SS_URL, MANAGE_SS_NAME, MANAGE_ROW_START + i, MANAGE_COL_DATE, get_dt_str())
    set_ss_value(url_sheet, "操作", 14, 3, get_dt_str())
    print_ex('初回データ取得 終了')

    return


def main():
    get_data_init()


if __name__ == "__main__":
    main()