#--------------------------------------------------------------------------------
# 毎日の店舗分析データ更新処理
#--------------------------------------------------------------------------------
# ① 注文実績を追加
# 　　・注文実績取得
# 　　・差分を取得してスプレッドシートに追加
# ② 出品データ更新
# 　　・減った分には削除フラグを付ける
# 　　・増えた分には出品データに追加
# ③ 増加した出品データ詳細を取得
# ④ カテゴリ抽出し市場データを取得
#--------------------------------------------------------------------------------
from scraping import *
from datetime import datetime, timedelta

def get_data_update():

    print_ex('出品データ更新 処理開始')

    try:
        # 外注管理リスト取得
        url_list = get_management_info()

        # 注文実績取得
        for i, row in enumerate(url_list):        

            url_sheet = row['sheet_url']    # スプレッドシートURL
            url_order = row['order_url']    # 注文実績URL
            url_item = row['item_url']      # 出品データURL
            state = row['state']            # 状態
            ss_name = row['sheet_name']     # スプレッドシート名

            set_ss_value(url_sheet, "操作", 13, 3, get_dt_str())

            # 状態が未取得, 取得中, エラーの場合に処理する
            if (state != GetDataStep.INIT_DONE.value and state != GetDataStep.UPDATE_ERROR.value and
                state != GetDataStep.UPDATE_DONE.value and
                state != GetDataStep.UPDATE_RUN_ORDER.value and state != GetDataStep.UPDATE_RUN_LIST.value and
                state != GetDataStep.UPDATE_RUN_ITEM.value and state != GetDataStep.UPDATE_RUN_MARKET.value):
                continue

            #--------------------------------------------------------------------------------
            # ① 注文実績を追加
            #--------------------------------------------------------------------------------
            print_ex('[1]注文実績更新 開始 ' + ss_name)
            set_ss_value(MANAGE_SS_URL, MANAGE_SS_NAME, MANAGE_ROW_START + i, MANAGE_COL_STATE, GetDataStep.UPDATE_RUN_ORDER.value)
            set_ss_value(url_sheet, "操作", 12, 3, GetDataStep.UPDATE_RUN_ORDER.value)

            # 注文実績→CSVファイル
            if get_order_data(get_dt_str(), url_order) == False:
                set_ss_value(MANAGE_SS_URL, MANAGE_SS_NAME, MANAGE_ROW_START + i, MANAGE_COL_STATE, GetDataStep.UPDATE_ERROR.value)
                set_ss_value(url_sheet, "操作", 12, 3, GetDataStep.UPDATE_ERROR.value)
                continue

            # 前回取得データの最新日時を取得
            data_latest = get_ss_all_values(url_sheet, ORDER_SHEET_NAME)

            if len(data_latest[0]) > 1:
                data_latest = sorted(data_latest[1:], key=sort_key)
                dt_latest = data_latest[-1][6]
                dt_latest = datetime.strptime(dt_latest, '%Y/%m/%d').date()
            
            # CSVファイルから読込み
            data = csv_to_array(FILE_PATH_ORDER)
            data_sorted = sorted(data, key=sort_key)

            # 追加するデータを特定
            tz = pytz.timezone('Asia/Tokyo')
            dt_today = datetime.now(tz).date()
            dt_yesterday = dt_today - timedelta(days=1)

            no_start = len(data_latest) + 1
            for j, r in enumerate(data_sorted):
                date = datetime.strptime(r[6].split()[0], '%Y/%m/%d').date()
                if dt_latest < date and date <= dt_yesterday:
                    r[0] = no_start     # No.を続きから振る
                    no_start += 1
                    data_latest.append(r)

            # スプレッドシートへ書込み
            set_ss_all_values(url_sheet, ORDER_SHEET_NAME, data_latest)

            print_ex('[1]注文実績更新 終了 ' + ss_name)

            #--------------------------------------------------------------------------------
            # ② 出品データ更新
            #--------------------------------------------------------------------------------
            print_ex('[2]出品データ更新 開始 ' + ss_name)
            set_ss_value(MANAGE_SS_URL, MANAGE_SS_NAME, MANAGE_ROW_START + i, MANAGE_COL_STATE, GetDataStep.UPDATE_RUN_LIST.value)
            set_ss_value(url_sheet, "操作", 12, 3, GetDataStep.UPDATE_RUN_LIST.value)

            # 差分比較用リスト取得
            get_item_list(url_item, FILE_PATH_LIST, ITEM_COLS)
            df_list = pd.read_csv(FILE_PATH_LIST)

            # 出品データ取得
            data = get_ss_all_values(url_sheet, '出品データ')
            data_cols = data[0]
            data_rows = data[1:]
            df_item = pd.DataFrame(data_rows, columns=data_cols)

            # IDをキーにマージ
            df_list['ID'] = df_list['ID'].astype(str)
            df_item['ID'] = df_item['ID'].astype(str)
            merged_df = pd.merge(df_list, df_item, on='ID', how='outer', indicator=True)

            # 新たに増えた商品データを商品リストに追加
            df_list_only = merged_df[merged_df['_merge'] == 'left_only']
            df_list_only_ids = df_list_only['ID'].tolist()
            df_add_rows = df_list[df_list['ID'].isin(df_list_only_ids)]
            df_add_rows['No.'] = range(len(df_item) + 1, len(df_item) + 1 + len(df_add_rows))   # No.を続きから振る
            df_item = pd.concat([df_item, df_add_rows], ignore_index=True)
            df_item.loc[df_item['ID'].isin(df_list_only_ids), '出品'] = '未取得'
            df_item.to_csv('list_inc.csv', index=False)

            # 減った商品データに削除フラグを付ける
            df_item_only = merged_df[merged_df['_merge'] == 'right_only']
            df_item_only_ids = df_item_only['ID'].tolist()
            df_item.loc[df_item['ID'].isin(df_item_only_ids), '出品'] = '削除'
            df_item.to_csv('list_dec.csv', index=False)

            # スプレッドシートへ書込み
            df_item.to_csv(FILE_PATH_ITEM, index=False)
            data = csv_to_array(FILE_PATH_ITEM)
            set_ss_all_values(url_sheet, ITEM_SHEET_NAME, data)

            print_ex('[2]出品データ更新 終了 ' + ss_name)

            #--------------------------------------------------------------------------------
            # ③ 増加した出品データ詳細を取得
            #--------------------------------------------------------------------------------
            print_ex('[3]増加した出品データ取得 開始 ' + ss_name)
            set_ss_value(MANAGE_SS_URL, MANAGE_SS_NAME, MANAGE_ROW_START + i, MANAGE_COL_STATE, GetDataStep.UPDATE_RUN_ITEM.value)
            set_ss_value(url_sheet, "操作", 12, 3, GetDataStep.UPDATE_RUN_ITEM.value)

            # 商品データ(詳細)取得
            result = False
            while not result:
                result = get_item_detail(url_sheet)
                print_ex( '[3]出品データ初回取得(詳細) 処理結果 ' + ('成功' if result else '失敗'))

            data = csv_to_array(FILE_PATH_ITEM)
            set_ss_all_values(url_sheet, ITEM_SHEET_NAME, data)
                
            print_ex('[3]増加した出品データ取得 終了 ' + ss_name)

            #--------------------------------------------------------------------------------
            # ④ カテゴリ抽出し市場データを取得
            #--------------------------------------------------------------------------------
            print_ex('[4]市場データ更新 開始 ' + ss_name)
            set_ss_value(MANAGE_SS_URL, MANAGE_SS_NAME, MANAGE_ROW_START + i, MANAGE_COL_STATE, GetDataStep.UPDATE_RUN_MARKET.value)
            set_ss_value(url_sheet, "操作", 12, 3, GetDataStep.UPDATE_RUN_MARKET.value)


            # 出品データ→市場データ用リスト
            item_to_market(url_sheet)

            # 市場データの出品数取得
            error_count = 0
            result = False
            while not result:
                result = get_market_data()
                print_ex( '[4]市場データ初回取得処理 結果 ' + ('成功' if result else '失敗'))
                
                if result:
                    error_count = 0
                else:
                    error_count += 1
                
                if error_count >= MARKET_MAX_RETRY:
                    raise

            # スプレッドシート出力
            data = csv_to_array(MARKET_PATH_ORDER)
            set_ss_all_values(url_sheet, MARKET_SHEET_NAME, data)

            print_ex('[4]市場データ更新 終了 ' + ss_name)

    except Exception as e:
        print_ex(f'エラー発生: {e}')
        set_ss_value(MANAGE_SS_URL, MANAGE_SS_NAME, MANAGE_ROW_START + i, MANAGE_COL_STATE, GetDataStep.UPDATE_ERROR.value)
        set_ss_value(url_sheet, "操作", 12, 3, GetDataStep.UPDATE_ERROR.value)
        raise

    set_ss_value(MANAGE_SS_URL, MANAGE_SS_NAME, MANAGE_ROW_START + i, MANAGE_COL_STATE, GetDataStep.UPDATE_DONE.value)
    set_ss_value(url_sheet, "操作", 12, 3, GetDataStep.UPDATE_DONE.value)
    set_ss_value(MANAGE_SS_URL, MANAGE_SS_NAME, MANAGE_ROW_START + i, MANAGE_COL_DATE, get_dt_str())
    set_ss_value(url_sheet, "操作", 14, 3, get_dt_str())
    print_ex('出品データ更新 処理終了')
    
    return

def main():
    get_data_update()
    return

if __name__ == "__main__":
    main()