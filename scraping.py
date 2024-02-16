import os
import re
import math
import pandas as pd
from datetime import datetime
from selenium.webdriver.common.by import By
from tool import *
from google_api import *
from web_driver import *
from enum import Enum

# パス関連
if os.getenv('GITHUB_ACTIONS') == 'true':
    base_dir = os.getenv('GITHUB_WORKSPACE')
else:
    base_dir = os.path.abspath(os.curdir)

#--------------------------------------------------------------------------------
# 処理状態
#--------------------------------------------------------------------------------
class GetDataStep(Enum):
    INIT_NONE = '初回データ取得予約'
    INIT_RUN_ORDER = '初回取得中(注文実績)'
    INIT_RUN_LIST = '初回取得中(出品リスト)'
    INIT_RUN_ITEM = '初回取得中(出品データ)'
    INIT_RUN_MARKET = '初回取得中(市場データ)'
    INIT_DONE = '初回取得済'
    INIT_ERROR = '初回取得エラー'
    UPDATE_RUN_ORDER = '更新中(注文実績)'
    UPDATE_RUN_LIST = '更新中(出品リスト)'
    UPDATE_RUN_ITEM = '更新中(出品データ)'
    UPDATE_RUN_MARKET = '更新中(市場データ)'
    UPDATE_DONE = '更新済'
    UPDATE_ERROR = '更新エラー'

#--------------------------------------------------------------------------------
# 外注総合管理ツール
#--------------------------------------------------------------------------------
#MANAGE_SS_URL = 'https://docs.google.com/spreadsheets/d/1MLI2MJuwj42959gcM9JYm7a44hQ1Zy2LNkbgQrQV0h0/'
MANAGE_SS_URL = 'https://docs.google.com/spreadsheets/d/1xPH9HHsLavqFE4G23Y3kfASYqKIH6SI336xJEpOHybY/'
MANAGE_SS_NAME = 'シート1'

MANAGE_ROW_START = 5
MANAGE_COL_STATE = 5
MANAGE_COL_DATE = 6

MANAGE_COLS = [
        'sheet_url',    # スプレッドURL
        'sheet_name',   # シート名
        'create_date',  # 作成日
        'shop_url',     # ライバルURL
        'order_url',    # 注文実績URL
        'item_url'      # 商品URL
        'state'         # 状態
        'update'        # 更新日時
    ]

#--------------------------------------------------------------------------------
# 出品データリスト(差分比較用)
#--------------------------------------------------------------------------------
LIST_ROW_START = 2              # スプレッドシートの開始行
ITEM_COUNT_PER_PAGE = 80        # 1ページ当たりの商品数

# 出品データCSVファイル
FILE_LIST_CSV = 'list.csv'
FILE_PATH_LIST = os.path.join(base_dir, FILE_LIST_CSV)

# スプレッドシートの列
LIST_COLS = ['No.', 'ID', '出品', '取得日時', '商品名', '商品URL', '出品日']

#--------------------------------------------------------------------------------
# 出品データ
#--------------------------------------------------------------------------------
ITEM_SHEET_NAME = '出品データ'
ITEM_ROW_START = 2              # スプレッドシートの開始行

# 出品データCSVファイル
FILE_ITEM_CSV = 'item.csv'
FILE_PATH_ITEM = os.path.join(base_dir, FILE_ITEM_CSV)

# スプレッドシートの列
ITEM_COLS = ['No.', 'ID', '出品', '取得日時', '商品名', '商品URL', '出品日',
            'ブランド', 'ブランド1', 'ブランド1 URL', 'ブランド2', 'ブランド2 URL', 'ブランド3', 'ブランド3 URL',
            'カテゴリ1', 'カテゴリ2', 'カテゴリ3', 'ブランド x カテゴリ3', '価格', 'タグ1', 'タグ2',
            'お問い合わせ', 'アクセス', 'お気に入りアイテム登録', '買付地', '買付先名', '発送地']

#--------------------------------------------------------------------------------
# 注文実績データ
#--------------------------------------------------------------------------------
ORDER_SHEET_NAME = '注文実績'
ORDER_MAX_PAGES = 30

# 注文実績データCSVファイル
FILE_ORDER_CSV = 'order.csv'
FILE_PATH_ORDER = os.path.join(base_dir, FILE_ORDER_CSV)

# スプレッドシートの列
ORDER_COLS = ['No.', '取得日時', '商品名', '商品URL', '商品ID', '出品日', '成約日']

# ソートキー(成約日=昇順 and No.=昇順)
def sort_key(row):
    sale_date = datetime.datetime.strptime(row[6], '%Y/%m/%d')
    no = int(row[0])
    return (sale_date, no)

#--------------------------------------------------------------------------------
# 市場データ
#--------------------------------------------------------------------------------
MARKET_SHEET_NAME = '市場データ'
MARKET_MAX_PAGES = 30
MARKET_MAX_RETRY = 5

# 注文実績データCSVファイル
MARKET_ORDER_CSV = 'market.csv'
MARKET_PATH_ORDER = os.path.join(base_dir, MARKET_ORDER_CSV)

# スプレッドシートの列
MARKET_COLS = ['No.', 'ブランド', 'ブランドURL', '出品数']

#--------------------------------------------------------------------------------
# 外注管理情報取得
#--------------------------------------------------------------------------------
def get_management_info():

    try:
        print_ex('外注管理情報取得 開始')

        # 管理情報シート取得
        data_all = get_ss_all_values(MANAGE_SS_URL, 'シート1')

        url_list = []

        for i, row in enumerate(data_all):
            if i < MANAGE_ROW_START - 1:
                continue

            data = {}
            for col in MANAGE_COLS:
                data[col] = ''

            # データ取得
            data['sheet_url'] = row[0]      # スプレッドシートURL
            data['sheet_name'] = row[1]     # シート名
            data['create_date'] = row[2]    # 作成日
            data['shop_url'] = row[3]       # ライバルURL
            data['state'] = row[4]          # 状態
            data['update'] = row[5]         # 更新日時

            # 注文実績URL
            shop_id = data['shop_url'].split('/')[-1].split('.html')[0]
            data['order_url'] = f'https://www.buyma.com/buyer/{shop_id}/sales_<% page %>.html'

            # 商品リストURL
            shop_id = data['shop_url'].split('/')[-1].split('.html')[0]
            data['item_url'] = f'https://www.buyma.com/r/-B{shop_id}O2/'

            url_list.append(data)

        print_ex('外注管理情報取得 終了')

    except Exception as e:
        print_ex("エラー発生: " + str(e))
        messege = f'エラーが発生しています。\nログで詳細を確認してください。'

        return None

    return url_list


#--------------------------------------------------------------------------------
# 注文実績取得
#--------------------------------------------------------------------------------
def get_order_data(dt, url):

    print_ex('注文実績データ取得処理 開始')

    try:
        # ChromeDriver
        driver = get_web_driver()
        
        df_data = pd.DataFrame(columns=ORDER_COLS)
        #df_data = df_data.astype(str)
        data_list = []
        count = 0
        
        for i in range(ORDER_MAX_PAGES):
            target_url = url.replace('<% page %>', str(i + 1))

            # URLアクセス
            driver.get(target_url)

            path = '.buyeritemtable_body'
            if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                elems = driver.find_elements(By.CSS_SELECTOR, path)
            else:
                continue
            
            for elem in elems:

                data = {}
                for col in ORDER_COLS:
                    data[col] = ''

                # No.
                count += 1
                data['No.'] = str(count)

                # 取得日時
                data['取得日時'] = str(dt)

                # 商品名
                # 商品URL
                path = '.buyeritem_name a'
                if len(elem.find_elements(By.CSS_SELECTOR, path)) > 0:
                    tmp = elem.find_element(By.CSS_SELECTOR, path).text.strip()
                    data['商品名'] = str(tmp)

                    tmp = elem.find_element(By.CSS_SELECTOR, path).get_attribute('href').strip()
                    data['商品URL'] = str(tmp)
                else:
                    path = '.buyeritem_name'
                    tmp = elem.find_element(By.CSS_SELECTOR, path).text.strip()
                    data['商品名'] = str(tmp)
                    data['商品URL'] = ''

                # 商品ID
                if data['商品URL'] != '':
                    parts = data['商品URL'].split('/')
                    tmp = parts[-2].strip()
                    data['商品ID'] = str(tmp)

                # 画像URL
                # 出品日
                path = '.buyeritemtable_img__wrap img'
                if len(elem.find_elements(By.CSS_SELECTOR, path)) > 0:
                    img_url = elem.find_element(By.CSS_SELECTOR, path).get_attribute('src').strip()

                    if 'nopub' in img_url:
                        data['出品日'] = ''
                    else:
                        parts = img_url.split('/')
                        date_str = parts[5]
                        yy = "20" + date_str[:2]
                        mm = date_str[2:4]
                        dd = date_str[4:6]
                        tmp2 = f"{yy}/{mm}/{dd}"
                        data['出品日'] = str(tmp2)

                # 成約日
                path = '.buyeritemtable_info > p'
                if len(elem.find_elements(By.CSS_SELECTOR, path)) > 0:
                    elems2 = elem.find_elements(By.CSS_SELECTOR, path)
                    for elem2 in elems2:
                        tmp1 = elem2.text
                        if '成約日' in tmp1:
                            tmp2 = re.search(r'\d{4}/\d{2}/\d{2}', tmp1).group()
                            data['成約日'] = str(tmp2)
                
                data_list.append(data)

            df_data = pd.DataFrame(data_list)
            df_data.to_csv(FILE_PATH_ORDER, index=False)

    except Exception as e:
        driver.quit()
        print_ex("エラー発生: " + str(e))
        return False

    driver.quit()
    print_ex('注文実績データ取得処理 終了')

    return True


#--------------------------------------------------------------------------------
# 出品データ取得（リスト）
#--------------------------------------------------------------------------------
def get_item_list(url, file, cols):

    print_ex('出品データ(リスト)取得処理 開始')

    try:
        driver = get_web_driver()

        # ページ数取得のためにアクセス
        driver.get(url)

        # ページ数算出
        path = '#totalitem_num'     # 該当件数
        if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
            tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()
            total_num = int(tmp.replace(',',''))
            total_page = math.ceil(total_num / ITEM_COUNT_PER_PAGE)

        df_data = pd.DataFrame(columns=cols)
        #df_data = df_data.astype(str)
        data_list = []
        count = 0
        
        for i in range(total_page):

            tmp = url[:-3]
            target_url = tmp + "_" + str(i + 1) + '/'

            # URLアクセス
            driver.get(target_url)

            path = '.product_lists li'
            if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                elems = driver.find_elements(By.CSS_SELECTOR, path)
            else:
                continue

            for elem in elems:

                data = {}
                for col in cols:
                    data[col] = ''

                # No.
                count += 1
                data['No.'] = str(count)

                #ID
                tmp = elem.get_attribute('item-id')
                data['ID'] = str(tmp)

                # 取得日時
                data['取得日時'] = get_dt_str()

                #商品名
                #ブランド
                #商品URL
                #カテゴリ1
                #カテゴリ2
                #カテゴリ3
                path = '.product_name a'
                if len(elem.find_elements(By.CSS_SELECTOR, path)) > 0:
                    tmp = elem.find_element(By.CSS_SELECTOR, path).text.strip()
                    data['商品名'] = str(tmp)

                    tmp = elem.find_element(By.CSS_SELECTOR, path).get_attribute('href')
                    data['商品URL'] = str(tmp)

                #画像URL
                #出品日
                path = '.product_img img'
                if len(elem.find_elements(By.CSS_SELECTOR, path)) > 0:
                    img_url = elem.find_element(By.CSS_SELECTOR, path).get_attribute('src').strip()

                    if 'nopub' in img_url:
                        data['出品日'] = ''
                    else:
                        parts = img_url.split('/')
                        date_str = parts[5]
                        yy = "20" + date_str[:2]
                        mm = date_str[2:4]
                        dd = date_str[4:6]
                        tmp2 = f"{yy}/{mm}/{dd}"
                        data['出品日'] = str(tmp2)

                data_list.append(data)

            df_data = pd.DataFrame(data_list)
            df_data.to_csv(file, index=False)

    except Exception as e:
        driver.quit()
        print_ex("エラー発生: " + str(e))
        return False

    driver.quit()
    print_ex('出品データ(リスト)取得処理 終了')

    return True


#--------------------------------------------------------------------------------
# 出品データ取得（詳細）
#--------------------------------------------------------------------------------
def get_item_detail(ss_url):

    print_ex('出品データ(詳細)取得処理 開始')

    try:
        driver = get_web_driver()
        
        #df_data = pd.read_csv(FILE_PATH_ITEM)  #★CSVではなくスプレッドシートから読込み
        #df_data = df_data.astype(str) #これをするとisnull比較ができなくなる'nan'となるため

        # スプレッドシートから出品データ取得
        data = get_ss_all_values(ss_url, '出品データ')
        data_cols = data[0]
        data_rows = data[1:]
        df_data = pd.DataFrame(data_rows, columns=data_cols)

        for i in range(df_data.shape[0]):

            # 既に取得済みならスキップ      # ★ここきかなくなったSSから読むと
            # if pd.isnull(df_data['ブランド'][i]) == False:
            #     continue

            if df_data['出品'][i] == '削除' or df_data['出品'][i] == '出品中':
                continue

            # URLアクセス
            target_url = df_data.loc[i, '商品URL']
            driver.get(target_url)

            no_item = False
            path = '.notfoundSection_txt'
            if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()

                if '申し訳ございません' in tmp:

                    path = '#detail_ttl'
                    if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                        tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()
                        df_data.loc[i, '商品名'] = str(tmp)
                    
                    path = '.n_item_grid li:nth-child(1) a'
                    if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                        driver.find_element(By.CSS_SELECTOR, path).click()
                        no_item = True

            # ブランド
            path = '#s_brand .brand-link'
            if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()
                df_data.loc[i, 'ブランド'] = str(tmp)

            # ブランド1
            path = '#s_brand .detail_txt_list li:nth-child(1)'
            if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()
                df_data.loc[i, 'ブランド1'] = str(tmp)

            # ブランド1 URL
            path = '#s_brand .detail_txt_list li:nth-child(1) a'
            if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                tmp = driver.find_element(By.CSS_SELECTOR, path).get_attribute('href')
                df_data.loc[i, 'ブランド1 URL'] = str(tmp)

            # ブランド2
            path = '#s_brand .detail_txt_list li:nth-child(2)'
            if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()
                df_data.loc[i, 'ブランド2'] = str(tmp)

            # ブランド2 URL
            path = '#s_brand .detail_txt_list li:nth-child(2) a'
            if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                tmp = driver.find_element(By.CSS_SELECTOR, path).get_attribute('href')
                df_data.loc[i, 'ブランド2 URL'] = str(tmp)

            # ブランド3
            path = '#s_brand .detail_txt_list li:nth-child(3)'
            if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()
                df_data.loc[i, 'ブランド3'] = str(tmp)

            # ブランド3 URL
            path = '#s_brand .detail_txt_list li:nth-child(3) a'
            if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                tmp = driver.find_element(By.CSS_SELECTOR, path).get_attribute('href')
                df_data.loc[i, 'ブランド3 URL'] = str(tmp)

            # カテゴリ1
            path = '#s_cate .detail_txt_list li:nth-child(1)'
            if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()
                df_data.loc[i, 'カテゴリ1'] = str(tmp)

            # カテゴリ2
            path = '#s_cate .detail_txt_list li:nth-child(2)'
            if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()
                tmp = tmp.replace('>', '').strip()
                df_data.loc[i, 'カテゴリ2'] = str(tmp)

            # カテゴリ3
            path = '#s_cate .detail_txt_list li:nth-child(3)'
            if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()
                tmp = tmp.replace('>', '').strip()
                df_data.loc[i, 'カテゴリ3'] = str(tmp)

            # ブランド x カテゴリ3
            tmp = df_data.loc[i, 'ブランド'] + ' x ' + df_data.loc[i, 'カテゴリ3']
            df_data.loc[i, 'ブランド x カテゴリ3'] = tmp

            # 出品中の未取得
            if no_item == False:

                # お問い合わせ
                path = '#tabmenu_inqcnt'
                if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                    tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()
                    df_data.loc[i, 'お問い合わせ'] = str(tmp)

                # アクセス
                path = '.ac_count'
                if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                    tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()
                    df_data.loc[i, 'アクセス'] = str(tmp)

                # お気に入りアイテム登録
                path = '.fav_count'
                if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                    tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()
                    tmp = tmp.replace('人', '')
                    df_data.loc[i, 'お気に入りアイテム登録'] = str(tmp)

                # 価格
                tmp = ''
                path = '#abtest_display_pc'
                if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                    tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()
                else:
                    path = '#priceWrap .price_dd .fab-typo-midium'
                    if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                        tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()
                tmp = tmp.replace('¥', '')
                tmp = tmp.replace(',', '')
                df_data.loc[i, '価格'] = str(tmp)

                # タグ1
                # タグ2
                path = '.itemcomment-disc__detail a'
                if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                    elems = driver.find_elements(By.CSS_SELECTOR, path)
                    for elem in elems:
                        tmp = elem.text.strip()
                        if '手元に在庫あり' in tmp:
                            df_data.loc[i, 'タグ1'] = '手元に在庫あり(即発送可能)'
                        elif 'アウトレット' in tmp:
                            df_data.loc[i, 'タグ2'] = 'アウトレット'

                # 買付地
                path = '#s_buying_area a'
                if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                    tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()
                    df_data.loc[i, '買付地'] = str(tmp)

                # 買付先名
                path = '#s_buying_area span'
                if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                    tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()
                    df_data.loc[i, '買付先名'] = str(tmp)

                # 発送地
                path = '#s_shipment_area dd'
                if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                    tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()
                    df_data.loc[i, '発送地'] = str(tmp)

            if no_item:
                # 出品が削除されてカテゴリだけ取得
                df_data.loc[i, '出品'] = '削除'
            else:
                # 出品
                df_data.loc[i, '出品'] = '出品中'

            # 取得日時
            df_data.loc[i, '取得日時'] = get_dt_str()

            df_data.to_csv(FILE_PATH_ITEM, index=False)

    except Exception as e:
        driver.quit()
        print_ex("エラー発生: " + str(e))
        return False
    
    driver.quit()
    print_ex('出品データ(詳細)取得処理 終了')

    return True


def item_to_market(ss_url):

    # スプレッドシートから出品データ取得
    data = get_ss_all_values(ss_url, '出品データ')
    data_cols = data[0]
    data_rows = data[1:]
    df_data = pd.DataFrame(data_rows, columns=data_cols)

    # ブランド3で重複除去
    df_data = df_data.drop_duplicates(subset='ブランド3')           # 重複除去
    df_data = df_data[df_data['ブランド3'] != '']                   # 欠損行を削除
    df_data = df_data[['No.', 'ブランド', 'カテゴリ3', 'ブランド x カテゴリ3', 'ブランド3', 'ブランド3 URL']]        # 列抽出
    df_data = df_data.reset_index(drop=True)
    df_data['No.'] = df_data.index + 1                              # 連番振り直し
    df_data['出品数'] = -1                                          # 列追加
    df_data.to_csv(MARKET_PATH_ORDER, index=False)                  # ファイル出力


def merge_item_data():

    # 外注総合管理情報
    url_list = get_management_info()

    # 処理対象スプレッドシート
    ss_url = url_list[0]['sheet_url']       # ★index=0固定？？

    # 出品データ取得
    data = get_ss_all_values(ss_url, '出品データ')
    data_cols = data[0]
    data_rows = data[1:]
    df_item = pd.DataFrame(data_rows, columns=data_cols)

    # 差分比較用リスト
    df_list = pd.read_csv(FILE_PATH_LIST)

    # IDをキーにマージ
    df_list['ID'] = df_list['ID'].astype(str)
    df_item['ID'] = df_item['ID'].astype(str)
    merged_df = pd.merge(df_list, df_item, on='ID', how='outer', indicator=True)
    merged_df.to_csv('test_merged_df.csv', index=False)   

    # 新たに増えた商品データを商品リストに追加
    df_list_only = merged_df[merged_df['_merge'] == 'left_only']
    df_list_only_ids = df_list_only['ID'].tolist()
    df_add_rows = df_list[df_list['ID'].isin(df_list_only_ids)]
    df_item = pd.concat([df_item, df_add_rows], ignore_index=True)
    df_item.to_csv('test_df_add_rows.csv', index=False)

    df_item.loc[df_item['ID'].isin(df_list_only_ids), '出品'] = '未取得'
    df_item.to_csv('test_01.csv', index=False)

    # 減った商品データに削除フラグを付ける
    df_item_only = merged_df[merged_df['_merge'] == 'right_only']
    df_item_only_ids = df_item_only['ID'].tolist()
    df_item.loc[df_item['ID'].isin(df_item_only_ids), '出品'] = '削除'
    df_item.to_csv('test_02.csv', index=False)

    print('テスト終了')


def get_market_data():

    print_ex('市場データ取得処理 開始')

    try:
        driver = get_web_driver()
        df_data = pd.read_csv(MARKET_PATH_ORDER)

        for i in range(df_data.shape[0]):

            # URLアクセス
            target_url = df_data.loc[i, 'ブランド3 URL']

            driver.get(target_url)

            # 該当件数
            path = '#totalitem_num'
            if len(driver.find_elements(By.CSS_SELECTOR, path)) > 0:
                tmp = driver.find_element(By.CSS_SELECTOR, path).text.strip()
                tmp = tmp.replace(',', '')
                df_data.loc[i, '出品数'] = tmp
            
            df_data.to_csv(MARKET_PATH_ORDER, index=False)

    except Exception as e:
        driver.quit()
        print_ex("エラー発生: " + str(e))
        return False
    
    driver.quit()
    print_ex('市場データ取得処理 終了')
    return True

# 注文実績にあるが未出品の商品IDリスト取得
def get_compare_order_of_item(ss_url):

    # 出品データ取得
    data_item = get_ss_all_values(ss_url, '出品データ')
    ids_item = [row[1] for row in data_item[1:]]

    # 注文実績取得
    data_order = get_ss_all_values(ss_url, '注文実績')
    ids_order = [row[4] for row in data_order[1:]]

    # 追加するIDリスト
    ids_new = set(ids_order) - set(ids_item)

    count = len(ids_item) + 1
    for id in ids_new:
        if id == '':
            continue

        new_row = [f'{count}'] + [id] + ['未取得'] +  [''] + [''] + [f'https://www.buyma.com/item/{id}/'] + [''] * (len(data_item[0]) - 6)
        data_item.append(new_row)
        count += 1
    
    set_ss_all_values(ss_url, '出品データ', data_item[1:])


def main():
    print('テスト実行')
    get_compare_order_of_item('https://docs.google.com/spreadsheets/d/1kToIpIvwmrSzcKrPn0Y93-oD8LvQC7SWnFo-NpVmMyk/')

if __name__ == "__main__":
    main()
