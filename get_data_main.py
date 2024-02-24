#--------------------------------------------------------------------------------
# データ取得メイン処理
#--------------------------------------------------------------------------------
# ① 外注総合管理ツールを読込み実行順を定める
#--------------------------------------------------------------------------------
import pytz
from get_data_init import get_data_init
from get_data_update import get_data_update
from scraping import *
from datetime import datetime, timedelta
from collections import deque

tz = pytz.timezone('Asia/Tokyo')


def get_data_main():

    print_ex('データ取得メイン処理 開始')

    try:
        clients = []
        for url in MANAGE_SS_URL:
            # 外注管理リスト取得
            url_list = get_management_info(url)
            clients.append(url_list)
        
        queueInit = deque()
        queueUpdate = deque()

        while any(clients):
            for i, client in enumerate(clients):
                for j, row in enumerate(client):
                    data = client.pop(0)

                    # 初回対象サーチ
                    state = data['state']
                    if (state == GetDataStep.INIT_NONE.value or         # 初回データ取得予約
                        state == GetDataStep.INIT_RUN_ORDER.value or    # 初回取得中(注文実績)
                        state == GetDataStep.INIT_RUN_LIST.value or     # 初回取得中(出品リスト)
                        state == GetDataStep.INIT_RUN_CHECK.value or    # 初回取得中(出品リスト確認)
                        state == GetDataStep.INIT_RUN_ITEM.value or     # 初回取得中(出品データ)
                        state == GetDataStep.INIT_RUN_MARKET.value or   # 初回取得中(市場データ)
                        state == GetDataStep.INIT_ERROR.value):         # 初回取得エラー
                        queueInit.append({'url': MANAGE_SS_URL[i], 'index': j, 'info': data})
                        break

                    # 更新対象サーチ
                    if (state == GetDataStep.INIT_DONE.value or         # 初回取得済
                        state == GetDataStep.UPDATE_RUN_ORDER.value or  # 更新中(注文実績)
                        state == GetDataStep.UPDATE_RUN_LIST.value or   # 更新中(出品リスト)
                        state == GetDataStep.UPDATE_RUN_ITEM.value or   # 更新中(出品データ)
                        state == GetDataStep.UPDATE_RUN_MARKET.value or # 更新中(市場データ)
                        state == GetDataStep.UPDATE_ERROR.value or      # 更新エラー
                        state == GetDataStep.UPDATE_DONE.value):        # 更新済
                        queueUpdate.append({'url': MANAGE_SS_URL[i], 'index': j, 'info': data})
                        break

        # 更新処理
        while queueUpdate:
            item = queueUpdate.popleft()
            get_data_update(item['url'], item['index'], item['info'])

        # 初期化処理
        while queueInit:
            item = queueInit.popleft()
            get_data_init(item['url'], item['index'], item['info'])

    except Exception as e:
        print_ex(f'エラー発生: {e}')
        raise

    print_ex('データ取得メイン処理 終了')
    return


def main():
    get_data_main()


if __name__ == "__main__":
    main()