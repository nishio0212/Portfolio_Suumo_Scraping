import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import re
import time
import os

# 出力フォルダ
output_dir = Path(__file__).parent / "01_OutputData"
os.makedirs(output_dir, exist_ok=True)

# 区名とコード読み込み
with open(Path(__file__).parent / 'codes.txt', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# 区名の英語マッピング（ファイル名用）
ward_name_eng_map = {
    '千代田区': 'chiyoda', '中央区': 'chuo', '港区': 'minato', '新宿区': 'shinjuku',
    '文京区': 'bunkyo', '台東区': 'taito', '墨田区': 'sumida', '江東区': 'koto',
    '品川区': 'shinagawa', '目黒区': 'meguro', '大田区': 'ota', '世田谷区': 'setagaya',
    '渋谷区': 'shibuya', '中野区': 'nakano', '杉並区': 'suginami', '豊島区': 'toshima',
    '北区': 'kita', '荒川区': 'arakawa', '板橋区': 'itabashi', '練馬区': 'nerima',
    '足立区': 'adachi', '葛飾区': 'katsushika', '江戸川区': 'edogawa'
}

headers = {"User-Agent": "Mozilla/5.0"}
MAX_PAGE = 50

def safe_text(tag):
    return tag.text.strip() if tag else ''

def safe_float(text):
    if not text:
        return None
    text = text.replace('万円', '').replace('円', '').replace(',', '').replace('m2', '').replace('㎡', '').strip()
    try:
        return float(text) if text != '-' else None
    except ValueError:
        return None

# 物件情報抽出
def extract_property_info(soup):
    data = []
    articles = soup.select('.cassetteitem')

    for article in articles:
        building_title = safe_text(article.select_one('.cassetteitem_content-title'))
        address = safe_text(article.select_one('.cassetteitem_detail-col1'))
        build_info = safe_text(article.select_one('.cassetteitem_detail-col3'))
        station_info = [s.text.strip() for s in article.select('.cassetteitem_detail-text')]

        table = article.select_one('table.cassetteitem_other')
        if not table:
            continue
        rows = table.select('tbody tr')

        for row in rows:
            if len(data) >= 1000:
                break

            layout = safe_text(row.select_one('.cassetteitem_madori'))
            area = safe_float(safe_text(row.select_one('.cassetteitem_menseki')))
            rent = safe_float(safe_text(row.select_one('.cassetteitem_price--rent')))
            mgmt_fee_raw = safe_float(safe_text(row.select_one('.cassetteitem_price--administration')))
            mgmt_fee = round(mgmt_fee_raw / 10000, 4) if mgmt_fee_raw is not None else None
            deposit = safe_float(safe_text(row.select_one('.cassetteitem_price--deposit')))
            key_money = safe_float(safe_text(row.select_one('.cassetteitem_price--gratuity')))
            floor = safe_text(row.select_one('.cassetteitem_other-floor'))

            # 部屋ごとのリンク取得
            link_tag = row.select_one('a.js-cassette_link_href')
            relative_url = link_tag['href'] if link_tag and 'href' in link_tag.attrs else ''
            detail_url = f'https://suumo.jp{relative_url}' if relative_url.startswith('/chintai/jnc_') else None

            movein_clean, features_text, direction = '', '', ''
            is_top, is_corner, image_count = 0, 0, 0
            detail_floor = ''

            if detail_url:
                try:
                    detail_res = requests.get(detail_url, headers=headers, timeout=10)
                    detail_soup = BeautifulSoup(detail_res.text, 'html.parser')
                    time.sleep(1.5)

                    # 入居可能時期
                    movein_th = detail_soup.find('th', string='入居')
                    if movein_th and movein_th.find_next_sibling('td'):
                        movein_raw = movein_th.find_next_sibling('td').text.strip()
                        movein_clean = re.sub(r'[上中下]旬', '', movein_raw)

                    # 向き
                    direction_th = detail_soup.find('th', string='向き')
                    if direction_th and direction_th.find_next_sibling('td'):
                        direction = direction_th.find_next_sibling('td').text.strip()

                    # 詳細階数
                    floor_th = detail_soup.find('th', string=re.compile("階"))
                    if floor_th and floor_th.find_next_sibling('td'):
                        detail_floor = floor_th.find_next_sibling('td').text.strip()

                    remarks = detail_soup.get_text()
                    is_top = int('最上階' in remarks)
                    is_corner = int('角部屋' in remarks)

                    # 設備（h2 > span → div.section → ul.inline_list）
                    for h2 in detail_soup.select('h2'):
                        span = h2.find('span')
                        if span and '部屋の特徴・設備' in span.get_text():
                            sibling_div = h2.find_next_sibling('div', class_='section l-space_small')
                            if sibling_div:
                                ul = sibling_div.select_one('ul.inline_list')
                                if ul:
                                    features_text = ' / '.join([li.text.strip() for li in ul.select('li')])
                            break

                    # 画像数（部屋画像のみ）
                    ul_gallery = detail_soup.find('ul', id='js-view_gallery-navlist')
                    if ul_gallery:
                        image_count = len(ul_gallery.select('li'))
                    else:
                        image_count = 0
                except Exception as e:
                    print(f"詳細取得失敗: {detail_url} - {e}")

            data.append({
                '物件名': building_title or '',
                '所在地': address or '',
                '駅徒歩': ' / '.join(station_info) if station_info else '',
                '間取り': layout or '',
                '専有面積': area,
                '家賃': rent,
                '管理費・共益費': mgmt_fee,
                '敷金': deposit,
                '礼金': key_money,
                '築年数・構造': build_info or '',
                '階数': detail_floor or floor or '',
                '入居可能時期': movein_clean or '',
                '設備': features_text or '',
                '向き': direction or '',
                '角部屋': is_corner,
                '最上階': is_top,
                '画像数': image_count,
                '詳細URL': detail_url or ''
            })
            time.sleep(0.5)
    return data


def main():
    # --- 平均家賃集計用リスト ---
    avg_rents = []

    # --- 全区ループ実行 ---
    for idx, line in enumerate(lines, 1):
        ward_name, ward_code = line.strip().split(',')
        eng_name = ward_name_eng_map.get(ward_name, ward_name)
        print(f"{ward_name}（コード: {ward_code}）のデータを取得中...")

        all_data = []
        for page in range(1, MAX_PAGE + 1):
            url = f"https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&ta=13&sc={ward_code}&pn={page}"
            res = requests.get(url, headers=headers, timeout=10)
            if res.status_code != 200:
                print(f"ページ {page} のリクエストに失敗しました")
                break
            soup = BeautifulSoup(res.text, 'html.parser')
            batch = extract_property_info(soup)
            all_data.extend(batch)
            if len(all_data) >= 1000 or not batch:
                break

        df = pd.DataFrame(all_data)
        filename = os.path.join(output_dir, f"{str(idx).zfill(2)}_{eng_name}.csv")
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"{ward_name} のデータを {filename} に保存しました（{len(all_data)}件）")

        # --- 平均家賃を計算してリストに追加 ---
        try:
            mean_rent = df['家賃'].dropna().mean()
            avg_rents.append({'区名': ward_name, '平均家賃': round(mean_rent, 2)})
        except Exception as e:
            print(f"平均家賃の算出失敗（{ward_name}）: {e}")

    # --- まとめてCSV出力 ---
    avg_df = pd.DataFrame(avg_rents)
    avg_file = os.path.join(output_dir, '23区_平均家賃一覧.csv')
    avg_df.to_csv(avg_file, index=False, encoding='utf-8-sig')
    print(f"\n✅ 区ごとの平均家賃を {avg_file} に保存しました")


if __name__ == '__main__':
    main()
