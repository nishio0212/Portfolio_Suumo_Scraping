import pandas as pd
from pathlib import Path
import os, re, datetime

# 入力フォルダ・出力ファイル・駅名ファイル・方角ファイル
BASE_DIR = Path(__file__).parent
input_dir = BASE_DIR / "01_OutputData"
output_dir = BASE_DIR / "02_PreprocessingData"
output_file = BASE_DIR / "02_PreprocessingData" / "AllWards.csv"
directions_file = BASE_DIR / "directions.txt"
stations_file = BASE_DIR / "stations.txt"

# 出力フォルダを作成（存在する場合は無視）
os.makedirs(output_dir, exist_ok=True)


# マッピングのための関数
def mapping(file_path):
    data_map = {}  # 関数名との衝突を避けるため mapping → data_map に変更
    with open(file_path, encoding='UTF-8') as f:
        for line in f:
            name, code = line.strip().split(',')
            data_map[name] = int(code)
    return data_map


def extract_location(location):
    """
    所在地情報から「都市」「区」「残りの住所」を抽出・分割する関数。

    Args:
        location (str): 所在地の文字列（例："東京都新宿区西新宿2-8-1"）

    Returns:
        pd.Series: 以下の3つを含む Series を返す。
            - 都市名（例："東京都"、"横浜市"など）
            - 区名（例："新宿区"）
            - 残りの住所部分（例："西新宿2-8-1"）

    Notes:
        - 「市」が2つ含まれる場合（例："○○市△△市□□区"）は、2つ目の「市」までを都市名とする。
        - 「市」がない場合は、「都」「道」「府」「県」の区切り文字で都市名を抽出。
        - 区名は「○○区」の形式で抽出。
        - 不明または該当しない場合は空文字（''）のまま返される。
    """
    city = ''
    ward = ''
    remaining = location if isinstance(location, str) else ''  # 文字列であるか確認
    # "都市"の抽出
    if remaining != '':
        first_city_pos = remaining.find('市')
        if first_city_pos != -1:
            # 「市」の直後3文字以内にもう1つ「市」があるか確認
            check_range = remaining[first_city_pos + 1 : first_city_pos + 4]  # 3文字以内
            second_city_pos = check_range.find('市')
            if second_city_pos != -1:
                # 2つ目の「市」の直後までを都市名として取得
                city_end = first_city_pos + 1 + second_city_pos + 1
                city = remaining[:city_end]
            else:
                # 最初の「市」までで区切る
                city = remaining[:first_city_pos + 1]
        else:
            # 「都」「道」「府」「県」で区切る
            pref_match = re.search(r'(.+?[都道府県])', remaining)
            if pref_match:
                city = pref_match.group(1)

    remaining = remaining[len(city):] if city else remaining

    # "区"の抽出
    ward_match = re.search(r'(.+?区)', remaining)
    if ward_match:
        ward = ward_match.group(1)
        remaining = remaining[len(ward):]

    return pd.Series([city, ward, remaining])


def station_info_processing(walk_info, station_map):
    """
    駅徒歩情報から最寄り駅、駅ID、徒歩時間、沿線数、駅近フラグを抽出する関数

    Args:
        walk_info (str): "○○線/○○駅 歩△分" の形式の文字列
        station_map (dict): 駅名→ID のマッピング辞書（駅名に「駅」は含まない）

    Returns:
        pd.Series: [最寄り駅, 最寄り駅ID, 徒歩分数, 沿線数, 駅近フラグ]
    """
    nearest_station = '-'
    station_id = None
    line_count = 0
    is_near = 0

    min_walk = float('inf')  # 徒歩時間の最小値記録用
    current_station = ''

    if pd.notna(walk_info) and isinstance(walk_info, str):
        segments = walk_info.split('/')
        line_count = (len(segments) + 1) // 2 if segments else None

        for segment in segments:
            segment = segment.strip()
            if '駅' in segment:
                current_station = segment.split('駅')[0] + '駅'
            elif '分' in segment:
                walk = re.sub(r'[^0-9]', '', segment)
                if walk.isdigit():
                    walk_value = int(walk)

                    if walk_value < min_walk:
                        min_walk = walk_value
                        nearest_station = current_station  # この駅が最も近い

    if min_walk is not None and min_walk < 15:
        is_near = 1

    # "駅"が末尾にある場合だけ削る
    station_key = nearest_station[:-1] if nearest_station.endswith('駅') else nearest_station
    station_id = station_map.get(station_key, None)

    return pd.Series([
        nearest_station,
        station_id,
        None if min_walk == float('inf') else min_walk,
        line_count,
        is_near
    ])


def room_count(room):
    """
    間取りの文字列から部屋数をカウントする関数。

    対応内容：
    - 1LDK → 1 + L(1) + D(1) + K(1) = 4
    - 2DK＋S → 2 + D(1) + K(1) + S(1) = 5
    - ワンルーム → 1
    - L, D, K, S, N を部屋としてカウント
    - 不明または非文字列は None

    Args:
        room (str): 間取り情報の文字列

    Returns:
        int: 部屋数
    """
    if not isinstance(room, str):
        return None

    if 'ワンルーム' in room:
        return 1

    # ベースの数字（最初の数字）
    m = re.match(r'(\d+)', room)
    base = int(m.group(1)) if m else None
    if base is None:
        return None

    # L, D, K, S, N の文字数を加算
    extras = sum(room.count(x) for x in ['L', 'D', 'K', 'S', 'N'])

    return base + extras


def str_chk(x):
    """
    値を文字列に安全に変換し、前後の空白を削除する関数。

    - 欠損値（NaN）や文字列以外の型（数値、None など）の場合は '-' を返す。
    - 正常な文字列の場合は strip() により前後の空白を除去した文字列を返す。

    Args:
        x (any): 任意の値（str, int, float, None など）

    Returns:
        str: 前後空白を除いた文字列、または欠損時は '-'
    """
    return str(x).strip() if pd.notna(x) and isinstance(x, str) else '-'


def int_chk(x):
    """
    任意の値を int 型に安全に変換する関数。

    - 数値文字列（'3' など）や float（3.0 など）も変換可能。
    - 欠損値（None, NaN）や変換できない文字列（'abc' など）は None を返す。
    - 例外を出さず、安全に処理を行う。

    Args:
        x (any): 任意の値（str, int, float, None など）

    Returns:
        int or None: 整数値（変換成功時）、または None（変換失敗時）
    """
    try:
        return int(x)
    except (ValueError, TypeError):
        return None


def float_chk(val):
    """
    安全にfloatへ変換する関数。

    - None, NaN, 空文字は None を返す
    - 数値文字列や数値は float へ変換
    - 変換できないものは例外を出さず None を返す

    Args:
        val (any): 任意の値

    Returns:
        float or None: float値 または None
    """
    if pd.isna(val):
        return None
    if isinstance(val, str) and val.strip() == '':
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def process_all_records(df, start_id, station_map, direction_map):
    """
    DataFrame の全行を整形・正規化して新しい DataFrame を返す。

    Args:
        df (pd.DataFrame): スクレイピング結果の1区分のデータ
        start_id (int): この df の先頭行に割り当てる物件ID（連番の開始値）
        station_map (dict): 駅名→ID のマッピング辞書
        direction_map (dict): 方角→ID のマッピング辞書

    Returns:
        pd.DataFrame: 整形済みデータ
    """
    results = []

    for i, (_, row) in enumerate(df.iterrows()):
        record = {}

        # "所在地"カラムの分割処理
        city, ward, remaining = extract_location(str(row['所在地']))
        # "駅徒歩"カラムの分割処理
        nearest_station, station_id, min_walk, line_count, is_near = station_info_processing(str(row['駅徒歩']), station_map)
        # "間取り"カラムから"部屋数"カラムのカウント処理
        room_num = room_count(str(row['間取り']))

        # "家賃（込々）"カラムの計算
        rent = float_chk(row['家賃'])
        rent = rent if rent is not None else 0
        fee = float_chk(row['管理費・共益費'])
        fee = fee if fee is not None else 0

        # 敷金・礼金・初期費用の計算
        deposit = float_chk(row['敷金'])
        deposit_val = deposit if deposit is not None else 0
        key_money = float_chk(row['礼金'])
        key_money_val = key_money if key_money is not None else 0

        # 設備テキスト（設備フラグ抽出用）
        features_raw = row.get('設備', '')
        features = str(features_raw) if pd.notna(features_raw) else ''

        record['物件ID'] = str(start_id + i).zfill(5)
        record['物件名'] = str_chk(row['物件名'])
        record['都市'] = str_chk(city)
        record['区'] = str_chk(ward)
        record['所在地'] = str_chk(remaining)
        record['最寄り駅'] = str_chk(nearest_station)
        record['最寄り駅ID'] = int_chk(station_id)
        record['徒歩'] = int_chk(min_walk)
        record['沿線数'] = int_chk(line_count)
        record['駅近'] = int_chk(is_near)
        record['間取り'] = str_chk(row['間取り'])
        record['部屋数'] = int_chk(room_num)
        record['専有面積'] = float_chk(row['専有面積'])
        record['家賃'] = rent
        record['管理費・共益費'] = fee
        record['家賃（込々）'] = float_chk(rent + fee)
        record['敷金'] = deposit
        record['礼金'] = key_money
        record['初期費用'] = float_chk(deposit_val + key_money_val)
        record['築年数'] = None  # TODO: 未実装（'築年数・構造'列の文字列パースが必要）
        record['階建'] = None  # TODO: 未実装（'築年数・構造'列の文字列パースが必要）
        record['階数'] = str_chk(row['階数'])
        record['階数比率'] = None  # TODO: 未実装（階数・階建の両方が確定してから計算）
        record['入居可能時期'] = str_chk(row['入居可能時期'])
        record['バス・トイレ別'] = int('バス・トイレ別' in features)
        record['エアコン'] = int('エアコン' in features)
        record['クローゼット'] = int('クローゼット' in features)
        record['フローリング'] = int('フローリング' in features)
        record['2階以上'] = int('2階以上' in features)
        record['洗面所独立'] = int('洗面所独立' in features)
        record['室内洗濯機置場'] = int('洗濯機置場' in features)
        record['オートロック'] = int('オートロック' in features)
        record['TVインターホン'] = int('TVインターホン' in features or 'テレビドアホン' in features)
        record['追焚機能浴室'] = int('追焚' in features)
        record['ネット使用料不要'] = int('ネット使用料無料' in features or 'インターネット無料' in features)
        record['宅配ボックス'] = int('宅配ボックス' in features)
        record['温水洗浄便座'] = int('温水洗浄便座' in features or 'ウォシュレット' in features)
        record['システムキッチン'] = int('システムキッチン' in features)
        record['3口以上コンロ'] = int('3口' in features)
        record['浴室乾燥機'] = int('浴室乾燥機' in features)
        record['防犯カメラ'] = int('防犯カメラ' in features)
        record['照明付き'] = int('照明' in features)
        record['高速ネット対応'] = int('高速インターネット' in features or 'インターネット対応' in features)
        record['向き'] = str_chk(row['向き'])
        record['角部屋'] = int_chk(row['角部屋'])
        record['最上階'] = int_chk(row['最上階'])
        record['画像数'] = int_chk(row['画像数'])
        record['詳細URL'] = str_chk(row['詳細URL'])

        results.append(record)

    return pd.DataFrame(results)


def main():
    station_map = mapping(stations_file)
    direction_map = mapping(directions_file)

    idno = 1
    all_results = []

    for file in sorted(input_dir.glob("??_*.csv")):
        df = pd.read_csv(file, encoding='utf-8')
        processed_df = process_all_records(df, idno, station_map, direction_map)
        idno += len(df)  # 次のファイルの開始IDに更新
        all_results.append(processed_df)  # DataFrame のみ追加（タプルではない）

    final_df = pd.concat(all_results, ignore_index=True)
    final_df.to_csv(output_file, index=False, encoding='utf-8-sig')


if __name__ == '__main__':
    main()
