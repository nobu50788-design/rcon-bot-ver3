import os
import json
import time
import re
import threading
import websocket
import pandas as pd
import gspread
from datetime import datetime
from google.oauth2.service_account import Credentials

# --- 設定 (GitHub Actions Secrets から取得) ---
RCON_IP = os.getenv("RCON_SERVER_IP")
RCON_PORT = os.getenv("RCON_PORT")
RCON_PASS = os.getenv("RCON_PASS")
SHEET_ID = os.getenv("GOOGLE_SHEET_ID")
# JSON文字列を辞書に変換
creds_dict = json.loads(os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON"))

# --- グローバル変数 ---
rcon_message = ""

def on_message(ws, message):
    global rcon_message
    try:
        data = json.loads(message)
        if "Message" in data and data["Message"]:
            rcon_message = data["Message"]
        ws.close()
    except Exception as e:
        print(f"Error parsing message: {e}")

def on_error(ws, error):
    print(f"RCON WebSocket Error: {error}")

def on_open(ws):
    print("Connected to RCON. Sending status command...")
    payload = {
        "Identifier": 100,
        "Message": "status",
        "Name": "WebRcon"
    }
    ws.send(json.dumps(payload))

def fetch_rcon_data():
    global rcon_message
    url = f"ws://{RCON_IP}:{RCON_PORT}/{RCON_PASS}"
    
    ws = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error
    )

    # threading.Timer を使用してタイムアウトを管理 (仕様準拠)
    def stop_ws():
        print("RCON timeout reached. Closing connection.")
        ws.close()

    timer = threading.Timer(10.0, stop_ws) # 10秒でタイムアウト
    timer.start()

    try:
        ws.run_forever()
    except Exception as e:
        print(f"RCON Connection failed: {e}")
    finally:
        timer.cancel()
    
    return rcon_message

def parse_rcon_output(raw_text):
    """
    RCONの文字列を解析してリスト形式で返す
    Format: id name ping connected addr ...
    """
    players = []
    # 正規表現: ID, "Name", Ping, ConnectedTime (s) を抽出
    # 例: 76561198751660849 "ゆも" 5 1682.854s ...
    pattern = re.compile(r'(\d+)\s+"([^"]+)"\s+(\d+)\s+([\d.]+)s')
    
    lines = raw_text.strip().split('\n')
    for line in lines:
        match = pattern.search(line)
        if match:
            p_id, p_name, p_ping, p_conn_sec = match.groups()
            
            # 接続時間を HH:MM:SS に変換
            total_seconds = float(p_conn_sec)
            hours = int(total_seconds // 3600)
            minutes = int((total_seconds % 3600) // 60)
            seconds = int(total_seconds % 60)
            conn_formatted = f"{hours:02}:{minutes:02}:{seconds:02}"

            players.append({
                "id": p_id,
                "name": p_name,
                "ping": int(p_ping),
                "connected": conn_formatted
            })
    return players

def main():
    print("Starting Rust Player Collector...")

    # 1. RCONデータ取得
    raw_status = fetch_rcon_data()
    if not raw_status:
        print("No RCON data received or server is empty.")
        # 仕様: 空リストの場合は書き換えを行わず終了
        return

    new_players_list = parse_rcon_output(raw_status)
    if not new_players_list:
        print("No players found in RCON status. Ending process without changing sheet.")
        return

    # 2. Google Sheets 準備
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SHEET_ID)
    sheet = spreadsheet.get_worksheet(0) # 一番左のシート

    # 3. 既存データの読み込み (B2:F の範囲を想定)
    # A列(1)と1行目を飛ばすため、全データを取得してからスライスするか、range指定する
    all_values = sheet.get_all_values()
    
    existing_data = []
    if len(all_values) > 1:
        # B2セル以降のデータ (Row 2+, Col B-F) を取得
        for row in all_values[1:]: # 1行目はヘッダーなので飛ばす
            if len(row) >= 5:
                # 列インデックスは A=0, B=1, C=2... なので B列以降は index 1から
                # 指定された項目: id(B), name(C), ping(D), connected(E), lastModifiedDate(F)
                existing_data.append({
                    "id": str(row[1]),
                    "name": row[2],
                    "ping": row[3],
                    "connected": row[4],
                    "lastModifiedDate": row[5] if len(row) > 5 else ""
                })

    df_existing = pd.DataFrame(existing_data)
    df_new = pd.DataFrame(new_players_list)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 4. データ統合 (Upsert)
    if df_existing.empty:
        # 初回実行時
        for _, p in df_new.iterrows():
            p['lastModifiedDate'] = now_str
            new_players_list.append(p.to_dict())
        df_final = pd.DataFrame(new_players_list)
    else:
        # 既存プレイヤーと新規プレイヤーの統合
        # 新規/継続プレイヤー用リスト
        updated_rows = []
        
        # 現在接続中のプレイヤーを処理
        for _, p_new in df_new.iterrows():
            match = df_existing[df_existing['id'] == str(p_new['id'])]
            if not match.empty:
                # 継続プレイヤー: 更新
                row = p_new.to_dict()
                row['lastModifiedDate'] = now_str
                updated_rows.append(row)
            else:
                # 新規プレイヤー: 追加
                row = p_new.to_dict()
                row['lastModifiedDate'] = now_str
                updated_rows.append(row)
        
        # オフラインプレイヤー (既存にあるが、新しいリストにいない人)
        # 仕様: 更新（上書き）を行わず、そのまま保持する
        offline_players = df_existing[~df_existing['id'].isin(df_new['id'].astype(str))]
        
        # 統合
        df_final = pd.concat([pd.DataFrame(updated_rows), offline_players], ignore_index=True)

    # 5. 並び替え (更新日時の降順)
    # lastModifiedDateが空のものは後ろに送るための処理
    df_final['lastModifiedDate'] = pd.to_datetime(df_final['lastModifiedDate'], errors='coerce')
    df_final = df_final.sort_values(by='lastModifiedDate', ascending=False, na_position='last')
    # 日時形式を文字列に戻す
    df_final['lastModifiedDate'] = df_final['lastModifiedDate'].dt.strftime("%Y-%m-%d %H:%M:%S")

    # 6. 書き込み準備 (B-F列のリストを作成)
    # 仕様: A列と1行目は触らない。B2から右下に限定。
    write_data = []
    for _, row in df_final.iterrows():
        # B, C, D, E, F 列の内容をリスト化
        write_data.append([
            str(row['id']),
            str(row['name']),
            row['ping'],
            str(row['connected']),
            str(row['lastModifiedDate'])
        ])

    # 7. 一括書き込み (重要: sheet.clear() は使用しない)
    if write_data:
        # B2 から始まる範囲を指定して一括更新
        # rangeは "B2:F" + 行数
        end_row = len(write_data) + 1
        range_label = f"B2:F{end_row}"
        sheet.update(range_label, write_data)
        print(f"Successfully updated {len(write_data)} rows.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"System Error: {e}")
        # GitHub Actions を Failure にさせないための適切な終了
