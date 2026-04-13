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

    def stop_ws():
        ws.close()

    timer = threading.Timer(10.0, stop_ws)
    timer.start()

    try:
        ws.run_forever()
    except Exception as e:
        print(f"RCON Connection failed: {e}")
    finally:
        timer.cancel()
    
    return rcon_message

def parse_rcon_output(raw_text):
    players = []
    pattern = re.compile(r'(\d+)\s+"([^"]+)"\s+(\d+)\s+([\d.]+)s')
    lines = raw_text.strip().split('\n')
    for line in lines:
        match = pattern.search(line)
        if match:
            p_id, p_name, p_ping, p_conn_sec = match.groups()
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
    print("Starting Rust Player Collector (with Column A Sync)...")

    # 1. RCONデータ取得
    raw_status = fetch_rcon_data()
    if not raw_status:
        print("No RCON data received.")
        return

    new_players_list = parse_rcon_output(raw_status)
    if not new_players_list:
        print("No players found in RCON status. Ending without change.")
        return

    # 2. Google Sheets 準備
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(creds)
    spreadsheet = client.open_by_key(SHEET_ID)
    sheet = spreadsheet.get_worksheet(0)

    # 3. 既存データの読み込み (A列からF列まで取得)
    all_values = sheet.get_all_values()
    
    existing_data = []
    if len(all_values) > 1:
        for row in all_values[1:]: # 2行目以降
            # A=0, B=1, C=2, D=3, E=4, F=5
            # A列が空、または空白文字のみの場合は "未検証" とする
            status_val = row[0].strip() if len(row) > 0 and row[0].strip() != "" else "未検証"
            
            existing_data.append({
                "status": status_val,         # A列: プルダウンの値
                "id": str(row[1]) if len(row) > 1 else "", # B列
                "name": row[2] if len(row) > 2 else "",    # C列
                "ping": row[3] if len(row) > 3 else 0,     # D列
                "connected": row[4] if len(row) > 4 else "", # E列
                "lastModifiedDate": row[5] if len(row) > 5 else "" # F列
            })

    df_existing = pd.DataFrame(existing_data)
    df_new = pd.DataFrame(new_players_list)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 4. データ統合 (Upsert)
    if df_existing.empty:
        # 初回：全員 "未検証" で追加
        for _, p in df_new.iterrows():
            p['status'] = "未検証"
            p['lastModifiedDate'] = now_str
            new_players_list.append(p.to_dict())
        df_final = pd.DataFrame(new_players_list)
    else:
        updated_rows = []
        # 現在接続中のプレイヤーを処理（継続 or 新規）
        for _, p_new in df_new.iterrows():
            match = df_existing[df_existing['id'] == str(p_new['id'])]
            if not match.empty:
                # 継続プレイヤー：既存の A列 (status) を引き継ぎ、他を更新
                row = p_new.to_dict()
                row['status'] = match.iloc[0]['status']
                row['lastModifiedDate'] = now_str
                updated_rows.append(row)
            else:
                # 新規プレイヤー：A列は "未検証"
                row = p_new.to_dict()
                row['status'] = "未検証"
                row['lastModifiedDate'] = now_str
                updated_rows.append(row)
        
        # オフラインプレイヤー：既存のデータをそのまま保持（A列も維持）
        offline_players = df_existing[~df_existing['id'].isin(df_new['id'].astype(str))]
        
        df_final = pd.concat([pd.DataFrame(updated_rows), offline_players], ignore_index=True)

    # 5. 並び替え (更新日時の降順)
    df_final['lastModifiedDate'] = pd.to_datetime(df_final['lastModifiedDate'], errors='coerce')
    df_final = df_final.sort_values(by='lastModifiedDate', ascending=False, na_position='last')
    df_final['lastModifiedDate'] = df_final['lastModifiedDate'].dt.strftime("%Y-%m-%d %H:%M:%S")

    # 6. 書き込み用リストの作成 (A列からF列まで)
    write_data = []
    for _, row in df_final.iterrows():
        # 万が一 status が空になった場合のセーフティ
        status = str(row['status']).strip() if pd.notna(row['status']) else "未検証"
        if not status:
            status = "未検証"

        write_data.append([
            status,               # A列 (Status/プルダウン)
            str(row['id']),       # B列 (ID)
            str(row['name']),     # C列 (Name)
            int(row['ping']) if pd.notna(row['ping']) else 0, # D列 (Ping)
            str(row['connected']),# E列 (Connected)
            str(row['lastModifiedDate']) # F列 (Last Modified)
        ])

    # 7. 一括書き込み (A2からF列の範囲)
    if write_data:
        end_row = len(write_data) + 1
        range_label = f"A2:F{end_row}" # A2から開始するように変更
        sheet.update(range_label, write_data)
        print(f"Successfully updated {len(write_data)} rows (Columns A-F).")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"System Error: {e}")
