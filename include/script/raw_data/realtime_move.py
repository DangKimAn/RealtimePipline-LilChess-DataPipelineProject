# from boto3.resources import response
# from sympy.assumptions.sathandlers import exactlyonearg
import json 
import requests
import time
from get_channel import get_all_channel
from get_game import get_game_id_multi_channel, get_game_id_a_channel
import pandas as pd 
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
import sys
import os
# Thêm thư mục `script` (thư mục cha) vào hệ thống path để Python hiểu được package `kafka`
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from kafka.producer import MyProducer

from get_user import get_user
def listen_to_one_game_moves(game_id , producer_move):  
    # URL lắng nghe nước đi realtime của ván cờ 4LH8VoKa

    url = f"https://lichess.org/api/stream/game/{game_id}"
    
    # print("Đang kết nối vào ván cờ...")
    
    headers = {
        "Accept": "application/x-ndjson",
        "User-Agent": "dangkiman41005@gmail.com/1.0"
    }
    # Không cần headers token, chỉ cần stream=True
    response = requests.get(url, stream= True, headers= headers)
    data_moves = { 
        'game_id': [],
        'num_of_moves':[],
        'color_turn':[],
        'fen' :[],
        'time_left':[],
        'move':[]
         }
    # print(response.json())

    data_game ={}

    get_user_white_id_after = {}
    get_user_white_elo_after = {}
    get_user_black_id_after = {}
    get_user_black_elo_after = {}
    if response.status_code == 200:
        print("Kết nối thành công! Đang chờ dữ liệu...\n")  
        for line in response.iter_lines():
            if line:
                raw_data = json.loads(line.decode('utf-8'))
                if raw_data.get('lm'):
                    fen = str(raw_data.get('fen')).split(' ')
                    data_moves['fen'].append(fen[0])
                    data_moves['move'].append(raw_data.get('lm'))
                    data_moves['time_left'].append(int(raw_data.get('wc')) if str.lower(fen[1]) == 'b' else int(raw_data.get('bc')))
                    data_moves['num_of_moves'].append(int(fen[len(fen) -1 ]) if str.lower(fen[1]) == 'b'  else int(fen[len(fen) -1 ]) -1 )
                    data_moves['color_turn'].append('white' if str.lower(fen[1]) == 'b' else 'black')
                    data_moves['game_id'].append(game_id)

                    data_test ={
                        'fen' : fen[0] ,
                    'move' : raw_data.get('lm') ,
                    'time_left' : int(raw_data.get('wc')) if str.lower(fen[1]) == 'b' else int(raw_data.get('bc')), 
                    'num_of_moves' : int(fen[len(fen) -1 ]) if str.lower(fen[1]) == 'b'  else int(fen[len(fen) -1 ]) -1 ,
                    'color_turn' : 'white' if str.lower(fen[1]) == 'b' else 'black',
                    'game_id' : game_id
                    }
                    # print(data_test)
                    producer_move.send_message(key=game_id, value=json.dumps(data_test))
                elif raw_data.get('winner'):
                    data_game['game_id'] = game_id
                    data_game['winner'] = raw_data.get('winner') if raw_data.get('winner') else ''
                    data_game['winner_id'] = raw_data.get('players').get(raw_data.get('winner')).get('user').get('id')
                    data_game['status_name'] = raw_data.get('status').get('name')
                    data_game['status_id'] = raw_data.get('status').get('id')
                    data_game['turns'] = raw_data.get('turns')
                    data_game['white_id'] = raw_data.get('players').get('white').get('user').get('id')
                    data_game['black_id'] = raw_data.get('players').get('black').get('user').get('id')
                    data_game['rated'] = raw_data.get('rated')
                    data_game['source'] = raw_data.get('source')
                    data_game['speed'] = raw_data.get('speed')
                    data_game['perf'] = raw_data.get('perf')
                    data_game['createdAt'] = raw_data.get('createdAt')
                    get_user_white_id_after , get_user_white_elo_after = get_user(username=raw_data.get('players').get('white').get('user').get('id'))
                    get_user_black_id_after , get_user_black_elo_after = get_user(username=raw_data.get('players').get('black').get('user').get('id'))
                elif raw_data.get('id') and not raw_data.get('lm'):
                    # Message gameFull
                    white_info = raw_data.get('white', {})
                    black_info = raw_data.get('black', {})
                    if isinstance(white_info, dict) and 'rating' in white_info:
                        data_game['white_rating_before'] = white_info.get('rating')
                        data_game['black_rating_before'] = black_info.get('rating')

        data_user = {k: get_user_white_id_after.get(k, []) + get_user_black_id_after.get(k, []) for k in get_user_white_id_after.keys()}
        data_elo = {k: get_user_white_elo_after.get(k, []) + get_user_black_elo_after.get(k, []) for k in get_user_white_elo_after.keys()}

        return data_moves , data_game , data_user , data_elo
    else:
        # print(f"Lỗi: {response.status_code}")
        print(f"Lỗi: {response.status_code}")
        # THÊM DÒNG NÀY ĐỂ BÁO LỖI CHO CALLBACK
        raise RuntimeError(f"Kết nối API thất bại  van {game_id} với mã lỗi {response.status_code}")
        

# def on_game_finished(game_id, active_games, future,producer_game ,producer_elo , producer_user):
def on_game_finished(game_id, active_games, producer_game, producer_elo, producer_user, future):
    """ Hàm callback chạy tự động khi 1 luồng ván cờ kết thúc """
    active_games.discard(game_id)
    try:
        data_moves, data_game, data_user, data_elo = future.result()
        print(f"\n[HOÀN THÀNH] Ván cờ {game_id} đã lưu trữ xong dữ liệu!")
        
        producer_game.send_message(key=game_id, value=json.dumps(data_game))
        producer_user.send_message(key=game_id, value=json.dumps(data_user))
        producer_elo.send_message(key=game_id, value=json.dumps(data_elo))
    except Exception as e:
        print(f"[LỖI] Ván {game_id} kết thúc thất bại: {e}")

def listen_to_multi_game_moves_in_channel(channel , producer_game , producer_elo , producer_user , producer_move, max_workers , time_sleep):
    active_games = set()
    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            print(f"Bắt đầu theo dõi kênh: {channel}...")
            while True:
                if len(active_games) < max_workers:
                    dict_list_game_id = get_game_id_a_channel(channel)
                    print(f'dict_list_game_id {dict_list_game_id}')
                    if dict_list_game_id is None:
                        print(f"Không thể lấy danh sách game cho kênh {channel}, chờ 10s rồi thử lại...")
                        time.sleep(10)
                        continue

                    list_game_id = dict_list_game_id.get(channel)
                    
                    if list_game_id:
                        for game_id in list_game_id:
                            if game_id not in active_games:
                                active_games.add(game_id)                            
                                future = executor.submit(listen_to_one_game_moves, game_id, producer_move)
                                callback = partial(on_game_finished, game_id, active_games, producer_game ,producer_elo , producer_user)
                                future.add_done_callback(callback)                

                            if len(active_games) >= max_workers:
                                break # Dừng thêm game mới nếu đã đầy slot
                time.sleep(time_sleep)
    except Exception as e:
        print(e)
        return False

def safe_api_call(func, *args, **kwargs):
    """
    Wrapper để gọi API an toàn. Nếu gặp 429, đợi và thử lại.
    """
    while True:
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if hasattr(e, 'response') and e.response.status_code == 429:
                wait_time = int(e.response.headers.get("Retry-After", 30)) # Lichess bảo đợi bao lâu thì đợi bấy nhiêu
                print(f"[CẢNH BÁO] Bị rate limit! Chờ {wait_time}s...")
                time.sleep(wait_time)
            else:
                raise e

if __name__ == "__main__":
    producer_game = MyProducer(topic_name='game')
    producer_user = MyProducer(topic_name='user')
    producer_elo = MyProducer(topic_name='elo')
    producer_move = MyProducer(topic_name='move')
    max_workers = 50 
    time_sleep= 10
    listen_to_multi_game_moves_in_channel('bullet' , producer_game , producer_elo , producer_user , producer_move, max_workers , time_sleep)
    # safe_api_call(listen_to_multi_game_moves_in_channel, 'bullet' , producer_game , producer_elo , producer_user , producer_move )
