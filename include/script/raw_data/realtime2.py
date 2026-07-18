import json
import requests
import time
import sys
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial

# Thêm thư mục `script` (thư mục cha) vào sys.path để import package kafka
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from get_channel import get_all_channel
from get_game import get_game_id_a_channel
from get_user import get_user
from kafka.producer import MyProducer


MAX_GAMES_PER_CHANNEL = 5

MAX_CHANNEL_WORKERS = 20

CHANNEL_POLL_INTERVAL = 20

SKIP_GAME_FETCH_PREFIX = "search"


# ─────────────────────────────────────────────────────────────────────────────
# LAYER 1 – Lắng nghe 1 ván cờ
# ─────────────────────────────────────────────────────────────────────────────

def listen_to_one_game_moves(game_id, producer_move):
    """Stream nước đi realtime của 1 ván cờ, gửi từng nước vào Kafka topic 'move'."""
    url = f"https://lichess.org/api/stream/game/{game_id}"
    headers = {
        "Accept": "application/x-ndjson",
        "User-Agent": "dangkiman41005@gmail.com/1.0"
    }

    response = requests.get(url, stream=True, headers=headers)
    print(f"[GAME] {game_id} – kết nối HTTP {response.status_code}")

    data_moves = {
        'game_id': [],
        'num_of_moves': [],
        'color_turn': [],
        'fen': [],
        'time_left': [],
        'move': [],
        'prev_fen': []
    }
    data_game = {}
    prev_fen = ''
    get_user_white_id_after = {}
    get_user_white_elo_after = {}
    get_user_black_id_after = {}
    get_user_black_elo_after = {}

    if response.status_code != 200:
        raise RuntimeError(f"Kết nối API thất bại – ván {game_id} – HTTP {response.status_code}")

    print(f"[GAME] {game_id} – đang stream nước đi...")
    for line in response.iter_lines():
        if not line:
            continue
        raw_data = json.loads(line.decode('utf-8'))

        # ── Nước đi mới ──────────────────────────────────────────────────────
        if raw_data.get('lm'):
            fen_parts = str(raw_data.get('fen')).split(' ')
            color = str.lower(fen_parts[1])           # 'w' hoặc 'b'
            move_number = int(fen_parts[-1])

            msg = {
                'game_id':      game_id,
                'fen':          raw_data.get('fen'),
                'move':         raw_data.get('lm'),
                'time_left':    int(raw_data.get('wc')) if color == 'b' else int(raw_data.get('bc')),
                'num_of_moves': move_number if color == 'b' else move_number - 1,
                'color_turn':   'white' if color == 'b' else 'black',
                'prev_fen':     prev_fen
            }
            prev_fen = raw_data.get('fen')
            producer_move.send_message(key=game_id, value=json.dumps(msg))

            # Ghi vào dict nội bộ (tùy chọn, dùng nếu cần debug)
            for k, v in msg.items():
                data_moves[k].append(v)

        # ── Ván kết thúc ─────────────────────────────────────────────────────
        elif raw_data.get('winner'):
            winner = raw_data.get('winner')
            players = raw_data.get('players', {})
            data_game = {
                'game_id':      game_id,
                'winner':       winner,
                'winner_id':    players.get(winner, {}).get('user', {}).get('id'),
                'status_name':  raw_data.get('status', {}).get('name'),
                'status_id':    raw_data.get('status', {}).get('id'),
                'turns':        raw_data.get('turns'),
                'white_id':     players.get('white', {}).get('user', {}).get('id'),
                'black_id':     players.get('black', {}).get('user', {}).get('id'),
                'rated':        raw_data.get('rated'),
                'source':       raw_data.get('source'),
                'speed':        raw_data.get('speed'),
                'perf':         raw_data.get('perf'),
                'createdAt':    raw_data.get('createdAt'),
            }
            try:
                get_user_white_id_after, get_user_white_elo_after = get_user(
                    username=players.get('white', {}).get('user', {}).get('id'))
                time.sleep(3)
                get_user_black_id_after, get_user_black_elo_after = get_user(
                    username=players.get('black', {}).get('user', {}).get('id'))
            except Exception as e:
                print(f"[GAME] {game_id} – không lấy được thông tin user: {e}")

        # ── Tin nhắn khởi tạo (gameFull) ────────────────────────────────────
        elif raw_data.get('id') and not raw_data.get('lm'):
            white_info = raw_data.get('white', {})
            black_info = raw_data.get('black', {})
            if isinstance(white_info, dict) and 'rating' in white_info:
                data_game['white_rating_before'] = white_info.get('rating')
                data_game['black_rating_before'] = black_info.get('rating')

    # Merge dữ liệu user sau khi ván kết thúc
    data_user = {
        k: get_user_white_id_after.get(k, []) + get_user_black_id_after.get(k, [])
        for k in get_user_white_id_after
    }
    data_elo = {
        k: get_user_white_elo_after.get(k, []) + get_user_black_elo_after.get(k, [])
        for k in get_user_white_elo_after
    }
    return data_moves, data_game, data_user, data_elo


def on_game_finished(game_id, active_games, producer_game, producer_elo, producer_user, future):
    active_games.discard(game_id)
    try:
        _, data_game, data_user, data_elo = future.result()
        print(f"[DONE] Ván {game_id} kết thúc – đang gửi Kafka...")

        if data_game:
            producer_game.send_message(key=game_id, value=json.dumps(data_game))
        else:
            print(f"[WARN] {game_id} – không có data_game")

        if data_user:
            producer_user.send_message(key=game_id, value=json.dumps(data_user))
        else:
            print(f"[WARN] {game_id} – không có data_user")

        if data_elo:
            producer_elo.send_message(key=game_id, value=json.dumps(data_elo))
        else:
            print(f"[WARN] {game_id} – không có data_elo")

    except Exception as e:
        print(f"[ERR] Ván {game_id} kết thúc với lỗi: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# LAYER 3 – Lắng nghe nhiều game trong 1 kênh
# ─────────────────────────────────────────────────────────────────────────────

def listen_channel(channel, executor, active_games_map,
                   producer_game, producer_elo, producer_user, producer_move):
    """
    Vòng lặp vô hạn cho 1 kênh:
      - Poll danh sách game mới từ kênh
      - Submit các game chưa được theo dõi vào executor dùng chung
    """
    active_games = active_games_map.setdefault(channel, set())
    print(f"[CHANNEL] Bắt đầu theo dõi kênh: {channel}")

    while True:
        if len(active_games) < MAX_GAMES_PER_CHANNEL:
            result = get_game_id_a_channel(channel)
            if result is None:
                print(f"[CHANNEL] {channel} – không lấy được danh sách game, thử lại sau {CHANNEL_POLL_INTERVAL}s")
                time.sleep(CHANNEL_POLL_INTERVAL)
                continue

            list_game_id = result.get(channel, [])[:MAX_GAMES_PER_CHANNEL]

            for game_id in list_game_id:
                if game_id not in active_games:
                    active_games.add(game_id)
                    future = executor.submit(listen_to_one_game_moves, game_id, producer_move)
                    callback = partial(
                        on_game_finished,
                        game_id, active_games,
                        producer_game, producer_elo, producer_user
                    )
                    future.add_done_callback(callback)
                    print(f"[CHANNEL] {channel} – thêm game {game_id} (đang chạy: {len(active_games)})")

                if len(active_games) >= MAX_GAMES_PER_CHANNEL:
                    break

        time.sleep(CHANNEL_POLL_INTERVAL)


# ─────────────────────────────────────────────────────────────────────────────
# LAYER 4 – Điều phối toàn bộ kênh
# ─────────────────────────────────────────────────────────────────────────────

def listen_to_multi_channels(producer_game, producer_elo, producer_user, producer_move):
    """
    Lấy danh sách kênh từ lichess.org/games rồi lắng nghe tất cả kênh song song.

    Lưu ý:
      - Phần tử ĐẦU TIÊN của danh sách kênh (thường là kênh 'search' / top)
        sẽ bị BỎ QUA hoàn toàn – không poll game từ kênh này.
    """
    list_channel = get_all_channel()
    if not list_channel:
        print("[MAIN] Không lấy được danh sách kênh. Dừng lại.")
        return

    print(f"[MAIN] Tổng số kênh tìm được: {len(list_channel)}")
    print(f"[MAIN] Kênh đầu tiên (sẽ bỏ qua): '{list_channel[0]}'")

    # ── Lọc bỏ phần tử đầu tiên (kênh search/top) ───────────────────────────
    first_channel = list_channel[0]
    if first_channel.startswith(SKIP_GAME_FETCH_PREFIX) or list_channel:
        # Theo yêu cầu: phần tử đầu tiên luôn bị bỏ qua
        channels_to_watch = list_channel[1:]
        print(f"[MAIN] Bỏ qua kênh đầu tiên '{first_channel}' (search/top), "
              f"theo dõi {len(channels_to_watch)} kênh còn lại.")
    else:
        channels_to_watch = list_channel

    # Executor dùng chung cho TẤT CẢ game trên MỌI kênh
    total_max_workers = MAX_CHANNEL_WORKERS * MAX_GAMES_PER_CHANNEL
    active_games_map = {}   # channel -> set(game_id đang chạy)

    with ThreadPoolExecutor(max_workers=total_max_workers) as game_executor:
        # Mỗi kênh chạy trong 1 luồng riêng (channel-level concurrency)
        with ThreadPoolExecutor(max_workers=MAX_CHANNEL_WORKERS) as channel_executor:
            channel_futures = {
                channel_executor.submit(
                    listen_channel,
                    channel,
                    game_executor,
                    active_games_map,
                    producer_game,
                    producer_elo,
                    producer_user,
                    producer_move
                ): channel
                for channel in channels_to_watch
            }

            for future in as_completed(channel_futures):
                channel = channel_futures[future]
                try:
                    future.result()
                except Exception as e:
                    print(f"[ERR] Kênh '{channel}' gặp lỗi: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    producer_game  = MyProducer(topic_name='game')
    producer_user  = MyProducer(topic_name='user')
    producer_elo   = MyProducer(topic_name='elo')
    producer_move  = MyProducer(topic_name='move')

    listen_to_multi_channels(
        producer_game=producer_game,
        producer_elo=producer_elo,
        producer_user=producer_user,
        producer_move=producer_move
    )
