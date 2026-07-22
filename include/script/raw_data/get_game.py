import requests
from get_channel import get_all_channel
from bs4 import BeautifulSoup

from concurrent.futures import ThreadPoolExecutor,as_completed

def get_game_id_a_channel(channel):
    url = f'https://lichess.org/games/{channel}'
    headers = {
        "User-Agent": "dangkiman41005@gmail.com/1.0"
    }

    try:
        response = requests.get(url=url, headers=headers, timeout=10)
        if response.status_code == 200:
            html = response.text
            soup =  BeautifulSoup(html, 'html.parser')
            now_playing = soup.find( 'div' , class_ ='page-menu__content now-playing')
            a_tags = now_playing.select('a')
            list_game_id = [a_tag.get('data-live') for a_tag in a_tags if a_tag.get('data-live')]
            return {channel : list_game_id} 
        else:
            raise Exception(f'Cannot get channel')
    except Exception as e:
        print(e)



def get_game_id_multi_channel(list_channel, dict_list_game_id):
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(
                get_game_id_a_channel,
                channel=channel
            ) for channel in list_channel
        ]

        for future in as_completed(futures):
            result = future.result()
            if result:
                for key, val in result.items():
                    if key in dict_list_game_id:
                        dict_list_game_id[key].extend(val)
                    else:
                        dict_list_game_id[key] = val

    return dict_list_game_id

