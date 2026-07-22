import requests
from bs4 import BeautifulSoup


def get_all_channel():
    try:
        headers = {
            "User-Agent": "dangkiman41005@gmail.com/1.0"
        }
        response = requests.get('https://lichess.org/games' , headers=headers)
        if response.status_code == 200:
            html = response.text
            soup = BeautifulSoup(html , 'html.parser')
            a_tags = soup.select('a')
            hrefs = [a_tag.get('href')  for a_tag in a_tags if a_tag.get('href') and a_tag.get('href').startswith('/games/')]
            list_channel = [href.split('/')[2] for href in hrefs if len(href.split('/'))== 3]
            return list_channel
        else:
            raise Exception('Get failed')
    except Exception as e:
        print(e)
        return False
    