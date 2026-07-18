import requests 
import json 
import pandas as pd
from get_elo import get_elo_for_one_user
def get_user(username):
    url = f'https://lichess.org/api/user/{username}'
    data_user = {}
    try:
        response = requests.get(url=url)
        print(f'get user {username} status {response.status_code}')
        if response.status_code == 200:
            json_data = response.json()
            # print(json)
            data_user['id'] = [json_data.get('id')]
            data_user['username'] = [json_data.get('username')]
            data_user['title'] = [json_data.get('title')]
            data_user['created_at'] = [json_data.get('createdAt')]
            # data_user['profile'] = [json_data.get('profile')]
            data_user['location'] = [json_data.get('profile').get('location')if json_data.get('profile')else '']
            data_user['real_name'] = [json_data.get('profile').get('realName')if json_data.get('profile')else '']
            data_user['fide_rating'] = [json_data.get('profile').get('fideRating')if json_data.get('profile')else '']
            data_user['links'] = [json_data.get('profile').get('links')if json_data.get('profile')else '']
            data_user['bio'] = [json_data.get('profile').get('bio')if json_data.get('profile')else '']
            data_user['play_time'] = [json_data.get('playTime').get('total')]
            data_user['url'] = [json_data.get('url')]
            data_user['all_count'] = [json_data.get('count').get('all')]
            data_user['rated_count'] = [json_data.get('count').get('rated')]
            data_user['draw_count'] = [json_data.get('count').get('draw')]
            data_user['loss_count'] = [json_data.get('count').get('loss')]
            data_user['win_count'] = [json_data.get('count').get('win')]

            data_elo = get_elo_for_one_user(username=json_data.get('username') ,prefs=json_data.get('perfs'))
            return data_user ,data_elo

    except Exception as e:
        print(e)
        return False
    
# pd.set_option('display.max_columns', 100)

# data_user , data_elo = get_user(username='konstantinkazakov')

# print(pd.DataFrame(data_elo))
# print(pd.DataFrame(data_user))