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


def get_users_bulk(usernames):
    """
    Lấy thông tin nhiều user cùng lúc bằng Lichess Bulk API.
    usernames: list chứa các username (VD: ['nguoi_choi_1', 'nguoi_choi_2'])
    """
    url = 'https://lichess.org/api/users'
    # Lichess yêu cầu body là chuỗi text các user cách nhau bằng dấu phẩy
    data = ",".join(usernames)
    headers = {'Content-Type': 'text/plain'}
    
    # Khởi tạo dict chứa các list rỗng
    data_users = {
        'id': [], 'username': [], 'title': [], 'created_at': [], 
        'location': [], 'real_name': [], 'fide_rating': [], 
        'links': [], 'bio': [], 'play_time': [], 'url': [], 
        'all_count': [], 'rated_count': [], 'draw_count': [], 
        'loss_count': [], 'win_count': []
    }
    data_elos = {} 

    try:
        response = requests.post(url=url, data=data, headers=headers)
        print(f'Get users bulk {usernames} status {response.status_code}')
        
        if response.status_code == 200:
            json_data_list = response.json() 
            for json_data in json_data_list:
                profile = json_data.get('profile', {})
                count = json_data.get('count', {})
                play_time = json_data.get('playTime', {})
                
                # Append dữ liệu của từng user vào list tương ứng
                data_users['id'].append(json_data.get('id'))
                data_users['username'].append(json_data.get('username'))
                data_users['title'].append(json_data.get('title'))
                data_users['created_at'].append(json_data.get('createdAt'))
                data_users['location'].append(profile.get('location', ''))
                data_users['real_name'].append(profile.get('realName', ''))
                data_users['fide_rating'].append(profile.get('fideRating', ''))
                data_users['links'].append(profile.get('links', ''))
                data_users['bio'].append(profile.get('bio', ''))
                data_users['play_time'].append(play_time.get('total'))
                data_users['url'].append(json_data.get('url'))
                data_users['all_count'].append(count.get('all'))
                data_users['rated_count'].append(count.get('rated'))
                data_users['draw_count'].append(count.get('draw'))
                data_users['loss_count'].append(count.get('loss'))
                data_users['win_count'].append(count.get('win'))

                # Xử lý Elo cho user hiện tại
                user_elo = get_elo_for_one_user(username=json_data.get('username'), prefs=json_data.get('perfs'))
                # if not data_elos and user_elo:
                #     data_elos = {k: [] for k in user_elo.keys()}
                
                # if user_elo:
                #     for k in user_elo.keys():
                #         data_elos[k].extend(user_elo[k])
                # print(data_users)
                # print(user_elo)
            return data_users, user_elo
            
    except Exception as e:
        print(f"Lỗi khi gọi Bulk API: {e}")
        return {}, {}
        
    return {}, {}