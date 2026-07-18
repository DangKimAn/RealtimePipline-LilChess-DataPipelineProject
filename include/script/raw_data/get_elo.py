


def get_elo_for_one_user(username , prefs):
    data_elo = {
        'username' : [username]*(len(prefs.keys())),
        'type' : [],
        'num_games' : [],
        'rating' : [],
        'rd' :[],
        'prog' :[],
        'prov':[]
    }
    for key in prefs.keys():
        data_elo['num_games'].append(prefs[key].get('games') if prefs[key].get('games') else 0)
        data_elo['rd'].append(prefs[key].get('rd') if prefs[key].get('rd') else 0)
        data_elo['rating'].append(prefs[key].get('rating') if prefs[key].get('rating') else 0)
        data_elo['prog'].append(prefs[key].get('prog') if prefs[key].get('prog') else 0)
        data_elo['prov'].append(prefs[key].get('prov') if prefs[key].get('prov') else False)
        data_elo['type'].append(key)
    return data_elo