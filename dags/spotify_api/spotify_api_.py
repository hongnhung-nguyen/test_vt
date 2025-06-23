import base64
import json
import time
from requests import post, get, exceptions
from spotify_api.config_para import client_id, client_secret

def get_token():
    auth_string = client_id + ":" + client_secret
    auth_bytes=auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    url ="https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " +auth_base64,
        "Content-Type" : "application/x-www-form-urlencoded"
    }
    data = { "grant_type": "client_credentials"}

    result = post(url, headers=headers, data= data)
    json_result =json.loads(result.content)

    if "access_token" in json_result:
        return json_result["access_token"]
    else:
        # Trường hợp không có access_token, ex: lỗi xác thực...
        print(f"Error getting access token: {json_result.get('error_description', json_result.get('error', 'Unknown error'))}")
        raise KeyError("Access token not found in response.")

def get_auth_header(token):
    return {"Authorization": "Bearer " + token}

def search_for_genre_of_Artists(token, genre):
    search_url ="https://api.spotify.com/v1/search"
    headers = get_auth_header(token)

    all_artists= []
    current_offset = 0 #index of first result to return
    limit_per_request = 50  #số lượng artists trả ra với mỗi request
    check_list= set() #list dùng để kiểm tra là nghệ sĩ đó đã có trong list chưa trước khi thêm vào
    ###
    print("start here")
    ##
    while True:
        params ={
            'q': f'genre:{genre}',
            'type': 'artist',
            'limit': limit_per_request,
            'offset': current_offset

        }
        print(f'Fetching artists at offset {current_offset} ')
        try:
            result = get(search_url, headers=headers, params=params)
            result.raise_for_status() # Kiểm tra lỗi HTTP
            json_result = json.loads(result.content)
            print(json_result )

            # chỉ lấy giá trị total ở lần đầu tiên mà không cần ở những lần sau
            if(current_offset ==0):
                total_artists_found = json_result["artists"]["total"]
            artists_on_page = json_result['artists']['items']
            if not artists_on_page:
                print("No more artists found for this genre and offset. Stopping.")
                break
            
            print(artists_on_page)

            #lấy ra list artist của từng response trả về
            for artist in artists_on_page :
                artist_id = artist['id']
                if artist_id not in check_list and genre in artist['genres']:
                    all_artists.append({                   
                        'id': artist['id'],
                        'name': artist['name'],
                        'popularity': artist['popularity'],
                        'genres': ', '.join(artist['genres']) if artist['genres'] else None,
                        'followers_total': artist['followers']['total'],
                        'external_url_spotify': artist['external_urls']['spotify'],
                        'image_url': artist['images'][0]['url'] if artist['images'] else None
                    })
                    check_list.add(artist_id)
                else:
                    continue

            
            current_offset += limit_per_request 

            #kiểm tra đã lấy đủ số lượng artists mà truy vấn tìm được hay chưa
            if current_offset >= total_artists_found:
                print(f"Reached or exceeded total artists found ({total_artists_found}). Stopping.")
                break

            #nếu số lượng request nhiều gây nên việc cần phải chờ để được request lại
            if 'Retry-After' in result.headers:
                delay = int(result.headers['Retry-After'])
                print(f"Rate limited by Spotify. Waiting for {delay} seconds before next request.")
                time.sleep(delay)
        except exceptions.RequestException as e:
            print(f"Error fetching list artists by genre '{genre}' at offset {current_offset}: {e}")
            break
    return all_artists       

