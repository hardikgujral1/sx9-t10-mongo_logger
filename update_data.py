import requests
from pymongo import MongoClient
from datetime import datetime

uri = "mongodb://172.25.2.209:27017/betting?directConnection=true"

def fetch_data_and_update():
    try:
        response = requests.get("http://35.154.195.1:8000/api/getEventList?query=1")
        response.raise_for_status()
        data = response.json()
        if data and 'data' in data and 'events' in data['data']:
            fetched_events = data['data']['events']
            for event in fetched_events:
                if event:
                    meta = {
                        't20exchange_id': str(event.get('event_id', '')),
                        'sport_t20exchange_id': str(event.get('event_type_id', '')),
                        'event_t20exchange_id': '',
                        'market_t20exchange_id': ''
                    }
                    default_values = {
                        'Meta': meta,
                        'Name': event.get('name', '').strip(),
                        'BookmakerAvailable': event.get('bm_active', 0),
                        'BookmakerStatus': 'false',
                        'EventName': '',
                        'Fancy1Available': 0,
                        'FancyAvailable': 0,
                        'GameSubType': 1,
                        'GameTai': 0,
                        'GameType': 'CRCKT',
                        'InPlay': event.get('in_play', 0) == 1,
                        'IplSeason': False,
                        'IsSettlement': 0,
                        'MatchDateTime': event.get('open_date_format', ''),
                        'MatchOddStatus': 'false',
                        'MaxRate': 0,
                        'MinRate': 0,
                        'OddDifferent': 0,
                        'OddsAvailable': 0,
                        'RateType': 'Paisa',
                        'Same': 0,
                        'Status': 0,
                        'Suspend': 0,
                        'createdAt': datetime.utcnow(),
                        'customGame': False,
                        'isEventType': 'virtual',
                        'streamingUrl': event.get('tv_channel', '')
                    }
                    result = collection.update_one(
                        {'Name': event.get('name', '').strip(), 'Meta.t20exchange_id': str(event.get('event_id', ''))},
                        {'$set': default_values},
                        upsert=True
                    )

                    if result.matched_count == 0: 
                        print(f"Inserted: {default_values['Name']}, Event ID: {meta['t20exchange_id']}")
                    else:
                        print(f"Updated: {default_values['Name']}, Event ID: {meta['t20exchange_id']}")
    except requests.RequestException as e:
        print('HTTP request error:', e)
    except Exception as e:
        print('An error occurred:', e)

if __name__ == "__main__":
    client = MongoClient(uri)
    db = client["betting"]
    collection = db["games"]
    fetch_data_and_update()
