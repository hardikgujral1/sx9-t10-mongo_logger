import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from datetime import datetime
import aiohttp
from getMatchDetail import fetch_t20_exchange_ids

uri = "mongodb://172.25.2.209:27017/betting?directConnection=true"

async def fetch_data_from_api(id):
    start_time = datetime.now()
    url = f"http://35.154.195.1:8000/api/matchData/{id}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                end_time = datetime.now()
                if response.status == 200:
                    print(f"API request took: {(end_time - start_time).total_seconds() * 1000} ms")
                    return await response.json()
                else:
                    print(f"Failed to fetch data for game_id {id}. Status code: {response.status}")
                    return None
        except aiohttp.ClientError as e:
            print(f"An error occurred while fetching data for game_id {id}: {e}")
            return None

async def update_market(game_id, api_data, event_id):
    start_time = datetime.now()
    update_tasks = [
        update_random_data(generate_data_from_api(api_data, game_id, i, event_id))
        for i in range(len(api_data['data']))
    ]
    await asyncio.gather(*update_tasks)
    end_time = datetime.now()
    print(f"Database update took: {(end_time - start_time).total_seconds() * 1000} ms")
    print("Updated all documents in parallel.")

async def get_id(event_id):
    event_id = str(event_id)
    query = {'Meta.t20exchange_id': event_id}
    try:
        document = await games_collection.find_one(query)
        if document:
            game_id = document['_id']
            api_data = await fetch_data_from_api(event_id)
            if api_data:
                await update_market(game_id, api_data, event_id)
        else:
            print(f"Document with event_id {event_id} is NOT present in the database.")
    except Exception as e:
        print(f"An error occurred while processing event_id {event_id}: {e}")

def generate_data_from_api(api_data, game_id, i, event_id):
    try:
        response_data = api_data['data'][i]
        data = {
            "GameId": game_id,
            "Name": response_data['Name'],
            "Back": response_data['Back'],
            "BackText": response_data['BackText'],
            "BallByBallStatus": 0,
            "BetCount": 0,
            "BookMarketName": response_data['BookMarketName'],
            "CustomStatus": 0,
            "DesFlag": 0,
            "Difference": 0,
            "EightEleven": 0,
            "EventId": str(event_id),
            "ForceSuspend": 0,
            "ForceSuspendOrigin": None,
            "GameInplay": response_data.get('GameInplay', None),
            "HideOnFront": False,
            "Lay": response_data['Lay'],
            "LayText": response_data['LayText'],
            "MaxProLossRun": 0,
            "MaxRate": response_data['MaxRate'],
            "MaximumMarket": 0,
            "Meta": {},
            "MinRate": response_data['MinRate'],
            "NineEleven": 0,
            "Overlay": 0,
            "RatePerRun": 0,
            "RateType": "Odds",
            "Result": None,
            "ResultInitiated": None,
            "Same": 0,
            "SevenTwentyFive": 0,
            "SkyBookmakerId": None,
            "SkyFancyId": None,
            "SkyMarketId": response_data['SkyMarketId'],
            "SkySelectionId": None,
            "Status": 1 if response_data['Status'].lower() == 'suspended' else 0 or 2 if response_data['Status'] == 'Ball Running' else 0 or 1 if response_data['Status'].lower() == 'closed' else 0,
            "Sub": [],
            "Suspend": 1 if response_data['Status'].lower() == 'suspended' else 0 or 2 if response_data['Status'] == 'Ball Running' else 0 or 1 if response_data['Status'].lower() == 'closed' else 0,
            "__v": 0,
            "createdAt": datetime.utcnow(),
            "indiviDualShowHide": 0,
            "indvidualMinMax": 0,
            "marketStatus": "CLOSED"
        }
        return data
    except IndexError:
        print(f"IndexError: No data found for index {i} in API response")
        return None

async def update_random_data(data):
    try:
        await rate_collection.update_one(
            {"SkyMarketId": data["SkyMarketId"], "GameId": data["GameId"]},
            {"$set": data},
            upsert=True
        )
    except Exception as e:
        print(f"An error occurred while updating data: {e}")

async def main():
    client = AsyncIOMotorClient(uri, maxPoolSize=50)
    db = client["betting"]
    global games_collection
    games_collection = db["games"]
    global rate_collection
    rate_collection = db["rates"]

    
    while True:
        t20_exchange_ids = await fetch_t20_exchange_ids()
        tasks = [get_id(event_id) for event_id in t20_exchange_ids]
        await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())
