import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

uri = "mongodb://172.25.2.209:27017/betting?directConnection=true"
client = AsyncIOMotorClient(uri, maxPoolSize=50)
db = client["betting"]
collection = db["games"]
async def fetch_t20_exchange_ids():
    
    
    query = {
        'isEventType': 'virtual',
        'GameType': 'CRCKT'
    }
    
    t20_exchange_ids = await collection.distinct('Meta.t20exchange_id', query)

    return [t20_exchange_id for t20_exchange_id in t20_exchange_ids if t20_exchange_id]
