import asyncio
import aiohttp
import json
from pymongo import MongoClient
from dateutil import parser
from pandas import DataFrame as df
import pandas

TOKEN = "6259873610:AAEoPOxowITn7nM8fT2r3yB4MZ0PPV6hmA4"
DB_NAME = "sampleDB"

TELEGRAM_API = "https://api.telegram.org/bot"
DB_CONN = "mongodb://localhost:27017"

class Payments:

    group_freq_dict = {"month": "M", "day": "D", "hour": "H"}

    async def payment_calc(self, dt_from: str, dt_upto: str, group_type: str, data_frame: df):
        
        try:
            start_date = parser.parse(dt_from)
            end_date = parser.parse(dt_upto)
            group_freq = self.group_freq_dict[group_type]
        except e:
            print(e)

        df = data_frame[data_frame["dt"].between(start_date, end_date, inclusive = "both")]

        group_freq = self.group_freq_dict[group_type]
        result_payments = df.groupby(pandas.Grouper(key = "dt", freq=group_freq))["value"].sum()
        
        print("Result:", result_payments)

        result_df = result_payments.to_frame()
        
        result_df.reset_index(inplace=True)
        result_df = result_df.rename(columns = {'dt':'labels', 'value': 'dataset'})

        return {
            "labels": json.loads(result_df["labels"].to_json(orient="split", index=False, date_format="iso"))["data"], 
            "dataset": json.loads(result_df["dataset"].to_json(orient="split", index=False))["data"]
            }


class DB:
    def __init__(self):
        self.db = MongoClient(DB_CONN)[DB_NAME]
    
    async def get_all_items(self):
        return self.db["sample_collection"].find()


class Bot:

    last_id_update = 0

    def __init__(self, bot_token: str):
        self.api_url = TELEGRAM_API + bot_token
        self.payments = Payments()
        self.db = DB()

    async def run(self):
        
        while True:
            async with aiohttp.ClientSession() as session:
                
                try:
                    updates = await self.receive_updates(session)
                except:
                    print("error receiving updates")

                if updates == None:
                    print("None updates")
                    continue

                for update in updates:
                    await self.update_handler(update, session)


    async def receive_updates(self, session):
        
        updates_url = self.api_url + "/getUpdates"
        params_updates_url = {"offset": self.last_id_update}

        async with session.get(updates_url, params = params_updates_url) as response:

            resp_json = await response.json()
            count_updates = len(resp_json["result"])
            
            if count_updates <= 0:
                return None
        
            self.last_id_update = resp_json["result"][-1]["update_id"] + 1
            return resp_json["result"]

    async def update_handler(self, update: str, session):

        try:
            chat_id = update["message"]["from"]["id"]
            username = update["message"]["from"]["username"]
            message_text = update["message"]["text"]
            
            json_message_text = json.loads(message_text)

            dt_from = json_message_text["dt_from"]
            dt_upto = json_message_text["dt_upto"]
            group_type = json_message_text ["group_type"]

            db_data = await self.db.get_all_items()

            result = await self.payments.payment_calc(dt_from, dt_upto, group_type, df(data=db_data))

            print(result)

            send_message_url = self.api_url + "/sendMessage"
            params_send_message_url = {"chat_id": chat_id, "text": json.dumps(result)}

            async with session.get(send_message_url, params = params_send_message_url) as response:
                print(response)
        
        except:
            print("Invalid json response")


if __name__ == "__main__":
    
    bot = Bot(TOKEN)
    
    asyncio.run(bot.run())