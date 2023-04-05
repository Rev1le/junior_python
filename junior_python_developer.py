import asyncio
import aiohttp
import json
from pymongo import MongoClient
from dateutil import parser
import pandas as pd

TOKEN = "6259873610:AAEoPOxowITn7nM8fT2r3yB4MZ0PPV6hmA4"
DB_NAME = "sampleDB"

TELEGRAM_API = "https://api.telegram.org/bot"
DB_CONN = "mongodb://localhost:27017"

class Payments:

    group_freq_dict = {"month": "MS", "day": "D", "hour": "H"}

    async def payment_calc(self, dt_from: str, dt_upto: str, group_type: str, data_frame):
        
        try:
            start_date = parser.parse(dt_from)
            end_date = parser.parse(dt_upto)
            group_freq = self.group_freq_dict[group_type]
        except e:
            print(e)

        # Making a cut of the dates
        df = data_frame[data_frame["dt"].between(start_date, end_date, inclusive = "both")]

        # Generated dates 
        date_range = pd.date_range(start=start_date, end=end_date, freq = group_freq)

        d = {'dt': date_range, 'value': 0,}
        df_datas = pd.DataFrame(data=d)                                   

        # Concat dataframes 
        df = pd.concat([df, df_datas])
        print("Result:", df)

        # Group by dates and calculate the amount of value for the period group_frequency

        group_freq = self.group_freq_dict[group_type]
        result_payments = df.groupby(pd.Grouper(key = "dt", freq=group_freq))["value"].sum()

        result_df = result_payments.to_frame()
        result_df.reset_index(inplace=True)

        # Rename columns for correct output
        result_df = result_df.rename(columns = {'dt':'labels', 'value': 'dataset'})

        return {
            "dataset": json.loads(result_df["dataset"].to_json(orient="split", index=False))["data"],
            "labels": json.loads(result_df["labels"].to_json(orient="split", index=False, date_format="iso", date_unit="s"))["data"]
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
        
        except:
            send_message_url = self.api_url + "/sendMessage"
            params_send_message_url = {"chat_id": chat_id, "text": "Incorrected json message"}

            async with session.get(send_message_url, params = params_send_message_url) as response:
                print(response)

        try:
            
            # Result payments in dict format
            result = await self.payments.payment_calc(dt_from, dt_upto, group_type, pd.DataFrame(data=db_data))
            
            send_message_url = self.api_url + "/sendMessage"
            params_send_message_url = {"chat_id": chat_id, "text": json.dumps(result)}
            async with session.get(send_message_url, params = params_send_message_url) as response:
                print(response)
        
        except:
            send_message_url = self.api_url + "/sendMessage"
            params_send_message_url = {"chat_id": chat_id, "text": "Payment calculation error"}

            async with session.get(send_message_url, params = params_send_message_url) as response:
                print(response)



if __name__ == "__main__":
    
    bot = Bot(TOKEN)
    
    asyncio.run(bot.run())