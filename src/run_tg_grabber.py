"""
Telegram channel message grabber
"""
from datetime import datetime, timedelta
from typing import NoReturn, Union
import json
import os

import pandas as pd
from omegaconf import OmegaConf
from dotenv import load_dotenv
from telethon import hints
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from mysql.connector import connect, Error

from utils import getLogger
import constants

# Load environment variables from .env
load_dotenv()

# Reading constants
input_path = constants.INPUT_PATH
logs_path = constants.LOGS_PATH
logs_file = constants.LOGS_FILE
config_path = constants.CONFIG_PATH

# Add logging
logger = getLogger(
    name=__name__, 
    file_name=logs_file,
    format="%(asctime)s: %(levelname)s: %(name)s: %(message)s",
    date_format="%Y-%m-%d %H:%M:%S"
)

logger.info("Reading configuration")
config = OmegaConf.load(config_path)

logger.info("Reading connection params from environment variables")
api_id = os.getenv("TG_API_ID")
api_hash = os.getenv("TG_API_HASH")
phone = os.getenv("TG_PHONE")
mysql_host = os.getenv("MYSQL_HOST")
mysql_user = os.getenv("MYSQL_USER")
mysql_password = os.getenv("MYSQL_PASSWORD")
database = config.mysql.database
table = config.mysql.table

logger.info("Creating a telegram client")
client = TelegramClient(logs_path + 'tg_grabber', api_id, api_hash)
client.start(phone=phone)

class DateTimeEncoder(json.JSONEncoder):
    """
    Class serialize dates to JSON
    """
    def default(self, o) -> json.JSONEncoder.default:
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, bytes):
            return list(o)
        return json.JSONEncoder.default(self, o)


async def dump_all_messages(
        channel: hints.Entity,
        out_file_name: str,
        limit_msg: int,
        channel_name: Union[None, str] = None,
        channel_url: Union[None, str] = None
        ) -> NoReturn:
    """
    The function grabs all news from the given input channel
    from yesterday.
    The function is supposed to run on everyday schedule in a Docker container
    using crontab.

    Args:
        channel (hints.Entity): Telethon channel object.
        out_file_name (str): File to save grabbed news.
        limit_msg (int): Maximum number of messages to receive.
        channel_name (Union[None, str], optional): Channel name. Defaults to None.
        channel_url (Union[None, str], optional): Channel url. Defaults to None.

    Returns:
        NoReturn
    """
    # Get dates to filter out some news
    current_date = (datetime.today() + timedelta(hours=3)).date()
    date_start = current_date - timedelta(days=1)
    date_start = datetime(
        date_start.year,
        date_start.month, 
        date_start.day
    )
    all_messages = []
    while True:
        history = await client(GetHistoryRequest(
            peer=channel,
            offset_id=0,
            offset_date=current_date,
            add_offset=0,
            limit=limit_msg,
            max_id=0,
            min_id=0,
            hash=0
        ))
        if not history.messages:
            break
        
        # Get messages until unrequired date met
        messages = history.messages
        for message in messages:
            message = message.to_dict()
            message_prepared = {}
            # Convert message date to timezone MSK
            message_date_utc = message["date"].replace(tzinfo=None)
            message_date_msk = message_date_utc + timedelta(hours=3)
            if message_date_msk >= date_start and message_date_msk.date() < current_date:
                message_prepared["message_id"] = message["id"] 
                message_prepared["channel_id"] = message["peer_id"]["channel_id"]
                message_prepared["channel_name"] = channel_name
                message_prepared["channel_url"] = channel_url
                message_prepared["date"] = message_date_msk
                message_prepared["text"] = message["message"]
                all_messages.append(message_prepared)
            elif message_date_msk < date_start:
                total_messages = len(all_messages)
                logger.info(
                    "Number of news grabbed from channel '{}': {}". \
                        format(channel_name, total_messages)
                )
                logger.info(
                    "Saving data from channel '{}' to database". \
                        format(channel_name)
                )

                # Connect to database and save messages
                records = [tuple(message.values()) for message in all_messages]
                insert_query = \
                f"""
                INSERT IGNORE INTO {database}.{table}
                (message_id, channel_id, channel_name, channel_url, date, text)
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                conn = connect(
                    host=mysql_host,
                    password=mysql_password,
                    user=mysql_user,
                )
                with conn.cursor() as cursor:
                    cursor.executemany(insert_query, records)
                    conn.commit()
                    cursor.close()
                    conn.close()

                with open("../output/" + out_file_name, "w", encoding="utf-8") as out_file:
                    # Create dataframe from news records

                    # Save as json locally
                    json.dump(
                        all_messages, 
                        out_file, 
                        ensure_ascii=False, 
                        cls=DateTimeEncoder, 
                        indent=4
                    )
                break
        return


async def main():
    # Read given urls to channels and get messsages from them
    with open(input_path, "r") as url_file:
        urls = url_file.read().strip().split("\n")

    for url in urls:
        channel_name = url.replace("https://t.me/", "")
        channel = await client.get_entity(url)
        await dump_all_messages(
            channel,
            out_file_name=channel_name + ".json", 
            limit_msg=config.grabber.limit_msg,
            channel_name=channel_name,
            channel_url=url
        )


if __name__ == "__main__":
    logger.info("Running channel message grabber")
    with client:
        client.loop.run_until_complete(main())
