"""
Telegram channel message grabber
"""
import json
import os
from datetime import datetime, timedelta
from typing import NoReturn, Union

import pandas as pd
import boto3
from omegaconf import OmegaConf
from dotenv import load_dotenv
from telethon import hints
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from mysql.connector import connect

import constants
from utils import getLogger, DateTimeEncoder

# Load environment variables from .env
load_dotenv()

# Reading constants
input_path = constants.INPUT_PATH
output_path = constants.OUTPUT_PATH
logs_path = constants.LOGS_PATH
config_path = constants.CONFIG_PATH

# Add logging
logger = getLogger(
    name=__name__, 
    file_name=logs_path + "tg_grabber.log",
    format="%(asctime)s: %(levelname)s: %(name)s: %(message)s",
    date_format="%Y-%m-%d %H:%M:%S"
)

logger.info("Reading configuration")
config = OmegaConf.load(config_path)
db_database = config.db.database
db_table = config.db.table
s3_bucket = config.s3.bucket
s3_folder = config.s3.folder

logger.info("Reading connection params from environment variables")
tg_api_id = os.getenv("TG_API_ID")
tg_api_hash = os.getenv("TG_API_HASH")
tg_phone = os.getenv("TG_PHONE")
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
s3_access_key_id = os.getenv("S3_ACCESS_KEY_ID")
s3_secret_key = os.getenv("S3_SECRET_KEY")

logger.info("Creating a telegram client session")
client = TelegramClient(logs_path + "tg_grabber", tg_api_id, tg_api_hash)
client.start(phone=tg_phone)

logger.info("Creating an S3 session")
session = boto3.session.Session()
s3_client = session.client(
    service_name="s3",
    endpoint_url="https://ib.bizmrg.com",
    aws_access_key_id=s3_access_key_id,
    aws_secret_access_key=s3_secret_key
)


async def dump_all_messages(
        channel: hints.Entity,
        limit_msg: int,
        channel_url: Union[None, str] = None
        ) -> NoReturn:
    """
    The function grabs all news from the given input channel
    from yesterday.
    The function is supposed to run on everyday schedule in a Docker container
    using crontab.

    Args:
        channel (hints.Entity): Telethon channel object
        limit_msg (int): Maximum number of messages to receive
        channel_url (Union[None, str], optional): Channel url. Defaults to None

    Returns:
        NoReturn
    """
    # Get cannel name from url
    channel_name = channel_url.replace("https://t.me/", "")

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
                message_prepared["views"] = message["views"]
                message_prepared["forwards"] = message["forwards"]
                # Looking for urls in a message text
                found_urls = []
                try:
                    entities = message["entities"]
                    for entity in entities:
                        if entity["_"] == "MessageEntityTextUrl":
                            found_url = entity["url"]
                            if found_url not in found_urls:
                                found_urls.append(found_url)
                except KeyError:
                    pass
                found_urls_str = ", ".join(found_urls)
                message_prepared["found_urls"] = found_urls_str
                report_dttm = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
                message_prepared["report_dttm"] = report_dttm
                all_messages.append(message_prepared)
            elif message_date_msk < date_start:
                total_messages = len(all_messages)
                logger.info(
                    "Number of news grabbed from channel '{}': {}". \
                        format(channel_url, total_messages)
                )

                logger.info(
                    "Saving data from channel '{}' to database". \
                        format(channel_name)
                )
                # Connect to database and save messages
                records = [tuple(message.values()) for message in all_messages \
                           if message["text"] != "" and not pd.isna(message["text"])]
                insert_query = \
                f"""
                INSERT IGNORE INTO {db_database}.{db_table}
                (message_id, channel_id, channel_name, 
                channel_url, date, text, views, forwards, found_urls, report_dttm)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                try:
                    conn = connect(
                        host=db_host,
                        password=db_password,
                        user=db_user,
                    )
                    with conn.cursor() as cursor:
                        cursor.executemany(insert_query, records)
                        conn.commit()
                        cursor.close()
                        conn.close()
                except Exception as exc:
                    logger.error(exc)

                logger.info("Saving tmp files locally")
                # Create a unique file name
                date_start_str = date_start.strftime("%Y%m%d")
                out_file_name = channel_name + "_" + date_start_str + ".json"
                out_file_path = os.path.join(output_path, out_file_name)
                # Save as temporary json locally
                with open(out_file_path, "w", encoding="utf-8") as out_file:
                    json.dump(
                        all_messages, 
                        out_file, 
                        ensure_ascii=False, 
                        cls=DateTimeEncoder, 
                        indent=4
                    )

                logger.info(f"Uploading file {out_file_name} to S3 bucket")
                # Save to S3 bucket
                s3_file_path = os.path.join(s3_folder, out_file_name)
                s3_client.upload_file(
                    out_file_path,
                    s3_bucket,
                    s3_file_path
                )

                break
        return


async def main():
    # Read given urls to channels and get messsages from them
    with open(input_path, "r") as url_file:
        urls = url_file.read().strip().split("\n")

    for url in urls:
        channel = await client.get_entity(url)
        await dump_all_messages(
            channel,
            channel_url=url,
            limit_msg=config.grabber.limit_msg
        )


if __name__ == "__main__":
    logger.info("Running channel message grabber")
    with client:
        client.loop.run_until_complete(main())
