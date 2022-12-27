from datetime import datetime, timedelta
from typing import NoReturn
import json
import argparse

import pandas as pd
from omegaconf import OmegaConf
from telethon.errors import SessionPasswordNeededError
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest

from utils import getLogger
from connect_sql import save_table_mysql


# Add logging
logger = getLogger(__name__, file_name='../logs/tg_grabber.log')

logger.info('Read config files')
config = OmegaConf.load('../config.yaml')
database_config = OmegaConf.load('../database.yaml')

# TG constants
api_id = config.telegram.api_id
api_hash = config.telegram.api_hash
username = config.telegram.username
phone = config.telegram.phone
bot_token = config.telegram.bot_token
# Grabber parameters
limit_msg = config.grabber.limit_msg

logger.info('Create a telegram client')
# TG API client
client = TelegramClient('../logs/' + username, api_id, api_hash)
client.start(phone=phone)
# client = TelegramClient('../logs/' + username, api_id, api_hash)
# client.start(bot_token=bot_token)


class DateTimeEncoder(json.JSONEncoder):
    """
    To serialize dates to JSON
    """
    def default(self, o) -> json.JSONEncoder.default:
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, bytes):
            return list(o)
        return json.JSONEncoder.default(self, o)


async def dump_all_messages(
        channel: str, 
        channel_name: str,
        out_file_name: str,
        limit_msg: int
        ) -> NoReturn:
    """
    Write messages from channel to a json file
    """
    # target_date = (datetime.today() + timedelta(hours=3)).date()
    target_date = datetime.today().date()
    date_before = target_date - timedelta(days=2)
    date_before = datetime(
        date_before.year, 
        date_before.month, 
        date_before.day
    )
    all_messages = []
    while True:
        history = await client(GetHistoryRequest(
            peer=channel,
            offset_id=0,
            offset_date=target_date,
            add_offset=0,
            limit=limit_msg,
            max_id=0,
            min_id=0,
            hash=0,
        ))
        if not history.messages:
            break

        messages = history.messages
        for message in messages:
            message = message.to_dict()
            message_prepared = {}
            # Get messages until unrequired date met
            try:
                date_msk = message['date'].replace(tzinfo=None) + timedelta(hours=3)
                if date_msk > date_before:
                    message_prepared['id'] = message['id'] 
                    message_prepared['channel_id'] = message['peer_id']['channel_id']
                    message_prepared['channel_name'] = channel_name
                    message_prepared['date'] = date_msk
                    message_prepared['text'] = message['message']
                    all_messages.append(message_prepared)
                else:
                    total_messages = len(all_messages)
                    logger.info(
                        "Number of grabbed news from channel '{}': {}". \
                            format(channel_name, total_messages)
                    )
                    logger.info(
                        "Saving data from channel '{}' to database". \
                            format(channel_name)
                    )
                    with open('../output/' + out_file_name, 'w', encoding='utf-8') as out_file:
                        # Create dataframe from news records
                        news_df = pd.DataFrame.from_records(all_messages)
                        # Save dataframe to MySQL table
                        save_table_mysql(df=news_df, conf=database_config)
                        # Save as json locally
                        json.dump(all_messages, out_file, ensure_ascii=False, cls=DateTimeEncoder)
                    return
            # There can be a post with no text (only pictures)
            except KeyError:
                continue


async def main():
    # Read given urls to channels and get messsages from them
    with open('../input/channel_urls.txt', 'r') as url_file:
        urls = url_file.read().strip().split('\n')

    for url in urls:
        channel_name = url.replace('https://t.me/', '')
        channel = await client.get_entity(url)
        await dump_all_messages(
            channel, 
            channel_name=channel_name, 
            out_file_name=channel_name + '.json', 
            limit_msg=limit_msg
        )


logger.info('Running channel message grabber')
with client:
    client.loop.run_until_complete(main())
