import configparser
from datetime import datetime
import json 
import logging
# import argparse
# Telethon API
from telethon.sync import TelegramClient
# Class to work with channel
from telethon.tl.functions.messages import GetHistoryRequest

# Add logging
logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p',
                    filename='../logs/tg_parse.log',
                    filemode='w',
                    level=logging.DEBUG)

logging.info('Read config file')
# Reading config 
config = configparser.ConfigParser()
config.read('../config.ini')
# TG constants
api_id = config['Telegram']['api_id']
api_hash = config['Telegram']['api_hash']
username = config['Telegram']['username']
phone = config['Telegram']['phone']
bot_token = config['Telegram']['bot_token']
# Grabber parameters
offset_msg = int(config['Grabber']['offset_msg'])
limit_msg = int(config['Grabber']['limit_msg'])
total_count_limit = int(config['Grabber']['total_count_limit'])

logging.info('Create a telegram client')
# TG API client
client = TelegramClient('../logs/' + username, api_id, api_hash)
client.start(phone=phone)
# client.log_out()


class DateTimeEncoder(json.JSONEncoder):
    """
    To serialize dates to JSON
    """
    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, bytes):
            return list(o)
        return json.JSONEncoder.default(self, o)

        
async def dump_all_messages(channel, channel_name, out_file_name, offset_msg=0, limit_msg=100, total_count_limit=100):
    """
    Write messages from channel to a json file
    """
    all_messages = []
    total_messages = 0
    while True:
        history = await client(GetHistoryRequest(
            peer=channel,
            offset_id=offset_msg,
            offset_date=None,
            add_offset=0,
            limit=limit_msg,
            max_id=0,
            min_id=0,
            hash=0
        ))
        # print(history.stringify())
        if not history.messages:
            break 
        messages = history.messages
        for message in messages:
            message = message.to_dict()
            message_prepared = {}
            # Try to get the text of a post
            try:
                message_prepared['id'] = message['id'] 
                message_prepared['channel_id'] = message['peer_id']['channel_id']
                message_prepared['channel_name'] = channel_name
                message_prepared['date'] = message['date']
                message_prepared['text'] = message['message']
                all_messages.append(message_prepared)
            # There can be a post with no text (only pictures)
            except KeyError:
                continue
        offset_msg = messages[len(messages) - 1].id
        total_messages = len(all_messages)
        if total_count_limit != 0 and total_messages >= total_count_limit:
            break

    with open('../output/' + out_file_name, 'w', encoding='utf-8') as out_file:
        json.dump(all_messages, out_file, ensure_ascii=False, cls=DateTimeEncoder)


async def main():
    # Read given urls to channels and get messsages from them
    with open('../input/channel_urls.txt', 'r') as url_file:
        urls = url_file.read().strip().split('\n')

    for url in urls:
        channel_name = url.replace('https://t.me/', '')
        channel = await client.get_entity(url)
        await dump_all_messages(channel, channel_name=channel_name, out_file_name=channel_name + '.json', 
                                offset_msg=offset_msg, limit_msg=limit_msg, total_count_limit=total_count_limit)


logging.info('Run channel message grabber function')
with client:
    client.loop.run_until_complete(main())
