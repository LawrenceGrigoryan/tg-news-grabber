# Telegram news grabber

Allows to grab any post from any public channel and collect it in MySQL database

sudo chown <USERNAME> -R path/to/folder - let vs code write and delete files
sudo bash - get root access

aws s3 cp output/bbcrussian.json s3://tg_news/bbcrussian/test.json --endpoint-url https://hb.bizmrg.com

mysql --host=146.185.242.115 --user=user --password --database=gpb_news_external  # connect to mysql via command line
source create_table.sql # run .sql scipt in current connection to database