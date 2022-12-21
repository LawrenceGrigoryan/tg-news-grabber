FROM python:3.10-slim-buster
MAINTAINER glavrentiy123@gmail.com
RUN apt-get update && apt-get -y install cron vim
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt
COPY crontab /etc/cron.d/crontab
COPY . .
RUN rm crontab
RUN chmod 0644 /etc/cron.d/crontab
# RUN chmod 0644 /usr/bin
# RUN chmod 0644 /app
RUN /usr/bin/crontab /etc/cron.d/crontab
CMD ["cron", "-f"]
# CMD ["sh", "run_script.sh"]
