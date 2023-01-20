CREATE TABLE IF NOT EXISTS gpb_news_external.telegram_news(
        rownum INT AUTO_INCREMENT PRIMARY KEY,
        message_id BIGINT,
        channel_id BIGINT,
        channel_name VARCHAR(64),
        channel_url VARCHAR(64),
        `date` VARCHAR(64),
        `text` TEXT NOT NULL,
        views BIGINT,
        forwards BIGINT,
        found_urls VARCHAR(512),
        report_dttm VARCHAR(64),
        CONSTRAINT unique_message UNIQUE(message_id, channel_id)
);