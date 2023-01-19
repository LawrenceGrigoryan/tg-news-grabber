CREATE TABLE IF NOT EXISTS gpb_news_external.telegram_news(
        rownum INT AUTO_INCREMENT PRIMARY KEY,
        message_id BIGINT,
        channel_id BIGINT,
        channel_name VARCHAR(100),
        channel_url VARCHAR(100),
        `date` VARCHAR(100),
        `text` TEXT,
        CONSTRAINT unique_message UNIQUE(message_id, channel_id)
);