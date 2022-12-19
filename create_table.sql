CREATE TABLE IF NOT EXISTS telegram_news(
        id BIGINT,
        channel_id BIGINT,
        channel_name VARCHAR(100),
        `date` VARCHAR(100),
        `text` TEXT
);