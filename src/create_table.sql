CREATE TABLE IF NOT EXISTS telegram_news(
        -- id INT AUTO_INCREMENT PRIMARY KEY
        id BIGINT,
        channel_id BIGINT,
        channel_name VARCHAR(100),
        `date` VARCHAR(100),
        `text` TEXT
);