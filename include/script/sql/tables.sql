CREATE SCHEMA IF NOT EXISTS gold;

-- 1. Bảng game_evaluations (Partition theo Ngày)

CREATE TABLE IF NOT EXISTS gold.game_evaluations (id SERIAL, game_id VARCHAR(50),
                                                                     prev_fen TEXT, move VARCHAR(20),
                                                                                         fen TEXT, color_turn VARCHAR(10),
                                                                                                              time_left INTEGER, num_of_moves INTEGER, cp_loss DOUBLE PRECISION, remark INTEGER, collected_at TIMESTAMP DEFAULT now(), -- Khóa chính & Unique phải kèm theo partition key (collected_at)
 PRIMARY KEY (id,
              collected_at), UNIQUE(game_id, num_of_moves, color_turn, collected_at)) PARTITION BY RANGE (collected_at);

-- 2. Bảng games (Partition theo Tháng)

CREATE TABLE IF NOT EXISTS gold.games (id SERIAL, game_id VARCHAR(50),
                                                          winner INTEGER, status_id INTEGER, turns INTEGER, white_id VARCHAR(50),
                                                                                                                     black_id VARCHAR(50),
                                                                                                                              speed VARCHAR(50),
                                                                                                                                    perf VARCHAR(50),
                                                                                                                                         created_at TIMESTAMP, collected_at TIMESTAMP DEFAULT now(), -- Khóa chính & Unique phải kèm theo partition key (created_at)
 PRIMARY KEY (id,
              created_at), UNIQUE(game_id, created_at)) PARTITION BY RANGE (created_at);

---------------------------------------------------------------------------
-- HÀM HỖ TRỢ TẠO PARTITION TỰ ĐỘNG
---------------------------------------------------------------------------
 -- Hàm tạo partition cho bảng game_evaluations (Theo ngày)

CREATE OR REPLACE FUNCTION gold.create_game_evaluations_partitions(start_date DATE, end_date DATE) RETURNS void AS $$
DECLARE
    curr_date DATE := start_date;
    next_date DATE;
    partition_name TEXT;
BEGIN
    WHILE curr_date <= end_date LOOP
        next_date := curr_date + INTERVAL '1 day';
        partition_name := 'game_evaluations_' || to_char(curr_date, 'YYYY_MM_DD');

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS gold.%I PARTITION OF gold.game_evaluations FOR VALUES FROM (%L) TO (%L);',
            partition_name,
            curr_date,
            next_date
        );
        curr_date := next_date;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Hàm tạo partition cho bảng games (Theo tháng)

CREATE OR REPLACE FUNCTION gold.create_games_partitions(start_date DATE, end_date DATE) RETURNS void AS $$
DECLARE
    curr_date DATE := date_trunc('month', start_date);
    next_date DATE;
    partition_name TEXT;
BEGIN
    WHILE curr_date <= end_date LOOP
        next_date := curr_date + INTERVAL '1 month';
        partition_name := 'games_' || to_char(curr_date, 'YYYY_MM');

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS gold.%I PARTITION OF gold.games FOR VALUES FROM (%L) TO (%L);',
            partition_name,
            curr_date,
            next_date
        );
        curr_date := next_date;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- TẠO SẴN PARTITION ĐỂ TRÁNH LỖI KHI SPARK INSERT VÀO
-- (Chạy 1 lần ban đầu hoặc đưa vào cronjob chạy hàng tháng/tuần)

SELECT gold.create_game_evaluations_partitions(CURRENT_DATE - 2, CURRENT_DATE + 7);


SELECT gold.create_games_partitions((CURRENT_DATE - INTERVAL '1 month')::DATE, (CURRENT_DATE + INTERVAL '2 months')::DATE);


CREATE table if not EXISTS gold.users(id SERIAL, user_id VARCHAR(50),
                                                         username VARCHAR(50),
                                                                  title VARCHAR(50),
                                                                        created_at TIMESTAMP, location VARCHAR(50),
                                                                                                       real_name VARCHAR(50),
                                                                                                                 fide_rating VARCHAR(50),
                                                                                                                             links TEXT, bio TEXT, play_time INTEGER, url VARCHAR(50),
                                                                                                                                                                          all_count INTEGER, rated_count INTEGER, draw_count INTEGER, loss_count INTEGER, win_count INTEGER, collected_at TIMESTAMP DEFAULT now(),
                                                                                                                                                                                                                                                                                                            PRIMARY KEY (id,
                                                                                                                                                                                                                                                                                                                         collected_at)) PARTITION BY RANGE (collected_at);

-- Hàm tạo partition cho bảng users (Theo tháng)

CREATE OR REPLACE FUNCTION gold.create_users_partitions(start_date DATE, end_date DATE) RETURNS void AS $$
DECLARE
    curr_date DATE := date_trunc('month', start_date);
    next_date DATE;
    partition_name TEXT;
BEGIN
    WHILE curr_date <= end_date LOOP
        next_date := curr_date + INTERVAL '1 month';
        partition_name := 'users_' || to_char(curr_date, 'YYYY_MM');

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS gold.%I PARTITION OF gold.users FOR VALUES FROM (%L) TO (%L);',
            partition_name,
            curr_date,
            next_date
        );
        curr_date := next_date;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


SELECT gold.create_users_partitions((CURRENT_DATE - INTERVAL '1 month')::DATE, (CURRENT_DATE + INTERVAL '2 months')::DATE);