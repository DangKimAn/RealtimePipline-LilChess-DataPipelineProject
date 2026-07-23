CREATE SCHEMA IF NOT EXISTS gold;

-- 1. Bảng game_evaluations (Partition theo Ngày)

CREATE TABLE IF NOT EXISTS gold.game_evaluations (id SERIAL, game_id VARCHAR(50),
                                                                     prev_fen TEXT, move VARCHAR(20),
                                                                                         fen TEXT, color_turn VARCHAR(10),
                                                                                                              time_left INTEGER, num_of_moves INTEGER, cp_loss DOUBLE PRECISION, remark INTEGER, collected_at TIMESTAMP DEFAULT now(), -- Khóa chính & Unique phải kèm theo partition key (collected_at)
 PRIMARY KEY (id,
              collected_at), UNIQUE(game_id, num_of_moves, color_turn, collected_at)) PARTITION BY RANGE (collected_at);

-- 2. Bảng games (Partition theo Tháng)

CREATE TABLE IF NOT EXISTS gold.fact_game(id SERIAL, game_id VARCHAR(50),
                                                          winner INTEGER, status_id INTEGER, turns INTEGER, white_id VARCHAR(50),
                                                                                                                     black_id VARCHAR(50),
                                                                                                                              speed VARCHAR(50),
                                                                                                                                    perf VARCHAR(50),
                                                                                                                                         created_at TIMESTAMP, collected_at TIMESTAMP DEFAULT now(), -- Khóa chính & Unique phải kèm theo partition key (created_at)
 PRIMARY KEY (id,
              collected_at), UNIQUE(game_id, collected_at)) PARTITION BY RANGE (collected_at);

---------------------------------------------------------------------------
-- HÀM HỖ TRỢ TẠO PARTITION TỰ ĐỘNG
---------------------------------------------------------------------------
 -- Hàm tạo partition cho bảng game_evaluations (Theo ngày)


CREATE table if not EXISTS gold.dim_user(
    id SERIAL,
    user_id VARCHAR(50),
    username VARCHAR(50),
    title VARCHAR(50),  
    real_name text,
    links TEXT,
    bio TEXT,
    url VARCHAR(255),
    LOCATION text ,
    collected_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (id,collected_at)
) PARTITION BY RANGE (collected_at);

-- Hàm tạo partition cho bảng users (Theo tháng)



CREATE table if not EXISTS gold.fact_user(
    id SERIAL, 
    user_id VARCHAR(50),
    created_at TIMESTAMP,
    fide_rating INTEGER,
    play_time_seconds BIGINT, 
    all_count INTEGER, 
    rated_count INTEGER,
    draw_count INTEGER,
    loss_count INTEGER,
    win_count INTEGER,
    collected_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (id,collected_at)) 
PARTITION BY RANGE (collected_at);


create table if not EXISTS gold.fact_elo(
    id SERIAL,
    username text,
    rd INTEGER,
    num_games INTEGER,
    rating INTEGER,
    type text,
    prov boolean,
    prog INTEGER,
    collected_at TIMESTAMP DEFAULT(now()),
    PRIMARY key(id , collected_at)
) PARTITION by RANGE (collected_at);

-- drop TABLE IF EXISTS gold.users;

-- drop TABLE IF EXISTS gold.game_evaluations;
-- drop TABLE IF EXISTS gold.fact_game;

-- DROP TABLE if  EXISTS gold.dim_user;
-- drop TABLE if EXISTS gold.fact_user;

-- truncate table gold.fact_elo;
-- truncate table gold.game_evaluations;
-- truncate table gold.fact_game;
-- truncate table gold.dim_user;
-- truncate table gold.fact_user;


