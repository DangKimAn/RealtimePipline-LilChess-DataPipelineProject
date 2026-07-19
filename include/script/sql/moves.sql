        CREATE TABLE IF NOT EXISTS game_evaluations (
            game_id VARCHAR(50),
            prev_fen TEXT,
            move VARCHAR(20),
            fen TEXT,
            color_turn VARCHAR(10),
            time_left INTEGER,
            num_of_moves INTEGER,
            cp_loss DOUBLE PRECISION,
            remark INTEGER,
            PRIMARY KEY (game_id, num_of_moves, color_turn)
        );
