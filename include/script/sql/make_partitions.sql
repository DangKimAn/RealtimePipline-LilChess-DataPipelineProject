CREATE OR REPLACE FUNCTION gold.create_partitions(
    target_table TEXT, 
    interval_type TEXT, -- Nhận giá trị 'daily' hoặc 'monthly'
    start_date DATE, 
    end_date DATE
) 
RETURNS void AS $$
DECLARE
    curr_date DATE;
    next_date DATE;
    partition_name TEXT;
    date_format TEXT;
    step_interval INTERVAL;
BEGIN
    -- Cấu hình bước nhảy và định dạng tên dựa vào loại partition
    IF interval_type = 'monthly' THEN
        curr_date := date_trunc('month', start_date);
        step_interval := INTERVAL '1 month';
        date_format := 'YYYY_MM';
    ELSIF interval_type = 'daily' THEN
        curr_date := start_date;
        step_interval := INTERVAL '1 day';
        date_format := 'YYYY_MM_DD';
    ELSE
        RAISE EXCEPTION 'Loại partition % không hợp lệ. Vui lòng dùng ''daily'' hoặc ''monthly''', interval_type;
    END IF;

    -- Vòng lặp tự động tạo bảng
    WHILE curr_date <= end_date LOOP
        next_date := curr_date + step_interval;
        partition_name := target_table || '_' || to_char(curr_date, date_format);

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS gold.%I PARTITION OF gold.%I FOR VALUES FROM (%L) TO (%L);',
            partition_name,
            target_table,
            curr_date,
            next_date
        );
        
        curr_date := next_date;
    END LOOP;
END;
$$ LANGUAGE plpgsql;



---------------------------------------------------------------------------
-- THỰC THI TẠO PARTITION CHO TẤT CẢ CÁC BẢNG
---------------------------------------------------------------------------

-- 1. Bảng game_evaluations (Theo ngày)
SELECT gold.create_partitions(
    'game_evaluations', 
    'daily', 
    CURRENT_DATE - 2, 
    CURRENT_DATE + 7
);

-- 2. Bảng fact_game (Theo tháng)
SELECT gold.create_partitions(
    'fact_game', 
    'monthly', 
    (CURRENT_DATE - INTERVAL '1 month')::DATE, 
    (CURRENT_DATE + INTERVAL '2 months')::DATE
);

-- 3. Bảng dim_user (Theo tháng)
SELECT gold.create_partitions(
    'dim_user', 
    'monthly', 
    (CURRENT_DATE - INTERVAL '1 month')::DATE, 
    (CURRENT_DATE + INTERVAL '2 months')::DATE
);

-- 4. Bảng fact_user (Theo tháng)
SELECT gold.create_partitions(
    'fact_user', 
    'monthly', 
    (CURRENT_DATE - INTERVAL '1 month')::DATE, 
    (CURRENT_DATE + INTERVAL '2 months')::DATE
);

-- 5. Bảng fact_elo (Theo tháng)
SELECT gold.create_partitions(
    'fact_elo', 
    'monthly', 
    (CURRENT_DATE - INTERVAL '1 month')::DATE, 
    (CURRENT_DATE + INTERVAL '2 months')::DATE
);