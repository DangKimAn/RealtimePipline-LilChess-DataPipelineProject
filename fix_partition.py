import os
import psycopg2
from dotenv import load_dotenv

load_dotenv('.env')

conn = psycopg2.connect(
    host=os.getenv('DB_HOST'),
    port=os.getenv('DB_PORT'),
    dbname=os.getenv('DB_NAME'),
    user=os.getenv('DB_USER'),
    password=os.getenv('DB_PASSWORD')
)
conn.autocommit = True
cursor = conn.cursor()

# Create function for users partitions
cursor.execute("""
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
""")

# Create partitions for the current and next month
cursor.execute("""
SELECT gold.create_users_partitions((CURRENT_DATE - INTERVAL '1 month')::DATE, (CURRENT_DATE + INTERVAL '2 months')::DATE);
""")

print("Successfully created partitions for gold.users")
