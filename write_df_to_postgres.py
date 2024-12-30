import psycopg
from psycopg import sql
from io import StringIO


# write dataframe to PostgreSQL
def copy_chunk(
        conn,
        df_chunk: pd.DataFrame,
        table_name: str,
) -> None:
    """Upload a single chunk to the database using the COPY command."""
    with conn.cursor() as cur:
        # Create a buffer
        buffer = StringIO()
        df_chunk.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        copy_sql = sql.SQL('COPY {} FROM STDIN WITH (FORMAT CSV)').format(sql.Identifier(table_name))
        # Load data into the table using copy
        with buffer as f:
            with cur.copy(copy_sql) as copy:
                while data := f.read(10):
                    copy.write(data)

    conn.commit()

# Usage "postgresql://username:password@hostname:port/dbname"
conn_string = "postgresql://username:password@hostname:5432/dbname"
df_chunk = dataframe_name

# Establish connection
with psycopg.connect(conn_string) as conn:
    copy_chunk(conn, df_chunk, 'trade')
        
    # Close the connection
    conn.close()
