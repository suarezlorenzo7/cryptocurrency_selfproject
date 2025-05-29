import psycopg2

def fetch_and_load(host, port, database, user, password,
    datetime,
    btcusdt_rate,
    ethusdt_rate,
    solusdt_rate,
    btcbrl_rate,
    ethbrl_rate,
    solbrl_rate,
    btcusdt_total,
    ethusdt_total,
    solusdt_total,
    btcbrl_total,
    ethbrl_total,
    solbrl_total,
    brl_total,
    usdt_total):

    """Expects GCP database conn info and data, then fetches and loads it into the desired GCP database."""
        
    conn = psycopg2.connect (
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    cursor = conn.cursor()

    sql = """
    INSERT INTO public.demo_crypto_wallet (
        datetime,
        btcusdt_rate,
        ethusdt_rate,
        solusdt_rate,
        btcbrl_rate,
        ethbrl_rate,
        solbrl_rate,
        btcusdt_total,
        ethusdt_total,
        solusdt_total,
        btcbrl_total,
        ethbrl_total,
        solbrl_total,
        brl_total,
        usdt_total
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    values = [
        datetime,
        btcusdt_rate,
        ethusdt_rate,
        solusdt_rate,
        btcbrl_rate,
        ethbrl_rate,
        solbrl_rate,
        btcusdt_total,
        ethusdt_total,
        solusdt_total,
        btcbrl_total,
        ethbrl_total,
        solbrl_total,
        brl_total,
        usdt_total
    ]

    cursor.execute(sql, values)

    conn.commit()

    cursor.close()
    conn.close()