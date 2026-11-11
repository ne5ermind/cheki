import os
import time
import logging
from datetime import datetime
from time import sleep, time
from pathlib import Path
from dotenv import load_dotenv

import polars as pl
import clickhouse_connect
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError


load_dotenv()

pl.Config.set_tbl_rows(1000)    
pl.Config.set_tbl_cols(20)      
pl.Config.set_fmt_str_lengths(50)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    filename='ingest.log'
    # handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

CONFIG = {
    'ch_host': os.getenv('CH_HOST'),
    'ch_port': os.getenv('CH_PORT'),
    'ch_user': os.getenv('CH_USER'),
    'ch_pass': os.getenv('CH_PASS'),
    'ch_db': os.getenv('CH_DB'),
    'ch_table_cheese': os.getenv('CH_TABLE_CHEESE'),
    'ch_table_regular': os.getenv('CH_TABLE_REGULAR'),
    'poll_interval_sec': 5,
    'batch_size': 1000,
}

CHEESE_COLUMNS = [
    'user_id', 'id', 'date_actual', 'title', 'sum', 'quantity',
    'is_mm', 'check_id', 'cheese_type', 'is_canceled',
    'weight', 'fat_percentage', 'brand', 'flavor'
]

REGULAR_COLUMNS = [
    'user_id', 'id', 'date_actual', 'title', 'sum', 'quantity',
    'is_mm', 'check_id', 'category', 'is_canceled'
]

CREAMY = r'(?i)сливочный|cream\s*cheese|кремовый'
CURD = r'(?i)творожный'
MELTED = r'(?i)плавленый'
VANILLA = r'(?i)ванильный'
CREAMY_CURD_MELTED = r'(?i)сливочный|cream\s*cheese|кремовый|творожный|плавленый'

CHEESE_KEYWORDS = [
    'сыр сливочный',
    'сыр кремовый',
    'сыр твердый',
    'сыр плавленый',
    'сыр творожный',
    'сыр полутвердый',
    'cream cheese',
    'cheese ',
    'сыр '
]

CHEESE_TYPE_PATTERN = r'(?i)(' + '|'.join(CHEESE_KEYWORDS) + r')'
WEIGHT_PATTERN = r'(\d+)\s+[гg]'
FAT_PERCENTAGE_PATTERN = r'(\d+)\s*%'
RAW_TEXT_PATTERN = rf'{CHEESE_TYPE_PATTERN}\s*([^\d\n]+)'

CATEGORY_PATTERNS = {
    'молочка': r'(?i)\b(?:молоч|кефир|сметан|творог|йогурт|сливк|сгущен|морожен|пахт[аы]|обрат|айран|тан|кумыс)\b',
    'мясная продукция': r'(?i)\b(?:мяс|говяж|свинин|куриц|индейк|фарш|колбас|сосиск|ветчин|бекон|шашлык|котлет)\b',
    'рыба и морепродукты': r'(?i)\b(?:рыб|лосос|семг|форел|треск|кревет|кальмар|икр|краб|миди|тунец|скумбр|сельд)\b',
    'выпечка': r'(?i)\b(?:хлеб|батон|булк|пирог|печень|торт|кекс|булоч|круассан|лаваш|печен[ияе])\b',
    'напитки': r'(?i)\b(?:чай|кофе|вода|сок|компот|квас|лимонад|кола|энергетик|минеральн|газирован)\b',
    'соус': r'(?i)\b(?:соус|кетчуп|майонез|аджик|пест[оы]|ткемал|терияки|барбекю)\b',
    'бытовая химия': r'(?i)\b(?:порош|гель|средств|чистящ|моющ|отбелив|пятновыв|антижир|шуманит|доместос|фейри)\b',
    'личная гигиена': r'(?i)\b(?:мыл|шампун|паст|щетк|дезодор|крем|прокладк|подгузник|салфет|гигиен)\b',
    'овощи и Фрукты': r'(?i)\b(?:яблок|груш|банан|лимон|апельсин|помидор|огур|картоф|лук|чеснок|капуст|морков)\b',
    'услуга': r'(?i)\b(?:доставк|упаковк|сервис|сборк)\b',
}


def get_client():
    return clickhouse_connect.get_client(
        host=CONFIG['ch_host'],
        port=CONFIG['ch_port'],
        username=CONFIG['ch_user'],
        password=CONFIG['ch_pass'],
        database=CONFIG['ch_db'],
        secure=CONFIG['ch_port'] == 8443,
        send_receive_timeout=300,
        compress=True,
    )


def insert_data(df: pl.DataFrame, table: str, client) -> bool:
    if df.is_empty():
        return True
    
    try:
        logger.info(f'inserting nw data, columns: {df.columns}')

        arrow_table = df.to_arrow()

        columns = df.columns

        client.insert_arrow(
            table=table,
            arrow_table=arrow_table,
            #column_names=columns,
            settings={'async_insert': 1, 'wait_for_async_insert': 0}
        )
        logger.info(f'inserted {len(df)} rows in {table}')

        return True
    except (DatabaseError, OperationalError) as e:
        logger.error(f'clickhouse error: {e}')

        return False
    except Exception as e:
        logger.error(f'unexpected error: {e}')

        return False


'''
this function needs to be implemented with firstada's internal api with which they get transaction info in the first place
we dont have acces to it so this part of the code isnt functional :p
'''
def fetch_new_data():
    sleep(60)

    return pl.DataFrame()


'''
this function is only needed to fill up a new database with test data we have recieved
it showcases our approach to classifying data which is to be implemented in fetch_new_data
'''
def read_from_file(path='./top_cheese_test.tsv'):
    df = pl.scan_csv(path, separator='\t')

    result_cheese = normalize_cheese(df)
    result_other = normalize_other(df)

    return {'result_cheese': result_cheese, 'result_other': result_other}


def normalize_cheese(df) -> pl.DataFrame:
    df_cheese = (
        df
        .with_columns(pl.col('title').str.extract(RAW_TEXT_PATTERN, 2).alias('raw_text'))
        .filter(pl.col('title').str.contains(CHEESE_TYPE_PATTERN))
    )

    raw_words_list = pl.col('raw_text').str.strip_chars().str.split(' ')
    words_count = raw_words_list.list.len()
    mid_idx = pl.max_horizontal(words_count // 2 - 1, 1)

    result = (
        df_cheese.with_columns(
            pl.col('user_id').cast(pl.Utf8),
            pl.col('date_actual').str.to_datetime().cast(pl.Date),
            pl.col('sum').cast(pl.Float32),
            pl.col('quantity').cast(pl.Float32),
            pl.col('is_mm').cast(pl.Boolean),

            cheese_type=pl.when(pl.col('title').str.contains(CREAMY)).then(pl.lit('сыр сливочный'))
                    .when(pl.col('title').str.contains(VANILLA)).then(pl.lit('сыр ванильный'))
                    .when(pl.col('title').str.contains(CURD)).then(pl.lit('сыр творожный'))
                    .when(pl.col('title').str.contains(MELTED)).then(pl.lit('сыр плавленый'))
                    .otherwise(pl.lit('сыр обычный')).cast(pl.Utf8),
            
            is_canceled=pl.col('title').str.contains(r'(?i)возмещение').cast(pl.Boolean),

            weight=pl.col('title').str.extract(WEIGHT_PATTERN, 1).cast(pl.Float32, strict=False).fill_null(0.0),

            fat_percentage=pl.col('title').str.extract(FAT_PERCENTAGE_PATTERN, 1).cast(pl.UInt32, strict=False).fill_null(0),

            brand=pl.when(pl.col('title').str.contains(CREAMY_CURD_MELTED))
                    .then(raw_words_list.list.slice(0, mid_idx).list.join(' '))
                    .otherwise(pl.col('raw_text')).cast(pl.Utf8),
                        
            flavor=pl.when(pl.col('title').str.contains(CREAMY_CURD_MELTED) & (words_count > 1))
                    .then(raw_words_list.list.slice(mid_idx).list.join(' '))
                    .otherwise(None).cast(pl.Utf8),
        )
    ).drop('raw_text')

    return result.select(CHEESE_COLUMNS).collect()


def normalize_other(df) -> pl.DataFrame:
    df_other_products = df.filter(~pl.col('title').str.contains(CHEESE_TYPE_PATTERN))

    category_expr = pl.lit('другое')
    for cat_name, pattern in CATEGORY_PATTERNS.items():
        category_expr = pl.when(pl.col('title').str.contains(pattern)).then(pl.lit(cat_name)).otherwise(category_expr)

    result = (
        df_other_products.with_columns(
            pl.col('user_id').cast(pl.Utf8),
            pl.col('date_actual').str.to_datetime().cast(pl.Date),
            pl.col('sum').cast(pl.Float32),
            pl.col('quantity').cast(pl.Float32),
            pl.col('is_mm').cast(pl.Boolean),

            category=category_expr,
            is_canceled=pl.col('title').str.contains(r'(?i)возмещение').cast(pl.Boolean),
        )
    )

    return result.select(REGULAR_COLUMNS).collect()


def push_buffer(buffer, client, table):
    total_buffered = sum(len(i) for i in buffer)

    if buffer and (total_buffered >= CONFIG['batch_size'] or (buffer and time() % 60 < CONFIG['poll_interval_sec'])):
        combined_df = pl.concat(buffer, how='vertical')

        return insert_data(combined_df, table, client)
    return False               


def main():
    logger.info('ingest script started')

    buffer_cheese, buffer_regular = [], []
    client = None

    try:
        logger.info(f'connecting to {CONFIG["ch_host"]}:{CONFIG["ch_port"]}')

        client = get_client()
    except Exception as e:
        logger.error(f'unexpected error during initial load: {e}')
        return

    logger.info('connection established')

    try:
        if client.query(f'SELECT count() from {CONFIG['ch_table_cheese']}').result_set[0][0] == 0 and client.query(f'SELECT count() from {CONFIG['ch_table_regular']}').result_set[0][0] == 0:
            new_df = read_from_file()

            buffer_cheese.append(new_df['result_cheese'])
            buffer_regular.append(new_df['result_other'])

            logger.info('writing data from file')
        else:
            logger.info('database already filled')
    except Exception as e:
        logger.error(f'unexpected error while reading data from file: {e}')
        return

    try:
        while True:
            new_df = fetch_new_data()

            if not(new_df.is_empty()):
                logger.info(f'recieved {len(new_df)} new rows')
                
                buffer_cheese.append(normalize_cheese(new_df))
                buffer_regular.append(normalize_other(new_df))
            else:
                logger.info('no new data recieved')

            if push_buffer(buffer_cheese, client, CONFIG['ch_table_cheese']):
                logger.info(f'succesfully pushed {len(buffer_cheese)} rows')

                buffer_cheese.clear()

            if push_buffer(buffer_regular, client, CONFIG['ch_table_regular']):
                logger.info(f'succesfully pushed {len(buffer_regular)} rows')

                buffer_regular.clear()

            sleep(CONFIG['poll_interval_sec'])
    except Exception as e:
        logger.error(f'unexpected error while streaming data: {e}')
        return
    finally:
        if client:
            client.close()
            logger.info('connection closed')


if __name__ == '__main__':
    main()
