import polars as pl

pl.Config.set_tbl_rows(1000)    
pl.Config.set_tbl_cols(20)      
pl.Config.set_fmt_str_lengths(50)
# %%

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

df = pl.scan_csv('./top_cheese_test.tsv', separator='\t')
# %%

df_cheese = (
    df
    .with_columns(pl.col('title').str.extract(RAW_TEXT_PATTERN, 2).alias('raw_text'))
    .filter(pl.col('title').str.contains(CHEESE_TYPE_PATTERN))
)

raw_words_list = pl.col('raw_text').str.strip_chars().str.split(' ')
words_count = raw_words_list.list.len()
mid_idx = pl.max_horizontal(words_count // 2 - 1, 1)

normalized_df_cheese = (
    df_cheese.with_columns(
        cheese_type=pl.when(pl.col('title').str.contains(CREAMY)).then(pl.lit('сыр сливочный'))
                .when(pl.col('title').str.contains(VANILLA)).then(pl.lit('сыр ванильный'))
                .when(pl.col('title').str.contains(CURD)).then(pl.lit('сыр творожный'))
                .when(pl.col('title').str.contains(MELTED)).then(pl.lit('сыр плавленый'))
                .otherwise(pl.lit('сыр обычный')),
        
        is_canceled=pl.col('title').str.contains(r'(?i)возмещение'),
        weight=pl.col('title').str.extract(WEIGHT_PATTERN, 1),
        fat_percentage=pl.col('title').str.extract(FAT_PERCENTAGE_PATTERN, 1),

        brand=pl.when(pl.col('title').str.contains(CREAMY_CURD_MELTED))
                .then(raw_words_list.list.slice(0, mid_idx).list.join(' '))
                .otherwise(pl.col('raw_text')),
                    
        flavor=pl.when(pl.col('title').str.contains(CREAMY_CURD_MELTED) & (words_count > 1))
                .then(raw_words_list.list.slice(mid_idx).list.join(' '))
                .otherwise(None),
    )
)

normalized_df_cheese.collect().head(10)
# %%

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

WEIGHT_PAT = r'(\d+)\s*[гgG]'
FAT_PAT    = r'(\d+)\s*%'

df_other_products = df.filter(~pl.col('title').str.contains(CHEESE_TYPE_PATTERN))

category_expr = pl.lit('другое')
for cat_name, pattern in CATEGORY_PATTERNS.items():
    category_expr = pl.when(pl.col('title').str.contains(pattern)).then(pl.lit(cat_name)).otherwise(category_expr)

result = (
    df_other_products.with_columns(
        category=category_expr,
        is_canceled=pl.col('title').str.contains(r'(?i)возмещение'),
    )
)

result.filter(~pl.col('category').str.contains('другое')).collect().head(10)
# %%

