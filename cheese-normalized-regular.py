import polars as pl

pl.Config.set_tbl_rows(1000)    
pl.Config.set_tbl_cols(20)      
pl.Config.set_fmt_str_lengths(50)
# %%

keywords = [
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

cheese_type_pattern = r'(?i)(' + '|'.join(keywords) + r')'
weight_pattern = r'(\d+)\s+г'
fat_percentage_pattern = r'(\d+)%'
raw_text_pattern = rf'{cheese_type_pattern}\s*([^\d\n]+)'

df = pl.scan_csv('./top_cheese_test.tsv', separator='\t').with_columns(pl.col('title').str.extract(raw_text_pattern, 2).alias('raw_text'))

raw_words_list = pl.col('raw_text').str.strip_chars().str.split(' ')
words_count = raw_words_list.list.len()
mid_idx = pl.max_horizontal(words_count // 2, 1)

normalized_df = (df.filter(pl.col('title').str.contains(cheese_type_pattern))
                 .with_columns(
                    pl.when(pl.col('title').str.contains(r'(?i)сливочный|cream cheese|кремовый'))
                    .then(pl.lit('сыр сливочный'))
                    .when(pl.col('title').str.contains(r'(?i)ванильный'))
                    .then(pl.lit('сыр ванильный'))
                    .when(pl.col('title').str.contains(r'(?i)творожный'))
                    .then(pl.lit('сыр творожный'))
                    .otherwise(pl.lit('сыр обычный'))
                    .alias('cheese_type'),

                    pl.col('title').str.contains(r'(?i)возмещение').alias('is_canceled'),

                    pl.col('title').str.extract(weight_pattern, 1).alias('weight'),

                    pl.col('title').str.extract(fat_percentage_pattern, 1).alias('procenti'),

                    pl.when(pl.col('title').str.contains(r'(?i)сливочный|cream cheese|кремовый|творожный'))
                    .then(raw_words_list.list.slice(0, mid_idx).list.join(' '))
                    .otherwise(pl.col('raw_text'))
                    .alias('manufacturer'),
                    
                    pl.when(pl.col('title').str.contains(r'(?i)сливочный|cream cheese|кремовый|творожный'))
                    .then(raw_words_list.list.slice(mid_idx).list.join(' '))
                    .otherwise(None)
                    .alias('taste')
                 )
).drop('raw_text')

normalized_df.collect()
# %%
