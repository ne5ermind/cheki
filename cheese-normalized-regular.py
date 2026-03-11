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
    'сыр'
]

pattern = r'(?i)(' + '|'.join(keywords) + r')'

df = pl.scan_csv('./top_cheese_test.tsv', separator='\t')

normalized_df = (df.filter(pl.col('title').str.contains(pattern))
                 .with_columns(
                    pl.when(pl.col('title').str.contains(r'(?i)сливочный|cream cheese|кремовый'))
                    .then(pl.lit('сыр сливочный'))
                    
                    .when(pl.col('title').str.contains(r'(?i)ванильный'))
                    .then(pl.lit('сыр ванильный'))
                    
                    .when(pl.col('title').str.contains(r'(?i)творожный'))
                    .then(pl.lit('сыр творожный'))
                    
                    .otherwise(pl.lit('сыр обычный'))
                    
                    .alias('cheese_type')
                 )
).head(10000)

normalized_df.collect()
# %%

