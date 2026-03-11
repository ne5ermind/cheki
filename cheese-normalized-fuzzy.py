import polars as pl
from rapidfuzz import process, fuzz

pl.Config.set_tbl_rows(1000)    
pl.Config.set_tbl_cols(20)      
pl.Config.set_fmt_str_lengths(50)
# %%

keywords = [
    'сыр',
    'сыр сливочный',
    'сыр кремовый',
    'сыр твердый',
    'сыр плавленый',
    'сыр творожный',
    'сыр полутвердый'
]

def fuzzy_find(title, keywords, threshold=80):
    res = process.extractOne(title, keywords, scorer=fuzz.WRatio)

    if res and res[1] >= threshold:
        return res[0]
    return None

df = pl.scan_csv('./top_cheese_test.tsv', separator='\t')

normalized_df = df.with_columns(pl.col('title').map_elements(
    lambda x: fuzzy_find(x, keywords),
    return_dtype=pl.String
).alias('cheese_type')).filter(pl.col('cheese_type').is_not_null()).head(10000)

normalized_df.collect()
# %%

