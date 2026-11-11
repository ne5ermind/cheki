import polars as pl

pl.Config.set_tbl_rows(1000)    
pl.Config.set_tbl_cols(20)      
pl.Config.set_fmt_str_lengths(50)
# %%

df = pl.scan_csv('./sorted_cheese_top.tsv', separator='\t')
# %%

print(df.schema)
# %%

df.head(100).collect()
# %%

filtered = df.filter(pl.col('title').str.contains('сыр'))
# %%

filtered.head(100).collect()
# %%

df.select(pl.col(pl.NUMERIC_DTYPES)).describe()
# %%

# наличие нулевых значений в столбце quantity - артефакты

df.head(100000).select([
    pl.col(pl.Utf8).is_null().sum().name.suffix(' null_count'),
    pl.col(pl.Utf8).str.strip_chars().eq('').sum().name.suffix('empty_str')
]).collect()
# %%

# пустых знасчений в строковых столбцах нет

df.filter(
    (pl.col('title').str.len_chars() > 500) | (pl.col('title').str.len_chars() < 2)
).limit(10).collect()
# %%

# некоторые строки имеют лишние данные, из за которых смещаются значения

df.head(10000).group_by('title').len().sort('len').limit(200).collect()
# %%

# много уникальных категорий - часть из них не имеют никакого отношения к теме датасета
