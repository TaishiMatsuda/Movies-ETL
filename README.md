# Movies-ETL
Module 08 of UofT Data Analytics Boot Camp

## Assumptions in ETL Process

#### 1. Input File Location/Name to be Never Changed
Process to import json or csv file assumes that the file path (name or directories) to always remain the same. If the path is changed, `FileNotFoundError` will be returned.

In this case, we would want the ETL to be interrupted and hence we would not need to use `try-except` blocks.


#### 2. Input File always contain certain columns

We assume that each files will always contain the columns of interest and those columns name to be unchanged.

For example, in the process to clean the Kaggle Movie Meta Data, we are assuming that columns 'adult', 'video', 'budget', 'id', 'popularity','release_date' to exist.

```python
# Cleaning Movie Meta (Kaggle) Data
# Removing Movies rated Adult
kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
# Converting DataTypes
kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
```

If the column does not exist or was renamed in new file, `KeyError` will be returned.

In case of missing the important columns such as 'id','budget' and 'release_date', we would want the ETL to be interrupted and hence we would not need to use `try-except` blocks.

However, for columns like 'adult' or 'video' that are not important for the analysis, we can use the following `try-except` blocks to continue the ETL process.

```python
try:
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
except KeyError:
    print('column adult does not exist in Kaggle Data')
```


#### 3. No New (Important) Column(s) to be Added
The code below is used to reorder the cleaned DataFrame (movies_df).

```python
# Reordering Columns
movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]] 
```
