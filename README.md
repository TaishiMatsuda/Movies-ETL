# Movies-ETL
Module 08 of UofT Data Analytics Boot Camp

## Assumptions in ETL Process

#### 1. Input File Location/Name to be Never Changed
Process to import json or csv file assumes that the file path (name or directories) to always remain the same. If the path is changed, `FileNotFoundError` will be returned.

In this case, we would want the ETL to be interrupted and hence we would not need to use `try-except` blocks.


#### 2. Kaggle Movie Meta Data always contain certain columns

We assume that the Kaggle Movie Meta Data will always contain the columns of interest and those columns name to be unchanged.

For example, in the process to clean-up data, we are assuming that columns 'adult', 'video', 'budget', 'id', 'popularity','release_date' to exist.

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

In case of missing the important columns such as 'id' and 'budget', we would want the ETL to be interrupted and hence we would not need to use `try-except` blocks.

However, the column 'adult' is not important for the analysis. Missing it should not interrupt the process. In this case, we can use the following `try-except` blocks to continue the ETL process.

```python
try:
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
except KeyError:
    print('column adult does not exist in Kaggle Data')
```


#### 3. Input File is NOT Corrupted / Damaged
Obviously the ETL process assumes all input files to be not corrupted or damaged. For Kaggle and Wiki Data, there are multiple steps before uploading to SQL that would throw an error such as points 1 & 2 shown above. In this case, we would want the ETL to be interrupted and hence we would not need to use `try-except` blocks.

However for ratings, there are no data clean-up process and file is directly uploaded to PostgreSQL. Damage to the file, such as string miss-entered to column 'rating', could significantly impact the analysis that follows ETL process.
(We could trust Kaggle to not cause such mistakes or we could add a code to drop the rows if data type is not floating-point number)


#### 4. No New (Important) Column(s) to be Added
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

Although this process currently includes all necessary columns, no new column will be captured unless new column name is added to the code above. We have to regularly inspect the dataset to make sure we capture new important dataset added to Kaggle or Wikipedia.


#### 5. SQL Connection to be Never Changed
Code below is used to create connection to PostgreSQL. This code is not dynamic to database name or passwords. Make sure to share rules among project members to not change them.

```python
from sqlalchemy import create_engine
from config import db_password
# Creating Connection to Postgres DB
db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/Movie_Data"
engine = create_engine(db_string)
```

#### 6. Columns to Exist in movies_df
There are several commands to drop the columns from movies_df such as shown below.

```python
# Dropping Wikipedia Data
movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)

# Removing column with non-valuable data
movies_df.drop('video', axis=1, inplace=True)
```

These lines could potentially cause the error such as `KeyError` and we would not want these non-important columns to interrupt the ETL process.

For those lines, we can use `try-except` blocks like below to except any kind of errors as this would not impact the final data that will be uploaded to PostgreSQL.
```python
try:
    write df.drop here
except:
```