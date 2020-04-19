#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Importing dependencies
import json
import pandas as pd
import numpy as np
import re
import time
from sqlalchemy import create_engine
from config import db_password


# In[2]:


# Creating Connection to Postgres DB
db_string = f"postgres://postgres:{db_password}@127.0.0.1:5432/Movie_Data"
engine = create_engine(db_string)


# In[3]:


file_dir = 'Data/'


# In[4]:


# Import Wikipedia json file
with open(f'{file_dir}wikipedia.movies.json',mode='r') as file:
    wiki_movies_raw = json.load(file)


# In[5]:


# Import Kaggle data
kaggle_metadata = pd.read_csv(f'{file_dir}movies_metadata.csv',low_memory=False)


# In[6]:


# Cleaning Movie Meta (Kaggle) Data
# Removing Movies rated Adult
kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
# Converting DataTypes
kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])


# In[7]:


# Import Rating Data
ratings = pd.read_csv(f'{file_dir}ratings.csv')


# In[8]:


# Cleaning Rating Data (Converting timestamp)
ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')


# In[9]:


# Cleaning Wikipedia Data
# Removing TV Series and Movies without imdb URL or director
wiki_movies = [movie for movie in wiki_movies_raw
              if ('Director' in movie or 'Directed by' in movie)
                  and 'imdb_link' in movie
                  and 'No. of episodes' not in movie]


# In[10]:


# Function to combine all alternate titles and to rename columns
def clean_movie(movie):
    movie = dict(movie) # Create a non-destructive copy
    alt_titles = {}

    # Combine alternate titles into one list
    for key in ['Also known as','Arabic','Cantonese','Chinese','French','Hangul','Hebrew','Hepburn',
                'Japanese','Literally','Mandarin','McCune–Reischauer','Original title','Polish',
                'Revised Romanization','Romanized','Russian','Simplified','Traditional','Yiddish']:
        if key in movie:
            alt_titles[key] = movie[key]
            movie.pop(key)
    if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles
    
    # Merge column names
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)
            
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Directed by', 'Director')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Original release', 'Release date')
    change_column_name('Music by', 'Composer(s)')
    change_column_name('Produced by', 'Producer(s)')
    change_column_name('Producer', 'Producer(s)')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Released', 'Release Date')
    change_column_name('Release Date', 'Release date')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Theme music composer', 'Composer(s)')
    change_column_name('Written by', 'Writer(s)')
    
    return movie


# In[11]:


# Creating DataFrame of cleaned wiki data
clean_movies = [clean_movie(movie) for movie in wiki_movies]
wiki_movies_df = pd.DataFrame(clean_movies)


# In[12]:


# Extracting imbd id from imdb url and Removing duplicates
wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
wiki_movies_df.drop_duplicates(subset='imdb_link', inplace=True)


# In[13]:


# Remove column if 90% or more of its value is null
wiki_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df) * 0.9]
wiki_movies_df = wiki_movies_df[wiki_columns_to_keep]


# In[14]:


# Function to convert a string to a floating-point number
def parse_dollars(s):
    # if s is not a string, return NaN
    if type(s) != str:
        return np.nan

    # if input is of the form $###.# million
    if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " million"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a million
        value = float(s) * 10**6

        # return value
        return value

    # if input is of the form $###.# billion
    elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

        # remove dollar sign and " billion"
        s = re.sub('\$|\s|[a-zA-Z]','', s)

        # convert to float and multiply by a billion
        value = float(s) * 10**9

        # return value
        return value

    # if input is of the form $###,###,###
    elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

        # remove dollar sign and commas
        s = re.sub('\$|,','', s)

        # convert to float
        value = float(s)

        # return value
        return value

    # otherwise, return NaN
    else:
        return np.nan


# In[15]:


# Converting Data Types for Box Office
# Create list of Box Office and drop null values
box_office = wiki_movies_df['Box office'].dropna()
# Convert any lists to strings
box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
# Remove any values or texts between a doller sign and a hyphen
box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)

# regular expression for $###.# million or $###.# billion
form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
# regular expression for $###,###,###
form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'


# In[16]:


# Apply function and remove original column
wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
wiki_movies_df.drop('Box office', axis=1, inplace=True)


# In[17]:


# Converting Data Types for Budget
# Create list of budget and drop null values
budget = wiki_movies_df['Budget'].dropna()
# Convert any lists to strings
budget = budget.apply(lambda x: ' '.join(x) if type(x) == list else x)
# Remove any values or texts between a doller sign and a hyphen
budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
# Remove square brackets
budget = budget.str.replace(r'\[\d+\]\s*', '')


# In[18]:


# Apply function and remove original column
wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
wiki_movies_df.drop('Budget', axis=1, inplace=True)


# In[19]:


# Converting Data Types for Release Date
# Create list of budget, drop null values and convert any lists to strings
release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

# regular expression for full month name, one- to two-digit day, four-digit year (i.e., January 1, 2000)
date_form_one = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s[123]\d,\s\d{4}'
# regular expression for four-digit year, two-digit month, two-digit day, with any separator (i.e., 2000-01-01)
date_form_two = r'\d{4}.[01]\d.[123]\d'
# regular expression for full month name, four-digit year (i.e., January 2000)
date_form_three = r'(?:January|February|March|April|May|June|July|August|September|October|November|December)\s\d{4}'
# regular expression for four-digit year
date_form_four = r'\d{4}'

# Converting Strings to DateTime and removing original column
wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
wiki_movies_df.drop('Release date', axis=1, inplace=True)


# In[20]:


# Converting Data Types for Running Time
# Create list of budget, drop null values and convert any lists to strings
running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)

# Converting strings to floating-point number (minutes)
running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
wiki_movies_df.drop('Running time', axis=1, inplace=True)


# In[21]:


# Merging Kaggle Data and Wiki Data
movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])


# In[22]:


# Treatment of Competing Columns:
# Wiki                     Movielens                Resolution
#--------------------------------------------------------------------------
# title_wiki               title_kaggle             Drop Wikipedia
# running_time             runtime                  Keep Kaggle; fill in zeros with Wikipedia data
# budget_wiki              budget_kaggle            Keep Kaggle; fill in zeros with Wikipedia data
# box_office               revenue                  Keep Kaggle; fill in zeros with Wikipedia data
# release_date_wiki        release_date_kaggle      Drop Wikipedia
# Language                 original_language        Drop Wikipedia
# Production company(s)    production_companies     Drop Wikipedia


# In[23]:


# Dropping Wikipedia Data
movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)


# In[24]:


# Create Function to fill blank row of Kaggle data with Wikipedia Data
def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
    df[kaggle_column] = df.apply(
        lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
        , axis=1)
    df.drop(columns=wiki_column, inplace=True)


# In[25]:


# Applying functions to running time, budget and revenue
fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')


# In[26]:


# Removing column with non-valuable data
movies_df.drop('video', axis=1, inplace=True)


# In[27]:


# Reordering Columns
movies_df = movies_df[['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]                      


# In[28]:


# Renaming Columns
movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)


# In[29]:


# Use following DataFrame including ratings, if required
#rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
#                .rename({'userId':'count'}, axis=1) \
#                .pivot(index='movieId',columns='rating', values='count')
#rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]
#movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
#movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)


# In[30]:


movies_df.to_sql(name='movies', con=engine, if_exists='replace')


# In[31]:


rows_imported = 0
# get the start_time from time.time()
start_time = time.time()
for data in pd.read_csv(f'{file_dir}ratings.csv', chunksize=1000):
    # If data already exists in Table ratings, we need to replace in the first step
    if rows_imported == 0:
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        data.to_sql(name='ratings', con=engine, if_exists='replace')
    elif rows_imported == 1000:
        print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
        data.to_sql(name='ratings', con=engine, if_exists='append')
    else:
        break
    
    # Please note that the process shown above is intentionally interrupted after uploading
    # 2000 data points to save time (no point of uploading all data from ratings for assignment).
    # To upload all data, change chunksize to 100000, Replace elif by else, and delete break      
    
    rows_imported += len(data)
    # add elapsed time to final print out
    print(f'Done. {time.time() - start_time} total seconds elapsed.')
print('END')

