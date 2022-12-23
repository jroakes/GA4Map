
# Clean DataFrames from Bigquery based on report type

import re
from datetime import datetime
import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder


def clean_events(df):
    """Clean events data"""

    df = df[df.event_name.isin(['page_view', 'session_start'])]
    df['session_engaged'] = df.session_engaged.fillna(0).astype(int)
    df.dropna(subset=['user_pseudo_id', 'event_timestamp'], inplace=True)
    df['user_pseudo_id'] = df.user_pseudo_id.map(lambda x: x.replace('.', '_'))
    df['event_timestamp_dt'] = pd.to_datetime(df.event_timestamp, infer_datetime_format=True, utc=True)
    df['event_timestamp'] = df.event_timestamp_dt.map(datetime.timestamp)
    df['event_timestamp_diff'] = 0

    user_ids = df.user_pseudo_id.unique()
    df.sort_values(['event_timestamp', 'event_name'], ascending=[True, False], inplace=True)

    df['event_timestamp_diff'] = df.groupby('user_pseudo_id')['event_timestamp'].rolling(window=2).apply(np.diff).reset_index(0,drop=True)

    df.event_timestamp_diff.fillna(0, inplace=True)
  
    df.channel.fillna("(not set)", inplace=True)
    df.page_location.fillna("", inplace=True)

    # Normalize URLs and channels
    df['channel'] = df.channel.map(lambda x: x.strip().lower())
    df['page_location'] = df.page_location.map(lambda x: re.sub("(\?|\#).*$", "", x).strip().lower())
    df['page_location'] = df.page_location.map(lambda x: x if x[-1] == '/' else x + '/')

    # Double sort to make sure that `session_start` is first
    df.sort_values(['event_timestamp', 'event_name'], ascending=[True, False], inplace=True)


    return df



def user_stats(df):
    """Stats by user ids"""

    df_engaged = df[df.session_engaged == 1].groupby(['user_pseudo_id'], as_index=False).agg({'ga_session_id': 'nunique'})
    df_engaged.columns = ['user_id', 'engaged_sessions']

    df = df[df.event_name == 'page_view'].groupby(['user_pseudo_id'], as_index=False).agg({'event_date': 'nunique', 
                                                                                           'event_timestamp': 'nunique', 
                                                                                           'ga_session_id': 'nunique',
                                                                                           'page_location': ['nunique', 'first', 'last'],
                                                                                           'channel': 'first'})

    df.columns = ['user_id', 'unique_days', 'pageviews', 'total_sessions', 'unique_pages', 'first_page', 'last_page', 'initial channel']
    df = df.merge(df_engaged, on='user_id', how='left')
    df['engaged_sessions'] = df.engaged_sessions.fillna(0).astype(int)
    
    df['pct_engaged_sessions']= round(df.engaged_sessions/df.total_sessions, 2)

    df = df[['user_id', 'unique_days', 'pageviews', 'unique_pages', 
             'first_page', 'last_page', 'initial channel', 'total_sessions', 
             'engaged_sessions', 'pct_engaged_sessions']]
    
    df.sort_values(by="pageviews", ascending=False, inplace=True)

    df.reset_index(drop=True, inplace=True)
    
    return df



def add_event_labels(df, remove_duplicates=False):
    """Add labels to events"""

    oset = lambda x: list(dict.fromkeys(x).keys())

    df.sort_values(['event_timestamp_dt', 'event_name'], ascending=[True, False], inplace=True)
    
    df['label'] = "(not set)"
    df.loc[df.event_name == 'page_view', 'label'] = df[(df.event_name == 'page_view')]['page_location']
    df.loc[df.event_name == 'session_start', 'label'] = df[(df.event_name == 'session_start')]['channel']
    label_encoder = LabelEncoder()
    label_encoded = label_encoder.fit_transform(df.label.tolist())
    df['label_encoded'] = label_encoded

    df_user_label_mapping = df.groupby(['user_pseudo_id'], as_index=False).agg({'label_encoded': oset if remove_duplicates else list}) \
                                                                          .set_index('user_pseudo_id')['label_encoded'].to_dict()

    return df, df_user_label_mapping, label_encoder

