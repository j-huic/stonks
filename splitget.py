from datafunc import *
from functions import *
import pickle

splits = get_splits()
clean_splits = clean_splits(splits)
recent_splits = clean_splits[clean_splits['date'] > '2024-01-01']

with open('all_splits.pkl', 'wb') as file:
    pickle.dump(clean_splits, file)
with open('recent_splits.pkl', 'wb') as file:
    pickle.dump(recent_splits, file)

