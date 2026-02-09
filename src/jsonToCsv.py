import os
import pandas as pd

source_dir = 'output/'
df_list = []

for file in os.listdir(source_dir):
    filename = os.path.join(source_dir, file)
    df = pd.read_json(filename)
    df_list.append(df)

df_final = pd.concat(df_list)
df_final.to_csv('output.csv', index = False)
