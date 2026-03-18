import pandas as pd
import numpy as np

file_path_source = r'G:\My Drive\Bank Reconciliations\SMBC\Original\nyusyu_SMBC_1APR.csv'
file_path_dest = r'G:\My Drive\Bank Reconciliations\StatementImportTemplate.csv'

df_source = pd.read_csv(file_path_source, sep=",", encoding="shift_jis")
df_dest = pd.read_csv(file_path_dest,sep=",")

#print(df_source.head())
#print(df_dest.head())

# Find rows where transcation type = 1 or 2 (for debit or credit)
mask = (df_source.iloc[:, 2] == 1) | (df_source.iloc[:, 2] == 2)

print(f"mask is {mask}")

# Get the values from third column where condition is met
values_to_copy = df_source.loc[mask].iloc[:, 4].values.copy()

# Get the transaction types for the filtered rows
transaction_types = df_source.loc[mask].iloc[:, 2].values

# Make values negative where transaction type == 2
values_to_copy[transaction_types == 2] = -values_to_copy[transaction_types == 2]

# Ensure df_dest has enough rows (extend if needed)
required_rows = 1 + len(values_to_copy)
if len(df_dest) < required_rows:
    # Add empty rows to reach the required length
    df_dest = df_dest.reindex(range(required_rows))


# Write to destination starting at row 2 (index 2)
# This overwrites rows 2, 3, 4, etc. depending on how many values match
end_row = len(values_to_copy)
df_dest.iloc[0:end_row, 1] = values_to_copy

#print("number of values to copy is" ,len(values_to_copy))
print(f"values to copy is {values_to_copy}")
print(values_to_copy.size)
print(values_to_copy.shape)
print(values_to_copy)
print(f"end row is {end_row}")
# Save destination file
df_dest.to_csv(file_path_dest, index=False)

#df_dest = pd.read_csv(file_path_dest)

#print(df_source.head())
#print(df_dest.head())