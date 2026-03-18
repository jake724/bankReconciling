import pandas as pd
import numpy as np

file_path_source = r'G:\My Drive\Bank Reconciliations\SMBC\Original\nyusyu_SMBC_1APR.csv'
file_path_dest = r'G:\My Drive\Bank Reconciliations\StatementImportTemplate.csv'
file_path_payee_trans = r'G:\My Drive\Bank Reconciliations\SMBC\PayeeTranslations.csv'
file_path_descrip_trans = r'G:\My Drive\Bank Reconciliations\SMBC\DescriptionTranslations.csv'

df_source = pd.read_csv(file_path_source, sep=",", encoding="shift_jis")
df_dest = pd.read_csv(file_path_dest,sep=",")
df_payee_trans = pd.read_csv(file_path_payee_trans, sep=",", encoding="shift_jis")
df_descrip_trans = pd.read_csv(file_path_descrip_trans, sep=",", encoding="shift_jis")

# ==================================
# COPY TRANSACTION VALUES
# ==================================

# Find rows where transcation type = 1 or 2 (for debit or credit)
mask = (df_source.iloc[:, 2] == 1) | (df_source.iloc[:, 2] == 2)

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

# ===================================
# COPY AND CONVERT DATES
# ===================================

dates_to_copy = df_source.loc[mask].iloc[:, 1].values.copy()
converted_dates = dates_to_copy

for i in range(len(converted_dates)):
    date_to_convert = dates_to_copy[i]
    converted_dates[i] = f"{int(date_to_convert[-2:]):02d}/{int(date_to_convert[2:4]):02d}/{int(date_to_convert[:2])+2018:02d}"

df_dest.iloc[0:end_row, 0] = converted_dates

# ====================================
# TRANSLATE PAYEE & DESCRIPTION
# ====================================


i = 0
# if there's detail in payee section of bank statement
if pd.notna(df_source.iloc[i,12]):
    # look up value and get translated value
    translation = df_payee_trans[df_payee_trans.iloc[:,0] == df_source.iloc[i,12]].iloc[0,1]
    # paste translation into dest file
    if len(translation) > 0:
        df_dest.iloc[i, 2] = translation
    # if there is detail in payee section of bank statement but no translation - add into translation sheet
    else:
        required_rows = 1 + len(df_payee_trans.iloc[:,0])
        if len(df_payee_trans) < required_rows:
            # Add empty rows to reach the required length
            df_payee_trans = df_payee_trans.reindex(range(required_rows))
        # put in payee detail with no translation into the sheet (haven't yet tested)
        df_payee_trans.iloc[required_rows:0] = df_source.iloc[i,12]

print(f"length of translations is {len(df_payee_trans.iloc[:,0])}")

print(f"translation is {translation}")

print(f"katakana of first payee is {df_source.iloc[0,12]}")

# ====================================
# PRINT TESTS AND SAVE
# ====================================

#print(f"first date to convert is {date_to_convert}")
#print(f"converted date is {converted_date}")

#print(f"first date to convert is type {type(date_to_convert)}")
#print(f"values to copy are {values_to_copy}")
#print(f"dates to copy are {dates_to_copy}")
#print(f"date is size {dates_to_copy.shape}")

# Save destination file
df_dest.to_csv(file_path_dest, index=False)
    
