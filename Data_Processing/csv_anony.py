import dask.dataframe as dd
import random
import string

def data_anonymize(df):
    
    # Anonymizing the sensitive data - first_name, last_name, address
    df["First_name"] = df["First_name"].apply(
        lambda _: ''.join(random.choices(string.ascii_letters, k=8)), meta=('First_name', 'str'))
    df["Last_name"] = df["Last_name"].apply(
        lambda _: ''.join(random.choices(string.ascii_letters, k=8)), meta=('Last_name', 'str'))
    df["Address"] = df["Address"].apply(
        lambda _: ''.join(random.choices(string.ascii_letters + string.digits, k=15)), meta=('Address', 'str'))
    return df
#date of birth is not processed

# Reading Parquet file through dask library functions
parquet_file = "*\\parquet_file.parquet"
df = dd.read_parquet(parquet_file)

# Anonymize Data - passing parquet data to the function
anonymized_df = data_anonymize(df)

# Converting the parquet file to csv and saving to local machine
anonymized_df.to_csv("*\\anony_data.csv", single_file=True, index=False)
