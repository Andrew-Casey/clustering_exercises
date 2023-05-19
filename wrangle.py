import pandas as pd
import numpy as np


import seaborn as sns
import matplotlib.pyplot as plt

import env
import os
import wrangle as w

def check_file_exists(fn, query, url):
    """
    check if file exists in my local directory, if not, pull from sql db
    return dataframe
    """
    if os.path.isfile(fn):
        print('csv file found and loaded')
        return pd.read_csv(fn, index_col=0)
    else: 
        print('creating df and exporting csv')
        df = pd.read_sql(query, url)
        df.to_csv(fn)
        return df 

def get_zillow():
    """
    Retrieves Zillow data from a MySQL database and returns it as a Pandas DataFrame.

    The function connects to a MySQL database using the provided database URL
    and executes a query to fetch Zillow data for single-family residential properties
    from the year 2017. The data includes the tax value, square footage, county code,
    number of bedrooms, and number of bathrooms.

    Returns:
        pandas.DataFrame: Zillow data for single-family residential properties in 2017.

    Example:
        >>> df = get_zillow()
        >>> df.head()
           Tax_Value    Sqft  County  Bedrooms  Bathrooms
        0   360170.0  1316.0  6037.0       3.0        2.0
        1   585529.0  1458.0  6059.0       3.0        2.0
        2   119906.0  1421.0  6037.0       2.0        1.0
        3   244880.0  2541.0  6037.0       4.0        3.0
        4   434551.0  1491.0  6059.0       3.0        2.0
    """
    url = env.get_db_url('zillow')

    query = """
    select taxvaluedollarcnt as Tax_Value, calculatedfinishedsquarefeet as Sqft,
           fips as County, roomcnt as Bedrooms, fullbathcnt as Bathrooms
    from properties_2017
    join predictions_2017 using (parcelid)
    join propertylandusetype using (propertylandusetypeid)
    where propertylandusedesc Like 'Single Family Residential'
        and transactiondate Like '2017%%'
    """
    filename = 'zillow.csv'
    df = check_file_exists(filename, query, url)

    return df

def get_zillow2():
    """
    Retrieves extended Zillow data from a MySQL database and returns it as a Pandas DataFrame.

    The function connects to a MySQL database using the provided database URL
    and executes a query to fetch extended Zillow data for single-family residential properties
    from the year 2017. The data includes the tax value, square footage, county code, number of bedrooms,
    number of bathrooms, lot size, year built, and pool count. The additional information includes
    story type and construction type, if available.

    Returns:
        pandas.DataFrame: Extended Zillow data for single-family residential properties in 2017.

    Example:
        >>> df = get_zillow2()
        >>> df.head()
           Tax_Value    Sqft  County  Bedrooms  Bathrooms  Lot_Size  Year_Built  Pool
        0   360170.0  1316.0  6037.0       3.0        2.0    12647.0      1923.0   1.0
        1   585529.0  1458.0  6059.0       3.0        2.0     9035.0      1970.0   0.0
        2   119906.0  1421.0  6037.0       2.0        1.0     7500.0      1911.0   0.0
        3   244880.0  2541.0  6037.0       4.0        3.0     8777.0      2003.0   0.0
        4   434551.0  1491.0  6059.0       3.0        2.0     6388.0      1955.0   0.0
    """
    url = env.get_db_url('zillow')

    query = """
    select taxvaluedollarcnt as Tax_Value, calculatedfinishedsquarefeet as Sqft,
           fips as County, bedroomcnt as Bedrooms, bathroomcnt as Bathrooms,
           lotsizesquarefeet as Lot_Size, yearbuilt as Year_Built, poolcnt as Pool
    from properties_2017
    join predictions_2017 using (parcelid)
    join propertylandusetype using (propertylandusetypeid)
    left join storytype using (storytypeid)
    left join typeconstructiontype using (typeconstructiontypeid)
    where propertylandusedesc Like 'Single Family Residential'
        and transactiondate Like '2017%%'
    """
    filename = 'zillow2.csv'
    df = check_file_exists(filename, query, url)

    return df

def wrangle_zillow():
    """
    Performs data wrangling on Zillow dataset.

    This function retrieves the Zillow dataset using the `get_zillow()` function and performs
    several data wrangling steps. It changes FIPS codes to county names, creates dummy variables
    for the county column, drops null values, and handles outliers for the tax value, square footage,
    number of bedrooms, and number of bathrooms.

    Returns:
        pandas.DataFrame: Wrangled Zillow dataset.

    Example:
        >>> df = wrangle_zillow()
        >>> df.head()
           Tax_Value    Sqft County  Bedrooms  Bathrooms  Lot_Size  Year_Built  Pool  LA  Orange  Ventura
        0   360170.0  1316.0     LA       3.0        2.0   12647.0      1923.0   1.0   1       0        0
        1   585529.0  1458.0  Orange      3.0        2.0    9035.0      1970.0   0.0   0       1        0
        2   119906.0  1421.0     LA       2.0        1.0    7500.0      1911.0   0.0   1       0        0
        3   244880.0  2541.0     LA       4.0        3.0    8777.0      2003.0   0.0   1       0        0
        4   434551.0  1491.0  Orange      3.0        2.0    6388.0      1955.0   0.0   0       1        0
    """
    # Load Zillow database
    df = w.get_zillow()

    # Change FIPS codes to county name
    df['County'] = df['County'].replace([6037.0, 6059.0, 6111.0], ['LA', 'Orange', 'Ventura']).astype(str)

    # Create dummy variables for the county column
    dummy_df = pd.get_dummies(df['County'], drop_first=False)
    df = pd.concat([df, dummy_df], axis=1)

    # Drop all nulls
    df = df.dropna()

    # Handle outliers
    df = df[df.Tax_Value <= 2000000]
    df = df[df.Sqft <= 6000]
    df = df[df.Bedrooms <= 8]
    df = df[df.Bathrooms <= 5]

    return df

def wrangle_zillow2():
    """
    Performs data wrangling on extended Zillow dataset.

    This function retrieves the extended Zillow dataset using the `get_zillow2()` function and performs
    several data wrangling steps. It changes FIPS codes to county names, creates dummy variables for the
    county column, fills null values in the 'Pool' column with 0, drops remaining null values, and handles
    outliers for the tax value, square footage, number of bedrooms, number of bathrooms, lot size, and
    year built.

    Returns:
        pandas.DataFrame: Wrangled extended Zillow dataset.

    Example:
        >>> df = wrangle_zillow2()
        >>> df.head()
           Tax_Value    Sqft County  Bedrooms  Bathrooms  Lot_Size  Year_Built  Pool  LA  Orange  Ventura
        0   360170.0  1316.0     LA       3.0        2.0   12647.0      1923.0   1.0   1       0        0
        1   585529.0  1458.0  Orange      3.0        2.0    9035.0      1970.0   0.0   0       1        0
        2   119906.0  1421.0     LA       2.0        1.0    7500.0      1911.0   0.0   1       0        0
        3   244880.0  2541.0     LA       4.0        3.0    8777.0      2003.0   0.0   1       0        0
        4   434551.0  1491.0  Orange      3.0        2.0    6388.0      1955.0   0.0   0       1        0
    """
    # Load extended Zillow database
    df = w.get_zillow2()

    # Change FIPS codes to county name
    df['County'] = df['County'].replace([6037.0, 6059.0, 6111.0], ['LA', 'Orange', 'Ventura']).astype(str)

    # Create dummy variables for the county column
    dummy_df = pd.get_dummies(df['County'], drop_first=False)
    df = pd.concat([df, dummy_df], axis=1)

    # Fill pool null values with 0
    df.Pool = df.Pool.fillna(0)

    # Drop all remaining nulls
    df = df.dropna()

    # Handle outliers
    df = df[df.Tax_Value <= 2000000]
    df = df[df.Sqft <= 6000]
    df = df[df.Bedrooms <= 8]
    df = df[df.Bathrooms <= 5]
    df = df[df.Lot_Size <= 12000]
    df = df[df.Year_Built >= 1920]

    return df
