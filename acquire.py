#imports
import numpy as np
import pandas as pd
from pydataset import data
import os
import env

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

def get_zillow3():
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
    select *,
	CAST(latitude / 1e6 AS DECIMAL(10, 6)) AS latitude_dd,
    CAST(longitude / 1e6 AS DECIMAL(10, 6)) AS longitude_dd
    from properties_2017
    join predictions_2017 using (parcelid)
    join propertylandusetype using (propertylandusetypeid)
    left join architecturalstyletype using (architecturalstyletypeid)
    left join airconditioningtype using (airconditioningtypeid)
    left join buildingclasstype using (buildingclasstypeid)
    left join heatingorsystemtype using (heatingorsystemtypeid)
    left join storytype using (storytypeid)
    left join typeconstructiontype using (typeconstructiontypeid)
    left join unique_properties using (parcelid)
    where transactiondate Like '2017%%'
    ;
    """
    filename = 'zillow3.csv'
    df = check_file_exists(filename, query, url)

    return df