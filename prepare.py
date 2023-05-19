import pandas as pd
import numpy as np


import wrangle as w
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler

import seaborn as sns
import matplotlib.pyplot as plt

def split_data(df):
    '''
    take in a DataFrame and target variable. return train, validate, and test DataFrames.
    return train, validate, test DataFrames.
    '''
    train_validate, test = train_test_split(df, test_size=.2, random_state=123)
    train, validate = train_test_split(train_validate, 
                                       test_size=.25, 
                                       random_state=123)
    return train, validate, test

def scaled_df(train, validate, test):
    """
    This function scales the train, validate, and test data using the MinMaxScaler.

    Parameters:
    train (pandas DataFrame): The training data.
    validate (pandas DataFrame): The validation data.
    test (pandas DataFrame): The test data.

    Returns:
    Tuple of:
        X_train_scaled (pandas DataFrame): The scaled training data.
        X_validate_scaled (pandas DataFrame): The scaled validation data.
        X_test_scaled (pandas DataFrame): The scaled test data.
        y_train (pandas Series): The target variable for the training data.
        y_validate (pandas Series): The target variable for the validation data.
        y_test (pandas Series): The target variable for the test data.
    """

    X_train = train[['Sqft','Bedrooms','Bathrooms','LA','Orange','Ventura']]
    X_validate = validate[['Sqft','Bedrooms','Bathrooms','LA','Orange','Ventura']]
    X_test = test[['Sqft','Bedrooms','Bathrooms','LA','Orange','Ventura']]

    y_train = train.Tax_Value
    y_validate = validate.Tax_Value
    y_test = test.Tax_Value

    #making our scaler
    scaler = MinMaxScaler()
    #fitting our scaler 
    # AND!!!!
    #using the scaler on train
    X_train_scaled = scaler.fit_transform(X_train)
    #using our scaler on validate
    X_validate_scaled = scaler.transform(X_validate)
    #using our scaler on test
    X_test_scaled = scaler.transform(X_test)

    # Convert the array to a DataFrame
    X_train_scaled = pd.DataFrame(X_train_scaled, columns=X_train.columns, index=X_train.index)

    # Convert the array to a DataFrame
    X_validate_scaled = pd.DataFrame(X_validate_scaled, columns=X_validate.columns, index=X_validate.index)
    
    # Convert the array to a DataFrame
    X_test_scaled = pd.DataFrame(X_test_scaled,columns=X_test.columns, index=X_test.index)
    
    return X_train_scaled, X_validate_scaled, X_test_scaled, y_train, y_validate, y_test

def scaled_df2(train, validate, test):
    """
    This function scales the train, validate, and test data using the MinMaxScaler.

    Parameters:
    train (pandas DataFrame): The training data.
    validate (pandas DataFrame): The validation data.
    test (pandas DataFrame): The test data.

    Returns:
    Tuple of:
        X_train_scaled (pandas DataFrame): The scaled training data.
        X_validate_scaled (pandas DataFrame): The scaled validation data.
        X_test_scaled (pandas DataFrame): The scaled test data.
        y_train (pandas Series): The target variable for the training data.
        y_validate (pandas Series): The target variable for the validation data.
        y_test (pandas Series): The target variable for the test data.
    """

    X_train = train[['Sqft','Bedrooms','Bathrooms','Year_Built','Pool']]
    X_validate = validate[['Sqft','Bedrooms','Bathrooms','Year_Built','Pool']]
    X_test = test[['Sqft','Bedrooms','Bathrooms','Year_Built','Pool']]

    y_train = train.Tax_Value
    y_validate = validate.Tax_Value
    y_test = test.Tax_Value

    #making our scaler
    scaler = MinMaxScaler()
    #fitting our scaler 
    # AND!!!!
    #using the scaler on train
    X_train_scaled = scaler.fit_transform(X_train)
    #using our scaler on validate
    X_validate_scaled = scaler.transform(X_validate)
    #using our scaler on test
    X_test_scaled = scaler.transform(X_test)

    # Convert the array to a DataFrame
    X_train_scaled = pd.DataFrame(X_train_scaled, columns=X_train.columns, index=X_train.index)

    # Convert the array to a DataFrame
    X_validate_scaled = pd.DataFrame(X_validate_scaled, columns=X_validate.columns, index=X_validate.index)
    
    # Convert the array to a DataFrame
    X_test_scaled = pd.DataFrame(X_test_scaled,columns=X_test.columns, index=X_test.index)
    
    return X_train_scaled, X_validate_scaled, X_test_scaled, y_train, y_validate, y_test