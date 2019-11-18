import re
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import PolynomialFeatures
from sklearn.model_selection import train_test_split

def calculate_degree_1(inputDataframe, y_name, x_name, show_report=True, test_size=0.2):
    '''
    This finction receives dataframe, y Column name, x column name
    It then will try to calculate y_name = A*x_name + B
    The result format: (A,B)
    '''
    print("The dataframe used to calculate on has these columns: ", inputDataframe.columns)

    if (not y_name in inputDataframe.columns or not x_name in inputDataframe.columns):
        
        if not y_name in inputDataframe.columns:
            print('Unavailable column name: ', y_name)
        
        if not x_name in inputDataframe.columns:
            print('Unavailable column name: ', x_name)
        return

    # Drop nan rows
    inputDataframe = inputDataframe[[y_name,x_name]].dropna()

    # Get data
    y = inputDataframe[y_name]
    X = inputDataframe[[x_name]]

    # Split Data
    X_train, X_test, y_train, y_test = train_test_split(X,y, test_size=test_size, random_state=68)

    # LinearRegression
    lm = LinearRegression()
    lm.fit(X_train, y_train)

    y_test_predict = lm.predict(X_test)
    y_train_predict = lm.predict(X_train)

    # Pearson
    pearson_coef = inputDataframe[y_name].corr(inputDataframe[x_name], method="pearson")

    # Error
    train_error = np.sqrt(np.mean(np.square(y_train_predict - y_train)))
    test_error = np.sqrt(np.mean(np.square(y_test_predict - y_test)))

    if show_report:
        print('Pearson correlation coeeficient: ', pearson_coef)
        print('Train Error: ', train_error)
        print('Test Error: ', test_error)

    return lm.coef_[0], lm.intercept_ , pearson_coef, train_error, test_error
