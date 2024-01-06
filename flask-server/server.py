from flask import Flask, request, jsonify;
from flask_cors import CORS, cross_origin;
from flask import jsonify;
import mysql.connector;
import pandas as pd;
from datetime import date;
from numpy import dtype;
import numpy as np;

app = Flask(__name__)
CORS(app, origins=['http://localhost:3000'], support_credentials=True)

host = 'Ananyas-MacBook.local'
port = 3306
user = 'root'
password = 'Yellow_mellow2'
database = 'forecasting'

## function to connect to the SQL server
def SQLconnect():
    connection = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database
    )
    return connection

connection = SQLconnect()

## Checking if SQL server is connected
if connection.is_connected():
    print("Connected to MySQL server")
else:
    print("Failed to connect to MySQL server")

cursor = connection.cursor()

## SQL commands to insert into forecast and actual tables
fcast_table = "INSERT INTO fcast (SRC_SYS_CD, YEAR, MONTH, WEEK, MATL_NUM, LOC, FCAST) VALUES (%s, %s, %s, %s, %s, %s, %s)"
actual_table = "INSERT INTO actual (SRC_SYS_CD, YEAR, MONTH, WEEK, MATL_NUM, LOC, ACTUAL) VALUES (%s, %s, %s, %s, %s, %s, %s)"

## SQL commands to clear data from tables (so the data is refreshed with every use/user)
delete_fcast = "DELETE FROM fcast"
delete_actual = "DELETE FROM actual"

delete_null = "DELETE from productdesc where BRAND = ' ' OR BRAND IS NULL;"


## Storing the current year so that actual data can be filtered accordingly
today = date.today()
year = today.strftime("%Y")

data_list = []

## Cursor and connection is temporarily closed
cursor.close()
connection.close()

## CSV data is posted to these endpoint
@app.route("/fcast", methods=['POST', 'GET'])
@cross_origin(supports_credentials=True)
def forecast():
    if request.method == 'GET':
        return jsonify([])
    if request.method == 'POST':
        connection = SQLconnect()
        cursor = connection.cursor()
        csv_data_fcast = request.json['fcast']
        forecast = pd.DataFrame(csv_data_fcast, columns=['CONTINENT', 'MATL_NUM', 'LOC', 'FISC_YR_NBR', 'FISC_MO_NBR', 'FISC_WK_NBR', 'FCAST'])
        forecast = forecast.replace('', np.nan, inplace=False)
        forecast = forecast.dropna()
        continent = forecast['CONTINENT'].tolist()
        fisc_yr_nbr = forecast['FISC_YR_NBR'].tolist()
        fisc_mo_nbr = forecast['FISC_MO_NBR'].tolist()
        fisc_wk_nbr = forecast['FISC_WK_NBR'].tolist()
        matl_num = forecast['MATL_NUM'].tolist()
        loc = forecast['LOC'].tolist()
        fcast = forecast['FCAST'].tolist()

        forecast.columns = ['CONTINENT', 'MATL_NUM', 'LOC', 'YEAR', 'MONTH', 'WEEK', 'FCAST']
        if connection.is_connected():
            data_list.clear()
            for i in range(len(fisc_yr_nbr)):
                if (i != 0):
                    data = (
                        continent[i],
                        fisc_yr_nbr[i],
                        fisc_mo_nbr[i],
                        fisc_wk_nbr[i],
                        matl_num[i],
                        loc[i],
                        fcast[i]
                    )
                    data_list.append(data)  # Append each data tuple to the list

            try:
                cursor.executemany(fcast_table, data_list)  # Pass the list of data tuples
                # fix later cursor.execute(delete_null) 
                connection.commit()
                cursor.close()
                connection.close()
                return jsonify({'status': 'success'})
            except Exception as e:
                connection.rollback()
                return jsonify({'status': 'error', 'message': str(e)})
        else:
            # Handle the case where the connection is lost
            print("Connection to MySQL server is lost.")

@app.route("/matlnumdata", methods=['GET'])
@cross_origin(supports_credentials=True)
def matlnum():
    connection = SQLconnect()
    cursor = connection.cursor()

    # Fetch the forecast data
    cursor.execute('SELECT MATL_NUM, YEAR, MONTH, WEEK, FCAST FROM fcast')
    fcast_data = cursor.fetchall()
    fcast_data = pd.DataFrame(fcast_data, columns=['MATL_NUM', 'YEAR', 'MONTH', 'WEEK', 'FCAST'])
    fcast_data = fcast_data.groupby('MATL_NUM').apply(lambda x: x.drop('MATL_NUM', axis=1).to_dict('records')).to_dict()

    # Fetch the actual data
    cursor.execute('SELECT MATL_NUM, YEAR, MONTH, WEEK, ACTUAL FROM actual')
    actual_data = cursor.fetchall()
    actual_data = pd.DataFrame(actual_data, columns=['MATL_NUM', 'YEAR', 'MONTH', 'WEEK', 'ACTUAL'])
    actual_data = actual_data.groupby('MATL_NUM').apply(lambda x: x.drop('MATL_NUM', axis=1).to_dict('records')).to_dict()

    # Close the cursor and connection
    cursor.close()
    connection.close()

    response = jsonify(fcast=fcast_data, actual=actual_data)
    response.headers.add('Access-Control-Allow-Origin', 'http:localhost:3000')  # Replace '*' with the appropriate frontend URL
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET')

    # Return the forecast and actual data as separate dictionaries
    return jsonify(fcast=fcast_data, actual=actual_data)

@app.route("/locationdata")
@cross_origin(supports_credentials=True)
def location():
    connection = SQLconnect()
    cursor = connection.cursor()

    # Fetch the forecast data
    cursor.execute('SELECT LOC, YEAR, MONTH, WEEK, FCAST FROM fcast')
    fcast_data = cursor.fetchall()
    fcast_data = pd.DataFrame(fcast_data, columns=['LOC', 'YEAR', 'MONTH', 'WEEK', 'FCAST'])
    fcast_data = fcast_data.groupby('LOC').apply(lambda x: x.drop('LOC', axis=1).to_dict('records')).to_dict()

    # Fetch the actual data
    cursor.execute('SELECT LOC, YEAR, MONTH, WEEK, ACTUAL FROM actual')
    actual_data = cursor.fetchall()
    actual_data = pd.DataFrame(actual_data, columns=['LOC', 'YEAR', 'MONTH', 'WEEK', 'ACTUAL'])
    actual_data = actual_data.groupby('LOC').apply(lambda x: x.drop('LOC', axis=1).to_dict('records')).to_dict()

    # Close the cursor and connection
    cursor.close()
    connection.close()

    response = jsonify(fcast=fcast_data, actual=actual_data)
    response.headers.add('Access-Control-Allow-Origin', 'http:localhost:3000')  # Replace '*' with the appropriate frontend URL
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET')

    # Return the forecast and actual data as separate dictionaries
    return jsonify(fcast=fcast_data, actual=actual_data)

@app.route("/branddata")
@cross_origin(supports_credentials=True)
def brand():
    connection = SQLconnect()
    cursor = connection.cursor()

    # Fetch the brand data 
    cursor.execute('SELECT MATL_NUM, BRAND from productdesc')
    brand_data = cursor.fetchall()
    brand_data = pd.DataFrame(brand_data, columns=['MATL_NUM', 'BRAND'])

    # Fetch the forecast data
    cursor.execute('SELECT MATL_NUM, YEAR, MONTH, WEEK, FCAST FROM fcast')
    fcast_data = cursor.fetchall()
    fcast_data = pd.DataFrame(fcast_data, columns=['MATL_NUM', 'YEAR', 'MONTH', 'WEEK', 'FCAST'])

    # Fetch the actual data
    cursor.execute('SELECT MATL_NUM, YEAR, MONTH, WEEK, ACTUAL FROM actual')
    actual_data = cursor.fetchall()
    actual_data = pd.DataFrame(actual_data, columns=['LOC', 'YEAR', 'MONTH', 'WEEK', 'ACTUAL'])

    # Transform the dataset into a forecast brand dataset
    for x in fcast_data['MATL_NUM']:
        if (isinstance(x, int)):
            fcast_data.loc[(fcast_data['MATL_NUM'] == x), 'MATL_NUM'] = brand_data['BRAND'][brand_data.MATL_NUM.values.tolist().index(x)]

    fcast_data.rename(columns={'MATL_NUM':'BRAND'})
    fcast_data = fcast_data.groupby('BRAND').apply(lambda x: x.drop('BRAND', axis=1).to_dict('records')).to_dict()

    # Transform the dataset into an actual brand dataset
    for y in actual_data['MATL_NUM']:
        if (isinstance(y, int)):
            actual_data.loc[(actual_data['MATL_NUM'] == x), 'MATL_NUM'] = brand_data['BRAND'][brand_data.MATL_NUM.values.tolist().index(x)]

    actual_data.rename(columns={'MATL_NUM':'BRAND'})
    actual_data = actual_data.groupby('BRAND').apply(lambda x: x.drop('BRAND', axis=1).to_dict('records')).to_dict()
    return jsonify(fcast=fcast_data,actual=actual_data)

@app.route("/categorydata")
@cross_origin(supports_credentials=True)
def category():
    return jsonify([])

@app.route("/continentdata")
@cross_origin(supports_credentials=True)
def continent():
    connection = SQLconnect()
    cursor = connection.cursor()

    # Fetch the forecast data
    cursor.execute('SELECT SRC_SYS_CD, YEAR, MONTH, WEEK, FCAST FROM fcast')
    fcast_data = cursor.fetchall()
    fcast_data = pd.DataFrame(fcast_data, columns=['CONTINENT', 'YEAR', 'MONTH', 'WEEK', 'FCAST'])
    fcast_data = fcast_data.groupby('CONTINENT').apply(lambda x: x.drop('CONTINENT', axis=1).to_dict('records')).to_dict()

    # Fetch the actual data
    cursor.execute('SELECT SRC_SYS_CD, YEAR, MONTH, WEEK, ACTUAL FROM actual')
    actual_data = cursor.fetchall()
    actual_data = pd.DataFrame(actual_data, columns=['CONTINENT', 'YEAR', 'MONTH', 'WEEK', 'ACTUAL'])
    actual_data = actual_data.groupby('CONTINENT').apply(lambda x: x.drop('CONTINENT', axis=1).to_dict('records')).to_dict()

    # Close the cursor and connection
    cursor.close()
    connection.close()

    response = jsonify(fcast=fcast_data, actual=actual_data)
    response.headers.add('Access-Control-Allow-Origin', 'http:localhost:3000')  # Replace '*' with the appropriate frontend URL
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type, Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET')

    # Return the forecast and actual data as separate dictionaries
    return jsonify(fcast=fcast_data, actual=actual_data)

@app.route("/members")
@cross_origin(supports_credentials=True)
def members():
    return {"members": ["Member1", "Member2", "Member3"]}

@app.route("/actual", methods=['POST', 'GET'])
@cross_origin(supports_credentials=True)
def actual():
    if request.method == 'GET':
        return jsonify([])
    if request.method == 'POST':
        connection = SQLconnect()
        cursor = connection.cursor()
        csv_data_actual = request.json['actual']
        actual = pd.DataFrame(csv_data_actual, columns=['CONTINENT', 'MATL_NUM', 'LOC', 'FISC_YR_NBR', 'FISC_MO_NBR', 'FISC_WK_NBR', 'ACTUAL'])
        actual = actual.iloc[1: ]
        actual['FISC_YR_NBR'] = actual['FISC_YR_NBR'].astype(int)
        actual = actual[actual['FISC_YR_NBR'] >= (int(year) - 1)]
        actual.drop(actual[actual['ACTUAL'] == ''].index, inplace=True)
        continent = actual['CONTINENT'].tolist()
        fisc_yr_nbr = actual['FISC_YR_NBR'].tolist()
        fisc_mo_nbr = actual['FISC_MO_NBR'].tolist()
        fisc_wk_nbr = actual['FISC_WK_NBR'].tolist()
        matl_num = actual['MATL_NUM'].tolist()
        loc = actual['LOC'].tolist()
        fcast = actual['ACTUAL'].tolist()

        actual.columns = ['CONTINENT','MATL_NUM', 'LOC', 'YEAR', 'MONTH', 'WEEK', 'FCAST']
        if connection.is_connected():
            data_list.clear()
            for i in range(len(fisc_yr_nbr)):
                if (i != 0):
                    data = (
                        continent[i],
                        fisc_yr_nbr[i],
                        fisc_mo_nbr[i],
                        fisc_wk_nbr[i],
                        matl_num[i],
                        loc[i],
                        fcast[i]
                    )
                    data_list.append(data)  # Append each data tuple to the list

            try:
                cursor.executemany(actual_table, data_list)  # Pass the list of data tuples
                # fix later cursor.execute(delete_null)
                connection.commit()
                cursor.close()
                connection.close()
                return jsonify({'status': 'success'})
            except Exception as e:
                connection.rollback()
                return jsonify({'status': 'error', 'message': str(e)})
        else:
            # Handle the case where the connection is lost
            print("Connection to MySQL server is lo st.")

@app.route("/reset", methods=['GET'])
@cross_origin(supports_credentials=True)
def reset():
    connection = SQLconnect()
    cursor = connection.cursor()
    cursor.execute(delete_fcast)
    cursor.execute(delete_actual)
    connection.commit()
    print("Deleted forecast and actual values in mySQL")

    cursor.close()
    connection.close()
    return "successful delete"

    
@app.after_request
@cross_origin(supports_credentials=True)
def set_response_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "http://localhost:3000"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST"
    return response

if __name__ == "__main__":
    app.run(debug=True)