import requests
import datetime
from pymongo import MongoClient
from urllib.parse import quote_plus
import time

# URL encode username and password
username = quote_plus("Mr_Neeraj")
password = quote_plus("2003@nani")

# Connect to MongoDB Atlas
client = MongoClient(f"mongodb+srv://{username}:{password}@weather-station-vignan.nmrjxmb.mongodb.net/?retryWrites=true&w=majority&appName=Weather-Station-Vignan")

# Create/use a database
db = client['weather_db']

# Create/use a collection (similar to a table in SQL)
collection = db['weather_data']

# Function to fetch and insert weather data
def fetch_and_insert_weather_data():
    url = "https://www.weatherlink.com/embeddablePage/summaryData/4ba5c08d52e54c5fadfa3d8b33129c5c"
    response = requests.get(url)

    if response.status_code == 200:
        weather_data = response.json()
        last_received = weather_data["lastReceived"]
        date_time = datetime.datetime.fromtimestamp(last_received / 1000)
        formatted_date_time = date_time.strftime("%Y-%m-%d %H:%M:%S")
        weather_data["formattedDateTime"] = formatted_date_time

        # Check if the timestamp already exists in the database
        if collection.count_documents({"lastReceived": last_received}) == 0:
            # Insert the weather data into the collection
            insert_result = collection.insert_one(weather_data)
            print(f"Inserted document ID: {insert_result.inserted_id}")
        else:
            print("Data with this timestamp already exists. Skipping insertion.")
    else:
        print("Error:", response.status_code)


fetch_and_insert_weather_data()
time.sleep(30)
fetch_and_insert_weather_data()

# Close the connection
client.close()
