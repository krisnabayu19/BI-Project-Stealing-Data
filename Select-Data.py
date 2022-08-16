from csv import DictReader
from collections import Counter
import re
import datetime
from datetime import datetime
import pymysql
import pandas as pd
import googlemaps
import numpy as np
import csv
import math

# Connection to MySQL

db1 = pymysql.connect(host='localhost', user='root', password='', charset='utf8', db='db_curanmor_data')
cursor1 = db1.cursor()

def calculation_data():

    # cursor1.execute("TRUNCATE TABLE tb_data_place_nearby_toko")

    query = "SELECT * FROM tb_data_place_nearby_swalayan ;"
    data = pd.read_sql(query, db1)

    for index, row in data.iterrows():
        name = row['name']
        business_status = row['business_status']
        latitude = row['latitude']
        longitude = row['longitude']
        status = row['status']
        core_latitude = row['core_latitude']
        core_longitude = row['core_longitude']
        core_id = row['core_id']
        kategori = 'Swalayan'


        sql = "INSERT INTO tb_data_place_nearby_rekap (name, business_status, latitude, longitude, status, core_latitude, core_longitude, core_id, kategori) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (name,business_status,latitude,longitude,status,core_latitude,core_longitude,core_id,kategori)
        print(val)
        cursor1.execute(sql, val)
        db1.commit()


# Main
if __name__ == '__main__':
    print("Start")
    calculation_data()