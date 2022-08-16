import pymysql
import pandas as pd
import math

# Connection to MySQL
db1 = pymysql.connect(host='localhost', user='root', password='', charset='utf8', db='db_curanmor_data')
cursor1 = db1.cursor()

def calculation_data():

    cursor1.execute("TRUNCATE TABLE tb_data_calculation_place_nearby")

    query = open("data_calculation_6.sql", "r")
    query = query.read()
    data = pd.read_sql(query, db1)

    for index, row in data.iterrows():
        core_id = row['core_id']
        latitude = row['latitude']
        longitude = row['longitude']
        desa_kelurahan = row['desa_kelurahan']
        kecamatan = row['kecamatan']
        jumlah_kejahatan = row['jumlah_kejahatan']
        jumlah_toko_terdampak = row['jumlah_toko_terdampak']
        jumlah_pasar_terdampak = row['jumlah_pasar_terdampak']
        jumlah_rumahsakit_terdampak = row['jumlah_rumahsakit_terdampak']
        jumlah_swalayan_terdampak = row['jumlah_swalayan_terdampak']
        calculation = row['calculation']
        priority_status = row['priority_status']

        # Check Toko
        if math.isnan(jumlah_toko_terdampak):
            jumlah_toko_terdampak = None
        else:
            jumlah_toko_terdampak = jumlah_toko_terdampak

        # Check Pasar
        if math.isnan(jumlah_kejahatan):
            jumlah_kejahatan = None
        else:
            jumlah_kejahatan = jumlah_kejahatan

        # Check Pasar
        if math.isnan(jumlah_pasar_terdampak):
            jumlah_pasar_terdampak = None
        else:
            jumlah_pasar_terdampak = jumlah_pasar_terdampak

        # Check Rumah Sakit
        if math.isnan(jumlah_rumahsakit_terdampak):
            jumlah_rumahsakit_terdampak = None
        else:
            jumlah_rumahsakit_terdampak = jumlah_rumahsakit_terdampak

        # Check Swalayan
        if math.isnan(jumlah_swalayan_terdampak):
            jumlah_swalayan_terdampak = None
        else:
            jumlah_swalayan_terdampak = jumlah_swalayan_terdampak

        # Check Calculation
        if math.isnan(calculation):
            calculation = None
        else:
            calculation = calculation


        sql = "INSERT INTO tb_data_calculation_place_nearby (core_id, latitude, longitude, desa_kelurahan, kecamatan, jumlah_kejahatan, jumlah_toko_terdampak, jumlah_pasar_terdampak, jumlah_rumahsakit_terdampak, jumlah_swalayan_terdampak,calculation,priority_status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (core_id, latitude, longitude, desa_kelurahan, kecamatan,jumlah_kejahatan,jumlah_toko_terdampak,jumlah_pasar_terdampak,jumlah_rumahsakit_terdampak,jumlah_swalayan_terdampak,calculation,priority_status)
        print(val)
        cursor1.execute(sql, val)
        db1.commit()


# Main
if __name__ == '__main__':
    print("Start")
    calculation_data()
