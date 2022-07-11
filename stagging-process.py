from csv import DictReader
from collections import Counter
import re
import datetime
import pymysql
import psycopg2

# Connection to MySQL
db = pymysql.connect(host='localhost', user='root', password='', charset='utf8', db='db_curanmor')
cursor = db.cursor()

def etl_process():

    cursor.execute("TRUNCATE TABLE tb_stagging_data_csv")

    # open file in read mode
    with open('data_rekap_2013.csv', 'r', encoding='unicode_escape') as read_obj:
        csv_dict_reader = DictReader(read_obj)

        # Looping to Get per Each Row on The Data
        for row in csv_dict_reader:
            no = row['NO']
            dasar = row['DASAR']
            waktu_kejadian = row['WAKTU KEJADIAN']
            tkp = row['TKP']
            pelapor = row['PELAPOR']
            jenis = row['JENIS']
            no_pol = row['NO. POL']
            no_ka = row['NO. KA']
            no_sin = row['NO. SIN']
            modus = row['MODUS']
            kecamatan = row['Kecamatan']

            # Storing data to SQL database
            sql = "INSERT INTO tb_stagging_data_csv (dasar,waktu_kejadian,tkp,pelapor,jenis,no_pol,no_ka,no_sin,modus,kecamatan) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            val = (dasar,waktu_kejadian,tkp,pelapor,jenis,no_pol,no_ka,no_sin,modus,kecamatan)
            print(val)
            # cursor.execute(sql, val)
            # db.commit()

# Main
if __name__ == '__main__':
    print("Start")
    etl_process()
