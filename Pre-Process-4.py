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


def load_data_kelurahan():
    query = open("load_data_kelurahan.sql", "r")
    query = query.read()
    data = pd.read_sql(query, db1)

    with open('data-desa-kelurahan-unique.csv', 'w', encoding='UTF8', newline='') as f:
        for index, row in data.iterrows():
            id_desa_kelurahan = row['id_desa_kelurahan']
            desa_kelurahan = row['nama_desa_kelurahan']
            print(id_desa_kelurahan,desa_kelurahan)
            strDesaKelurahan = [id_desa_kelurahan, desa_kelurahan]
            writer = csv.writer(f)
            writer.writerow(strDesaKelurahan)

def normalize_process():

    cursor1.execute("TRUNCATE TABLE tb_pelapor")
    cursor1.execute("TRUNCATE TABLE tb_merk_kendaraan")
    cursor1.execute("TRUNCATE TABLE tb_teknik_kejahatan")

    query = "SELECT * FROM tb_stagging_geo_location ;"
    data = pd.read_sql(query, db1)


    # Jenis Merk Kendaraan
    dataUniqueJenisMerk = []
    dataNumJenisMerk = 0
    dataNumArrayJenisMerk = []

    # Modus
    dataUniqueModus = []
    dataNumModus = 0
    dataNumArrayMod = []


    for index, row in data.iterrows():
        dasar = row['dasar']
        nama_pelapor = row['nama_pelapor']
        umur_pelapor = row['umur_pelapor']
        gender_pelapor = row['gender_pelapor']
        alamat_pelapor = row['alamat_pelapor']
        merk_kendaraan = row['merk_kendaraan']
        jenis_merk_kendaraan = row['jenis_merk_kendaraan']
        teknik_kejahatan = row['teknik_kejahatan']
        desa_kelurahan_tkp = row['desa_kelurahan_tkp']

        # Check Umur Pelapor
        if math.isnan(umur_pelapor):
            umur_pelapor = None
        else:
            umur_pelapor = umur_pelapor

        print(dasar)


        # Pelapor
        if nama_pelapor == '' or nama_pelapor == ' ' or nama_pelapor is None:
            print("Data Pelapor Null")
        else:
            data_nama_pelapor = nama_pelapor
            data_umur_pelapor = umur_pelapor
            data_gender_pelapor = gender_pelapor
            data_alamat_pelapor = alamat_pelapor
            sql = "INSERT INTO tb_pelapor (nama_pelapor,umur_pelapor,gender_pelapor,alamat_pelapor) VALUES (%s,%s,%s,%s)"
            val = (data_nama_pelapor,data_umur_pelapor,data_gender_pelapor,data_alamat_pelapor)
            cursor1.execute(sql, val)
            db1.commit()


        # Merk dan Jenis Kendaraan
        if merk_kendaraan== '' or merk_kendaraan == ' ' or merk_kendaraan is None:
            data_jenis = ''
        else:
            data_jenis = merk_kendaraan

        if jenis_merk_kendaraan == '' or jenis_merk_kendaraan == ' ' or jenis_merk_kendaraan is None:
            data_merk = ''
        else:
            data_merk = jenis_merk_kendaraan

        data_jenis_merk = data_jenis,data_merk
        strDataJenisMerk = ' '.join(data_jenis_merk)
        if strDataJenisMerk not in dataUniqueJenisMerk:
            dataUniqueJenisMerk.append(strDataJenisMerk)
            dataNumJenisMerk = dataNumJenisMerk + 1
            dataNumArrayJenisMerk.append(dataNumJenisMerk)

            strDataJenisMerk = strDataJenisMerk.split()
            if not strDataJenisMerk:
                print('Data Array Null')
            else:
                dataMerkFix = strDataJenisMerk[0]
                dataJenisFix = strDataJenisMerk[1:]
                strDataJenisFix = ' '.join(dataJenisFix)
                if strDataJenisFix == '':
                    strDataJenisFix = None
                else:
                    strDataJenisFix = strDataJenisFix
                sql = "INSERT INTO tb_merk_kendaraan (merk_kendaraan,jenis_merk_kendaraan) VALUES (%s,%s)"
                val = (dataMerkFix,strDataJenisFix)
                cursor1.execute(sql, val)
                db1.commit()


        # Modus
        # data_modus = pre_data_modus(modus)
        if teknik_kejahatan not in dataUniqueModus and teknik_kejahatan != ' ' and teknik_kejahatan is not None:
            dataUniqueModus.append(teknik_kejahatan)
            dataNumModus = dataNumModus + 1
            dataNumArrayMod.append(dataNumModus)
            sql = "INSERT INTO tb_teknik_kejahatan (teknik_kejahatan) VALUES (%s)"
            val = (teknik_kejahatan)
            cursor1.execute(sql, val)
            db1.commit()

    with open('data-jenis-merk-unique.csv', 'w', encoding='UTF8', newline='') as f:
        for id_jenis_merk, value_jenis_merk in zip(dataNumArrayJenisMerk, dataUniqueJenisMerk):
            strJenisMerk = [id_jenis_merk, value_jenis_merk]
            writer = csv.writer(f)
            writer.writerow(strJenisMerk)

    with open('data-modus-unique.csv', 'w', encoding='UTF8', newline='') as f:
        for id_mod, value_mod in zip(dataNumArrayMod, dataUniqueModus):
            strModus = [id_mod, value_mod]
            writer = csv.writer(f)
            writer.writerow(strModus)

def data_store():
    cursor1.execute("TRUNCATE TABLE tb_data_curanmor")

    query = "SELECT * FROM tb_stagging_geo_location ;"
    data = pd.read_sql(query, db1)

    numData = []

    for index, row in data.iterrows():
        id = row['id']
        dasar = row['dasar']
        waktu_lapor = row['waktu_lapor']
        waktu_kejadian = row['waktu_kejadian']
        tkp = row['tkp']
        lat_tkp = row['lat_tkp']
        lon_tkp = row['lon_tkp']
        nama_pelapor = row['nama_pelapor']
        merk_kendaraan = row['merk_kendaraan']
        jenis_merk_kendaraan = row['jenis_merk_kendaraan']
        no_pol = row['no_pol']
        no_ka = row['no_ka']
        no_sin = row['no_sin']
        teknik_kejahatan= row['teknik_kejahatan']
        desa_kelurahan = row['desa_kelurahan_tkp']


        # Dasar / LP
        if dasar == '' or dasar== ' ' or dasar is None:
            data_dasar_lp = None
        else:
            data_dasar_lp = dasar

        # Waktu Lapor
        if waktu_lapor == '' or waktu_lapor== ' ' or waktu_lapor is None:
            data_dasar_waktu = None
        else:
            data_dasar_waktu = waktu_lapor


        # Waktu Kejadian
        if waktu_kejadian == '' or waktu_kejadian == ' ' or waktu_kejadian is None:
            data_waktu_kejadian = None
        else:
            data_waktu_kejadian = waktu_kejadian


        # Detail Kendaraan
        if no_pol == '' or no_pol == ' ' or no_pol is None:
            data_no_pol = None
        else:
            data_no_pol = no_pol
        if no_ka == '' or no_ka == ' ' or no_ka is None:
            data_no_ka = None
        else:
            data_no_ka = no_ka
        if no_sin == '' or no_sin == ' ' or no_sin is None:
            data_no_sin = None
        else:
            data_no_sin = no_sin

        # Jenis dan Merk Kendaraan
        data_jenis = merk_kendaraan
        data_merk = jenis_merk_kendaraan
        if data_jenis == '' or data_jenis == ' ' or data_jenis is None:
            data_jenis = ''
        else:
            data_jenis = data_jenis

        if data_merk == '' or data_merk == ' ' or data_merk is None:
            data_merk = ''
        else:
            data_merk = data_merk
        data_jenis_merk = data_jenis,data_merk
        strDataJenisMerk = ' '.join(data_jenis_merk)


        if nama_pelapor == '' or nama_pelapor == ' ' or nama_pelapor is None:
            nama_pelapor = None
            id_pelapor = None
        else:
            numData.append(1)
            id_pelapor = str(len(numData))


        # Data Modus
        data_modus = teknik_kejahatan

        # Data Desa Kelurahan
        data_desa_kelurahan = desa_kelurahan




        # Data Jenis dan Merk Kendaraan
        checkJenisMerk = []
        with open('data-jenis-merk-unique.csv', 'r', encoding='utf-8') as fileJenisMerk:
            for lineJenisMerk in fileJenisMerk:
                clear_line_jenis_merk = lineJenisMerk.replace("\n", '').strip()
                id_jenis_merk, value_jenis_merk = clear_line_jenis_merk.split(',')
                value_jenis_merk = value_jenis_merk.strip()
                strDataJenisMerk = strDataJenisMerk.strip()
                if value_jenis_merk in [strDataJenisMerk]:
                    checkJenisMerk.append(id_jenis_merk)
        strIdJenisMerk = ' '.join(checkJenisMerk)
        if strIdJenisMerk == '' or strIdJenisMerk == ' ' or strIdJenisMerk is None:
            strIdJenisMerkFix = None
        else:
            strIdJenisMerkFix = strIdJenisMerk


        # Data Modus ID
        checkModus = []
        with open('data-modus-unique.csv', 'r', encoding='utf-8') as fileModus:
            for lineModus in fileModus:
                clear_line_modus = lineModus.replace("\n", '').strip()
                id_modus, value_modus = clear_line_modus.split(',')
                if value_modus in [data_modus]:
                    checkModus.append(id_modus)
        strIdModus = ' '.join(checkModus)
        if strIdModus == '':
            strIdModusFix = None
        else:
            strIdModusFix = strIdModus

        # Data Desa Kelurahan ID
        checkDesaKelurahan = []
        with open('data-desa-kelurahan-unique.csv', 'r', encoding='utf-8') as fileDesaKelurahan:
            for lineDesaKelurahan in fileDesaKelurahan:
                clear_line_desa_kelurahan = lineDesaKelurahan.replace("\n", '').strip()
                id_desa_kelurahan, value_desa_kelurahan = clear_line_desa_kelurahan.split(',')
                value_desa_kelurahan_1 = str(value_desa_kelurahan).strip()
                data_desa_kelurahan_1 = str(data_desa_kelurahan).strip()
                data_desa_kelurahan_final = data_desa_kelurahan_1.upper()
                if value_desa_kelurahan_1 in [data_desa_kelurahan_final]:
                    checkDesaKelurahan.append(id_desa_kelurahan)

        if not checkDesaKelurahan:
            strIdDesaKelurahan = None
        else:
            strIdDesaKelurahan = ' '.join(checkDesaKelurahan)

        # Check Nan Lat
        if math.isnan(lat_tkp):
            lat_tkp = None
        else:
            lat_tkp = lat_tkp

        # Check Nan Lon
        if math.isnan(lon_tkp):
            lon_tkp = None
        else:
            lon_tkp = lon_tkp


        sql = "INSERT INTO tb_data_curanmor (laporan_dasar,waktu_lapor,waktu_kejadian,tkp,lat_tkp,lon_tkp,no_pol,no_ka,no_sin,id_pelapor,id_merk_kendaraan,id_teknik_kejahatan,id_desa_kelurahan) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (data_dasar_lp, data_dasar_waktu, data_waktu_kejadian,tkp,lat_tkp,lon_tkp, data_no_pol, data_no_ka, data_no_sin, id_pelapor, strIdJenisMerkFix, strIdModusFix,strIdDesaKelurahan)
        print(val)
        cursor1.execute(sql, val)
        db1.commit()

def calculation_data():

    cursor1.execute("TRUNCATE TABLE tb_data_calculation")

    query = open("data_calculation_4.sql", "r")
    query = query.read()
    data = pd.read_sql(query, db1)

    for index, row in data.iterrows():
        dasar = row['dasar']
        waktu_lapor = row['waktu_lapor']
        waktu_kejadian = row['waktu_kejadian']
        bulan_tahun = row['bulan_tahun']
        tahun_category = row['tahun_category']
        jam_kejadian = row['jam_kejadian']
        rentang_jam_kejadian = row['rentang_jam_kejadian']
        tkp = row['tkp']
        lat_tkp = row['lat_tkp']
        lon_tkp = row['lon_tkp']
        nama_pelapor = row['nama_pelapor']
        umur_pelapor = row['umur_pelapor']
        gender_pelapor = row['gender_pelapor']
        alamat_pelapor = row['alamat_pelapor']
        merk_kendaraan = row['merk_kendaraan']
        jenis_merk_kendaraan = row['jenis_merk_kendaraan']
        no_pol = row['no_pol']
        no_ka = row['no_ka']
        no_sin = row['no_sin']
        teknik_kejahatan = row['teknik_kejahatan']
        desa_kelurahan = row['desa_kelurahan']
        kecamatan = row['kecamatan']
        angka_kejahatan = row['angka_kejahatan']
        jumlah_kejahatan = row['status_jumlah_kejahatan']

        if dasar is None:
            strDasar = None
        else:
            dasar = dasar.split()
            dasarFix = dasar[1:6]
            strDasar = ' '.join(dasarFix)


        # Check Jam Kejadian
        if math.isnan(jam_kejadian):
            jam_kejadian = None
        else:
            jam_kejadian = jam_kejadian

        # Check Umur Pelapor
        if math.isnan(umur_pelapor):
            umur_pelapor = None
        else:
            umur_pelapor = umur_pelapor

        # Check Nan Lat
        if math.isnan(lat_tkp):
            lat_tkp = None
        else:
            lat_tkp = lat_tkp

        # Check Nan Lon
        if math.isnan(lon_tkp):
            lon_tkp = None
        else:
            lon_tkp = lon_tkp

        # Check Angka Kejahatan
        if math.isnan(angka_kejahatan):
            angka_kejahatan = None
        else:
            angka_kejahatan = angka_kejahatan

        # Jumlah Kejahatan
        if jumlah_kejahatan == '' or jumlah_kejahatan == ' ' or jumlah_kejahatan is None:
            jumlah_kejahatan = None
        else:
            jumlah_kejahatan = jumlah_kejahatan


        sql = "INSERT INTO tb_data_calculation (dasar,waktu_lapor,waktu_kejadian,jam_kejadian,rentang_jam_kejadian,bulan_tahun,tahun_category,tkp,lat_tkp,lon_tkp,nama_pelapor,umur_pelapor,gender_pelapor,alamat_pelapor,merk_kendaraan,jenis_merk_kendaraan,no_pol,no_ka,no_sin,teknik_kejahatan,desa_kelurahan,kecamatan,angka_kejahatan, jumlah_kejahatan) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (strDasar,waktu_lapor,waktu_kejadian,jam_kejadian,rentang_jam_kejadian,bulan_tahun,tahun_category,tkp,lat_tkp,lon_tkp,nama_pelapor,umur_pelapor,gender_pelapor,alamat_pelapor,merk_kendaraan,jenis_merk_kendaraan,no_pol,no_ka,no_sin,teknik_kejahatan,desa_kelurahan,kecamatan,angka_kejahatan,jumlah_kejahatan)
        print(val)
        cursor1.execute(sql, val)
        db1.commit()


# Main
if __name__ == '__main__':
    print("Start")
    # load_data_kelurahan()
    # normalize_process()
    # data_store()
    calculation_data()
