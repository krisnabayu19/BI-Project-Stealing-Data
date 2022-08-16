import coordinates as coordinates
import pymysql
import pandas as pd
import googlemaps
import numpy as np
import csv
import math
from geopy.geocoders import Nominatim
import time
import pandas as pd
from geopy.geocoders import GoogleV3
import geopy.distance
import googlemaps

# API Google Maps API
API = ''

# Fungsi Merubah Alamat Menjadi Latitude dan Longitude Menggunakan Google Maps API
def lat_lon(tkp):
    gmaps_key = googlemaps.Client(key=API)
    geocode_result = gmaps_key.geocode(tkp)
    try:
        lat = geocode_result[0]["geometry"]["location"]["lat"]
        lon = geocode_result[0]["geometry"]["location"]["lng"]
    except:
        lat = None
        lon = None

    return lat, lon

# Fungsi Merubah Latitude dan Longitude Menjadi GeoLocation Menggunakan Google Maps API, Agar Mendapatkan Data Desa, Kelurahan, dan Kecamatan
def geo_location(lat,lon):
    geolocator = GoogleV3(api_key=API)

    if lat is None or lon is None:
        strLocation_pre = None
        strLocationDesKel = None
        strLocationKec = None
    else:
        location = geolocator.reverse(f'{lat}, {lon}')
        strLocation_pre = str(location)
        strLocation = strLocation_pre.split(', ')
        arrayLocationDesKel = []
        arrayLocationKec = []
        if 'Indonesia' in strLocation:
            strLocation1 = strLocation[strLocation.index('Indonesia') - 4]
            strLocation2 = strLocation[strLocation.index('Indonesia') - 3]
            arrayLocationDesKel.append(strLocation1)
            arrayLocationKec.append(strLocation2)
        else:
            strLocation = None

        if not arrayLocationDesKel:
            strLocationDesKel = None
        else:
            strLocationDesKel = ' '.join(arrayLocationDesKel)

        if not arrayLocationKec:
            strLocationKec = None
        else:
            strLocationKec = ' '.join(arrayLocationKec)

    return strLocation_pre,strLocationDesKel,strLocationKec

# Cek Data 'TKP', Jika Ada Data yang Kosong Outputnya Adalah 'None', Selain itu Data 'TKP' di Upper Case, dan di Hilangkan Tanda '/', dan ',''
def pre_data_tkp(tkp):
    if tkp== '' or tkp==' ' or tkp is None:
        tkp = None
    else:
        tkp = tkp.upper()
        tkp = tkp.replace('/', '').replace(',', '')
        tkp = tkp.split()
        tkp = ' '.join(tkp)

    return tkp



if __name__ == '__main__':

    # Koneksi ke Database MySQL
    db1 = pymysql.connect(host='localhost', user='root', password='', charset='utf8', db='db_curanmor_data')
    cursor1 = db1.cursor()

    # Kode Menghapus Semua Data di Database, Sebeleum di Masukkan Data yang Baru
    cursor1.execute("TRUNCATE TABLE tb_stagging_geo_location")

    # Query Untuk Menampilkan Semua Data pada tb_stagging_data_csv
    query = "SELECT * FROM tb_stagging_data_csv ;"
    data = pd.read_sql(query, db1)

    # Proses Perulangan Untuk Import Data dari CSV, yang dimana Ada Sebanyak 779 Row
    for index, row in data.iterrows():
        dasar = row['dasar']
        waktu_lapor = row['waktu_lapor']
        waktu_kejadian = row['waktu_kejadian']
        tkp = row['tkp']
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

        # Cek Data 'TKP', Jika Ada Data yang Kosong Outputnya Adalah 'None', Selain Itu Lanjut ke Proses Converting Latitude & Longitude Serta Geolocation
        if tkp == '' or tkp == ' ' or tkp is None:
            data_tkp_coordinate_lat = None
            data_tkp_coordinate_lon = None
            data_geo_location_full = None
            data_desa_kelurahan = None
            data_kecamatan = None
        else:
            data_tkp = pre_data_tkp(tkp)
            data_tkp_coordinate = lat_lon(data_tkp)
            data_tkp_coordinate_lat = data_tkp_coordinate[0]
            data_tkp_coordinate_lon = data_tkp_coordinate[1]
            data_geo_location = geo_location(data_tkp_coordinate_lat,data_tkp_coordinate_lon)
            data_geo_location_full = data_geo_location[0]
            data_desa_kelurahan = data_geo_location[1]
            data_kecamatan = data_geo_location[2]

        # CCek Data 'TKP', Jika Ada Data yang Kosong Outputnya Adalah 'None', Selain Itu Data Umur Tetap
        if math.isnan(umur_pelapor):
            umur_pelapor = None
        else:
            umur_pelapor = umur_pelapor

        # Kode Menyiman Data ke Database MySQL
        sql = "INSERT INTO tb_stagging_geo_location (dasar,waktu_lapor,waktu_kejadian,tkp,lat_tkp,lon_tkp,geo_location_full,desa_kelurahan_tkp,kecamatan_tkp,nama_pelapor,umur_pelapor,gender_pelapor,alamat_pelapor,merk_kendaraan,jenis_merk_kendaraan,no_pol,no_ka,no_sin,teknik_kejahatan) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (dasar,waktu_lapor,waktu_kejadian,tkp,data_tkp_coordinate_lat,data_tkp_coordinate_lon,data_geo_location_full,data_desa_kelurahan,data_kecamatan,nama_pelapor,umur_pelapor,gender_pelapor,alamat_pelapor,merk_kendaraan,jenis_merk_kendaraan,no_pol,no_ka,no_sin,teknik_kejahatan)
        print(val)
        cursor1.execute(sql, val)
        db1.commit()
