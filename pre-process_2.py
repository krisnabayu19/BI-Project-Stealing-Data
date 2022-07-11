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

db1 = pymysql.connect(host='localhost', user='root', password='', charset='utf8', db='db_curanmor')
cursor1 = db1.cursor()

def mdy_to_ymd(d):
    return datetime.strptime(d, '%b %d,%Y %H:%M').strftime('%Y-%m-%d %H:%M')

def pre_data_dasar(dasar):
    if dasar == '':
        strDasar = None
        dateLaporanFinal = None
    else:
        # 1. Case Folding
        dasar = dasar.upper()

        # 2. Cleaning
        dasar = dasar.replace('/','').replace(',','').replace("'",'').replace('!','').replace('SEK','').replace('DENBAR','').replace('DENTIM','').replace('DENSEL','')
        dasar = dasar.split()

        # 3. Formating Data Month
        with open('month_dataset.csv', 'r', encoding='utf-8') as fileMonth:
            for lineMonth in fileMonth:
                clear_line_month = lineMonth.replace("\n", '').strip()
                datadefault, convert = clear_line_month.split(',')
                if datadefault in dasar:
                    dasar[-3] = convert
        strDasar = ' '.join(dasar)

        # 4. Pre-Processing Data Date Dasar Laporan
        dateLaporan = dasar[-3].title(),' ',dasar[-4], ',', dasar[-2], ' ', dasar[-1].replace('.',':')
        dateLaporanFinal = mdy_to_ymd(''.join(dateLaporan))

    return strDasar,dateLaporanFinal

def pre_data_waktu_kejadian(waktu_kejadian):
    if waktu_kejadian == '':
        dateKejadianFinal = None
    else:
        # 1. Upper Case
        waktu_kejadian = waktu_kejadian.upper()

        # 2. Cleaning Data
        waktu_kejadian = waktu_kejadian.replace('/','').replace(',','')
        waktu_kejadian = waktu_kejadian.split()

        # 3. Formating Data Month
        with open('month_dataset.csv', 'r', encoding='utf-8') as fileMonth:
            for lineMonth in fileMonth:
                clear_line_month = lineMonth.replace("\n", '').strip()
                datadefault, convert = clear_line_month.split(',')
                if datadefault in waktu_kejadian:
                    waktu_kejadian[-3] = convert
        dateKejadian = waktu_kejadian[-3].title(),' ', waktu_kejadian[-4],',',waktu_kejadian[-2], ' ', waktu_kejadian[-1].replace('.',':')
        dateKejadianFinal = mdy_to_ymd(''.join(dateKejadian))

    return dateKejadianFinal

def pre_data_tkp(tkp):
    if tkp== '':
        tkp = None
    else:
        tkp = tkp.upper()
        tkp = tkp.replace('/', '').replace(',', '')
        tkp = tkp.split()
        tkp = ' '.join(tkp)

    return tkp

def pre_data_pelapor(pelapor):

    # Nama Pelapor
    nama = pelapor.upper()
    nama = nama.replace(',', '').replace('.', '')
    nama = nama.split()
    arrayNama = []
    if 'LKS' in nama:
        nama = nama[0:nama.index('LKS')]
        nama = ' '.join(nama)
        arrayNama.append(nama)
    elif 'LK' in nama:
        nama = nama[0:nama.index('LK')]
        nama = ' '.join(nama)
        arrayNama.append(nama)
    elif 'PR' in nama:
        nama = nama[0:nama.index('PR')]
        nama = ' '.join(nama)
        arrayNama.append(nama)
    elif 'SDA' in nama:
        nama = nama[0:nama.index('SDA')]
        nama = ' '.join(nama)
        arrayNama.append(nama)
    elif 'JL' in nama:
        nama = nama[0:nama.index('JL')]
        nama = ' '.join(nama)
        arrayNama.append(nama)
    elif 'DSN' in nama:
        nama = nama[0:nama.index('DSN')]
        nama = ' '.join(nama)
        arrayNama.append(nama)
    elif 'BR' in nama:
        nama = nama[0:nama.index('BR')]
        nama = ' '.join(nama)
        arrayNama.append(nama)
    else:
        nama = '-'
        arrayNama.append(nama)
    strNama = ''.join(arrayNama)
    if strNama == '-':
        strNama = None
    else:
        strNama = strNama


    # Umur
    umur = pelapor.upper()
    umur = umur.replace(',','')
    umur = umur.split()
    arrayUmur = []
    if 'TH' in umur:
        umur = umur[umur.index('TH') - 1]
        arrayUmur.append(umur)
    else:
        umur = '-'
        arrayUmur.append(umur)
    strUmur = ''.join(arrayUmur)
    if strUmur == '-':
        strUmur = None
    else:
        strUmur = strUmur

    # Gender
    gender = pelapor.upper()
    gender = gender.replace(',','').replace('.','')
    gender = gender.split()
    arrayGen = []
    if 'LK' in gender:
        arrayGen.append('L')
    elif 'LKS' in gender:
        arrayGen.append('L')
    elif 'PR' in gender:
        arrayGen.append('P')
    else :
        arrayGen.append('-')
    strGender = ''.join(arrayGen)
    if strGender== '-':
        strGender = None
    else:
        strGender = strGender

    # Alamat
    alamat = pelapor.upper()
    alamat = alamat.replace(',', '').replace('.', '').replace('/', '')
    alamat = alamat.split()
    arrayAlamat = []
    if 'JL' in alamat:
        alamat = alamat[alamat.index('JL'):]
        if 'RP' in alamat:
            replaceStr = alamat[alamat.index('RP'):]
            replaceStr = ' '.join(replaceStr)
            alamat = ' '.join(alamat)
            alamat = alamat.replace(replaceStr, '')
            arrayAlamat.append(alamat)
        else:
            alamat = ' '.join(alamat)
            arrayAlamat.append(alamat)

    elif 'SDA' in alamat:
        alamat = alamat[alamat.index('SDA'):]
        if 'RP' in alamat:
            replaceStr = alamat[alamat.index('RP'):]
            replaceStr = ' '.join(replaceStr)
            alamat = ' '.join(alamat)
            alamat = alamat.replace(replaceStr, '')
            arrayAlamat.append(alamat)
        else:
            alamat = ' '.join(alamat)
            arrayAlamat.append(alamat)

    elif 'BR' in alamat:
        alamat = alamat[alamat.index('BR'):]
        if 'RP' in alamat:
            replaceStr = alamat[alamat.index('RP'):]
            replaceStr = ' '.join(replaceStr)
            alamat = ' '.join(alamat)
            alamat = alamat.replace(replaceStr, '')
            arrayAlamat.append(alamat)
        else:
            alamat = ' '.join(alamat)
            arrayAlamat.append(alamat)

    elif 'PERUM' in alamat:
        alamat = alamat[alamat.index('PERUM'):]
        if 'RP' in alamat:
            replaceStr = alamat[alamat.index('RP'):]
            replaceStr = ' '.join(replaceStr)
            alamat = ' '.join(alamat)
            alamat = alamat.replace(replaceStr, '')
            arrayAlamat.append(alamat)
        else:
            alamat = ' '.join(alamat)
            arrayAlamat.append(alamat)

    elif 'DSN' in alamat:
        alamat = alamat[alamat.index('DSN'):]
        if 'RP' in alamat:
            replaceStr = alamat[alamat.index('RP'):]
            replaceStr = ' '.join(replaceStr)
            alamat = ' '.join(alamat)
            alamat = alamat.replace(replaceStr, '')
            arrayAlamat.append(alamat)
        else:
            alamat = ' '.join(alamat)
            arrayAlamat.append(alamat)
    else:
        alamat = '-'
        arrayAlamat.append(alamat)
    strAlamat = ' '.join(arrayAlamat)

    if strAlamat == '-':
        strAlamat = None
    else:
        strAlamat = strAlamat

    return strNama, strUmur,strGender, strAlamat

def pre_data_jenis_kendaraan(jenis):

    arrColor = []
    with open('color_dictionary.csv', 'r', encoding='utf-8') as fileColor:
        for i in fileColor:
            clear_line_color = i.replace("\n", '').strip()
            arrColor.append(clear_line_color)


    if jenis == '':
        strJenis = None
    else:
        jenis = jenis.upper()
        jenis = jenis.replace('/', '').replace(',', '').replace('.','').replace('TH','')
        jenis = jenis.split()
        strJenis = ' '.join(jenis)

        merk = jenis[0]
        merkStr = ' '.join(jenis)


        if np.char.isnumeric(jenis[-1]):
            tahun = jenis[-1]
            jenisMerk = merkStr.replace(tahun,'').replace(merk,'')
            jenisMerk = jenisMerk.split()
            strJenisMerk = ' '.join(jenisMerk)
            if strJenisMerk == '' or strJenisMerk == ' ' or strJenisMerk is None:
                strJenisMerkFix = None
            else:
                strJenisMerkPre = pre_jenis_merk(strJenisMerk)
                strJenisMerkFix = strJenisMerkPre

        else:
            tahun = None
            jenisMerk = merkStr.replace(merk,'')
            jenisMerk = jenisMerk.split()
            strJenisMerk = ' '.join(jenisMerk)
            if strJenisMerk == '' or strJenisMerk == ' ' or strJenisMerk is None:
                strJenisMerkFix = None
            else:
                strJenisMerkPre = pre_jenis_merk(strJenisMerk)
                strJenisMerkFix = strJenisMerkPre


    return strJenis,merk,strJenisMerkFix,tahun

def pre_jenis_merk (jenis_merk_kendaraan):

    arrColor = []
    with open('color_dictionary.csv', 'r', encoding='utf-8') as fileColor:
        for i in fileColor:
            clear_line_color = i.replace("\n", '').strip()
            arrColor.append(clear_line_color)

    arrJenisK = []
    if jenis_merk_kendaraan is None:
        jenis_merk_kendaraan = None
    else:
        jenis_merk_kendaraan = jenis_merk_kendaraan.split()
        for i in jenis_merk_kendaraan:
            if i not in arrColor:
                arrJenisK.append(i)

    jenisMK = ' '.join(arrJenisK)
    if jenisMK == ' ':
        strJenisMK = None
    else:
        strJenisMK = jenisMK

    return strJenisMK

def pre_data_no_pol(no_pol):
    if no_pol == '':
        no_pol = None
    else:
        no_pol = no_pol.upper()
        no_pol = no_pol.replace('/','').replace(',','').replace(',','')
        no_pol = no_pol.split()
        no_pol = ' '.join(no_pol)
    return no_pol

def pre_data_no_ka(no_ka):
    if no_ka == '':
        no_ka = None
    else:
        no_ka = no_ka.upper()
        no_ka = no_ka.replace('/', '').replace(',', '').replace(',', '')
        no_ka = no_ka.split()
        no_ka = ''.join(no_ka)
    return no_ka

def pre_data_no_sin(no_sin):
    if no_sin == '':
        no_sin = None
    else:
        no_sin = no_sin.upper()
        no_sin = no_sin.replace('/', '').replace(',', '').replace(',', '')
        no_sin = no_sin.split()
        no_sin = ''.join(no_sin)
    return no_sin

def pre_data_modus(modus):
    if modus == '':
        modus = None
    else:
        modus = modus.upper()
        modus = modus.split()
        modus = ' '.join(modus)
    return modus

def pre_data_kecamatan(kecamatan):
    if kecamatan == '':
        kecamatan = None
    else:
        kecamatan = kecamatan.upper()
    return kecamatan

def lat_lon(tkp):
    gmaps_key = googlemaps.Client(key='AIzaSyBiM-okLvji2OxJLqfrAaIfSofmwmq8scM')
    geocode_result = gmaps_key.geocode(tkp)
    try:
        lat = geocode_result[0]["geometry"]["location"]["lat"]
        lon = geocode_result[0]["geometry"]["location"]["lng"]
    except:
        lat = None
        lon = None

    return lat, lon

def normalize_process():

    cursor1.execute("TRUNCATE TABLE tb_laporan_dasar")
    cursor1.execute("TRUNCATE TABLE tb_waktu_lapor")
    cursor1.execute("TRUNCATE TABLE tb_waktu_kejadian")
    cursor1.execute("TRUNCATE TABLE tb_pelapor")
    cursor1.execute("TRUNCATE TABLE tb_detail_merk_kendaraan")
    cursor1.execute("TRUNCATE TABLE tb_merk_kendaraan")
    cursor1.execute("TRUNCATE TABLE tb_jenis_merk_kendaraan")
    cursor1.execute("TRUNCATE TABLE tb_teknik_kejahatan")
    cursor1.execute("TRUNCATE TABLE tb_kecamatan")

    query = "SELECT * FROM tb_stagging_data_csv ;"
    data = pd.read_sql(query, db1)

    # Waktu Lapor
    dataUniqueWLapor = []
    dataNumWLapor = 0
    dataNumArrayWLapor = []

    # Waktu Kejadian
    dataUniqueWKejadian = []
    dataNumWKejadian = 0
    dataNumArrayWKejadian = []

    # TKP
    dataUniqueTkp = []
    dataNumTkp = 0
    dataNumArrayTkp = []

    # Jenis Kendaraan
    dataUniqueJenis = []
    dataNumJenis = 0
    dataNumArrayJenis = []

    # Merk Kendaraan
    dataUniqueMerk = []
    dataNumMerk = 0
    dataNumArrayMerk = []

    # Tahun Kendaraan
    dataUniqueTahun = []
    dataNumTahun = 0
    dataNumArrayTahun = []

    # Modus
    dataUniqueModus = []
    dataNumModus = 0
    dataNumArrayMod = []

    # Kecamatan
    dataUniqueKecamatan = []
    dataNumKecamatan = 0
    dataNumArrayKec = []


    for index, row in data.iterrows():
        dasar = row['dasar']
        waktu_kejadian = row['waktu_kejadian']
        pelapor = row['pelapor']
        jenis = row['jenis']
        modus = row['modus']
        kecamatan = row['kecamatan']

        # Dasar / LP
        data_dasar = pre_data_dasar(dasar)
        data_dasar_lp = data_dasar[0]
        data_dasar_waktu = data_dasar[1]

        # Waktu Lapor
        if data_dasar_waktu not in dataUniqueWLapor and data_dasar_waktu!= ' ':
            dataUniqueWLapor.append(data_dasar_waktu)
            dataNumWLapor = dataNumWLapor + 1
            dataNumArrayWLapor.append(dataNumWLapor)
            sql = "INSERT INTO tb_waktu_lapor (waktu_lapor) VALUES (%s)"
            val = (data_dasar_waktu)
            cursor1.execute(sql, val)
            db1.commit()
        else:
            print('Data Available')

        # Waktu Kejadian
        data_waktu_kejadian = pre_data_waktu_kejadian(waktu_kejadian)
        if data_waktu_kejadian not in dataUniqueWKejadian and data_waktu_kejadian != ' ':
            dataUniqueWKejadian.append(data_waktu_kejadian)
            dataNumWKejadian = dataNumWKejadian + 1
            dataNumArrayWKejadian.append(dataNumWKejadian)
            sql = "INSERT INTO tb_waktu_kejadian (waktu_kejadian) VALUES (%s)"
            val = (data_waktu_kejadian)
            cursor1.execute(sql, val)
            db1.commit()
        else:
            print('Data Available')

        # Pelapor
        data_pelapor = pre_data_pelapor(pelapor)
        data_nama_pelapor = data_pelapor[0]
        data_umur_pelapor = data_pelapor[1]
        data_gender_pelapor = data_pelapor[2]
        data_alamat_pelapor = data_pelapor[3]
        sql = "INSERT INTO tb_pelapor (nama_pelapor,umur_pelapor,gender_pelapor,alamat_pelapor) VALUES (%s,%s,%s,%s)"
        val = (data_nama_pelapor,data_umur_pelapor,data_gender_pelapor,data_alamat_pelapor)
        # cursor1.execute(sql, val)
        # db1.commit()


        # Jenis Kendaraan
        data_jenis_kendaraan = pre_data_jenis_kendaraan(jenis)
        data_jenis = data_jenis_kendaraan[1]
        if data_jenis not in dataUniqueJenis and data_jenis != '':
            dataUniqueJenis.append(data_jenis)
            dataNumJenis = dataNumJenis+1
            dataNumArrayJenis.append(dataNumJenis)
            sql = "INSERT INTO tb_merk_kendaraan (merk_kendaraan) VALUES (%s)"
            val = (data_jenis)
            # cursor1.execute(sql, val)
            # db1.commit()
        else:
            print('Data Available')

        # Merk Kendaraan
        data_jenis_kendaraan = pre_data_jenis_kendaraan(jenis)
        data_merk = data_jenis_kendaraan[2]
        if data_merk not in dataUniqueMerk and data_merk is not None and data_merk != '':
            dataUniqueMerk.append(data_merk)
            dataNumMerk = dataNumMerk + 1
            dataNumArrayMerk.append(dataNumMerk)
            sql = "INSERT INTO tb_jenis_merk_kendaraan (jenis_merk_kendaraan) VALUES (%s)"
            val = (data_merk)
            # cursor1.execute(sql, val)
            # db1.commit()
        else:
            print('Data Available')

        # Modus
        data_modus = pre_data_modus(modus)
        if data_modus not in dataUniqueModus and data_modus != ' ':
            dataUniqueModus.append(data_modus)
            dataNumModus = dataNumModus + 1
            dataNumArrayMod.append(dataNumModus)
            sql = "INSERT INTO tb_teknik_kejahatan (teknik_kejahatan) VALUES (%s)"
            val = (data_modus)
            # cursor1.execute(sql, val)
            # db1.commit()
        else:
            print('Data Available')

        # Kecamatan
        data_kecamatan = pre_data_kecamatan(kecamatan)
        if data_kecamatan not in dataUniqueKecamatan and data_kecamatan != ' ':
            dataUniqueKecamatan.append(data_kecamatan)
            dataNumKecamatan = dataNumKecamatan+1
            dataNumArrayKec.append(dataNumKecamatan)
            sql = "INSERT INTO tb_kecamatan (kecamatan) VALUES (%s)"
            val = (data_kecamatan)
            # cursor1.execute(sql, val)
            # db1.commit()
        else:
            print('Data Available')
        print('\n')

    with open('data-waktu-lapor-unique.csv', 'w', encoding='UTF8',newline='') as f:
        for id_waktu_lapor, value_waktu_lapor in zip(dataNumArrayWLapor,dataUniqueWLapor):
            strWaktuLapor = [id_waktu_lapor,value_waktu_lapor]
            writer = csv.writer(f)
            writer.writerow(strWaktuLapor)

    with open('data-waktu-kejadian-unique.csv', 'w', encoding='UTF8',newline='') as f:
        for id_waktu_kejadian, value_waktu_kejadian in zip(dataNumArrayWKejadian,dataUniqueWKejadian):
            strWaktuKejadian = [id_waktu_kejadian,value_waktu_kejadian]
            writer = csv.writer(f)
            writer.writerow(strWaktuKejadian)

    with open('data-tkp-unique.csv', 'w', encoding='UTF8',newline='') as f:
        for id_tkp, value_tkp in zip(dataNumArrayTkp,dataUniqueTkp):
            strTkp = [id_tkp,value_tkp]
            writer = csv.writer(f)
            writer.writerow(strTkp)

    with open('data-jenis-unique.csv', 'w', encoding='UTF8', newline='') as f:
        for id_jenis, value_jenis in zip(dataNumArrayJenis, dataUniqueJenis):
            strJenis = [id_jenis, value_jenis]
            writer = csv.writer(f)
            writer.writerow(strJenis)

    with open('data-merk-unique.csv', 'w', encoding='UTF8', newline='') as f:
        for id_merk, value_merk in zip(dataNumArrayMerk, dataUniqueMerk):
            strMerk = [id_merk, value_merk]
            writer = csv.writer(f)
            writer.writerow(strMerk)

    with open('data-tahun-unique.csv', 'w', encoding='UTF8', newline='') as f:
        for id_tahun, value_tahun in zip(dataNumArrayTahun, dataUniqueTahun):
            strTahun = [id_tahun, value_tahun]
            writer = csv.writer(f)
            writer.writerow(strTahun)

    with open('data-modus-unique.csv', 'w', encoding='UTF8', newline='') as f:
        for id_mod, value_mod in zip(dataNumArrayMod, dataUniqueModus):
            strModus = [id_mod, value_mod]
            writer = csv.writer(f)
            writer.writerow(strModus)

    with open('data-kecamatan-unique.csv', 'w', encoding='UTF8', newline='') as f:
        for id_kec, value_kec in zip(dataNumArrayKec, dataUniqueKecamatan):
            strKecamatan = [id_kec, value_kec]
            writer = csv.writer(f)
            writer.writerow(strKecamatan)

def data_store_detail_merk():
    cursor1.execute("TRUNCATE TABLE tb_detail_merk_kendaraan")

    query = "SELECT * FROM tb_stagging_data_csv ;"
    data = pd.read_sql(query, db1)

    numData = []

    for index, row in data.iterrows():
        jenis = row['jenis']

        # Jenis Kendaraan
        data_jenis_kendaraan = pre_data_jenis_kendaraan(jenis)
        data_jenis = data_jenis_kendaraan[1]
        checkJenis = []
        with open('data-jenis-unique.csv', 'r', encoding='utf-8') as fileJenis:
            for lineJenis in fileJenis:
                clear_line_jenis = lineJenis.replace("\n", '').strip()
                id_jenis, value_jenis = clear_line_jenis.split(',')
                if value_jenis in [data_jenis]:
                    checkJenis.append(id_jenis)
        strIdJenis = ' '.join(checkJenis)
        if strIdJenis == '':
            strIdJenisFix = None
        else:
            strIdJenisFix = strIdJenis


        # Merk Kendaraan
        data_jenis_kendaraan = pre_data_jenis_kendaraan(jenis)
        data_merk = data_jenis_kendaraan[2]
        checkJenisMerk = []
        with open('data-merk-unique.csv', 'r', encoding='utf-8') as fileJenisMerk:
            for lineJenisMerk in fileJenisMerk:
                clear_line_jenis_merk = lineJenisMerk.replace("\n", '').strip()
                id_jenis_merk, value_jenis_merk = clear_line_jenis_merk.split(',')
                if value_jenis_merk in [data_merk]:
                    checkJenisMerk.append(id_jenis_merk)
        strIdJenisMerk = ' '.join(checkJenisMerk)
        if strIdJenisMerk == '':
            strIdJenisMerkFix = None
        else:
            strIdJenisMerkFix = strIdJenisMerk

        sql = "INSERT INTO tb_detail_merk_kendaraan(id_merk_kendaraan,id_jenis_merk_kendaraan) VALUES (%s,%s)"
        val = (strIdJenisFix,strIdJenisMerkFix)
        print(val)
        # cursor1.execute(sql, val)
        # db1.commit()

def data_store():
    cursor1.execute("TRUNCATE TABLE tb_data_curanmor")

    query = "SELECT * FROM tb_stagging_data_csv ;"
    data = pd.read_sql(query, db1)

    numData = []

    for index, row in data.iterrows():
        id = row['id']
        dasar = row['dasar']
        waktu_kejadian = row['waktu_kejadian']
        tkp = row['tkp']
        pelapor = row['pelapor']
        jenis = row['jenis']
        no_pol = row['no_pol']
        no_ka = row['no_ka']
        no_sin = row['no_sin']
        modus = row['modus']
        kecamatan = row['kecamatan']

        # Dasar / LP
        data_dasar = pre_data_dasar(dasar)
        data_dasar_lp = data_dasar[0]
        data_dasar_waktu = data_dasar[1]

        # Waktu Kejadian
        data_waktu_kejadian = pre_data_waktu_kejadian(waktu_kejadian)

        # TKP
        data_tkp = pre_data_tkp(tkp)
        data_tkp_coordinate = lat_lon(data_tkp)
        data_tkp_coordinate_lat = data_tkp_coordinate[0]
        data_tkp_coordinate_lon = data_tkp_coordinate[1]

        # Detail Kendaraan
        data_no_pol = pre_data_no_pol(no_pol)
        data_no_ka = pre_data_no_ka(no_ka)
        data_no_sin = pre_data_no_sin(no_sin)

        # Jenis Kendaraan
        data_jenis_kendaraan = pre_data_jenis_kendaraan(jenis)
        data_jenis = data_jenis_kendaraan[1]
        data_merk = data_jenis_kendaraan[2]

        # Data Modus
        data_modus = pre_data_modus(modus)

        # Data Kecamatan
        data_kecamatan = pre_data_kecamatan(kecamatan)

        # Data Pelapor dan Detail Merk Kendaraan
        numData.append(1)
        id_data = str(len(numData))

        # Data Waktu Lapor
        checkWaktuLapor = []
        with open('data-waktu-lapor-unique.csv', 'r', encoding='utf-8') as fileWaktuLapor:
            for lineWaktuLapor in fileWaktuLapor:
                clear_line_waktu_lapor = lineWaktuLapor.replace("\n", '').strip()
                id_waktu_lapor, value_waktu_lapor = clear_line_waktu_lapor.split(',')
                if value_waktu_lapor in [data_dasar_waktu]:
                    checkWaktuLapor.append(id_waktu_lapor)
        strIdWaktuLapor = ' '.join(checkWaktuLapor)
        if strIdWaktuLapor == '':
            strIdWaktuLaporFix = None
        else:
            strIdWaktuLaporFix = strIdWaktuLapor

        # Data Waktu Kejadian
        checkWaktuKejadian = []
        with open('data-waktu-kejadian-unique.csv', 'r', encoding='utf-8') as fileWaktuKejadian:
            for lineWaktuKejadian in fileWaktuKejadian:
                clear_line_waktu_kejadian = lineWaktuKejadian.replace("\n", '').strip()
                id_waktu_kejadian, value_waktu_kejadian = clear_line_waktu_kejadian.split(',')
                if value_waktu_kejadian in [data_waktu_kejadian]:
                    checkWaktuKejadian.append(id_waktu_kejadian)
        strIdWaktuKejadian = ' '.join(checkWaktuKejadian)
        if strIdWaktuKejadian == '':
            strIdWaktuKejadianFix = None
        else:
            strIdWaktuKejadianFix = strIdWaktuKejadian

        # Data Jenis Kendaraan
        checkJenis = []
        with open('data-jenis-unique.csv', 'r', encoding='utf-8') as fileJenis:
            for lineJenis in fileJenis:
                clear_line_jenis = lineJenis.replace("\n", '').strip()
                id_jenis, value_jenis = clear_line_jenis.split(',')
                if value_jenis in [data_jenis]:
                    checkJenis.append(id_jenis)
        strIdJenis = ' '.join(checkJenis)
        if strIdJenis == '':
            strIdJenisFix = None
        else:
            strIdJenisFix = strIdJenis

        # Data Jenis Merk Kendaraan
        checkJenisMerk = []
        with open('data-merk-unique.csv', 'r', encoding='utf-8') as fileJenisMerk:
            for lineJenisMerk in fileJenisMerk:
                clear_line_jenis_merk = lineJenisMerk.replace("\n", '').strip()
                id_jenis_merk, value_jenis_merk = clear_line_jenis_merk.split(',')
                if value_jenis_merk in [data_merk]:
                    checkJenisMerk.append(id_jenis_merk)
        strIdJenisMerk = ' '.join(checkJenisMerk)
        if strIdJenisMerk == '':
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

        # Data Kecamatan ID
        checkKecamatan = []
        with open('data-kecamatan-unique.csv', 'r', encoding='utf-8') as fileKecamatan:
            for lineKecamatan in fileKecamatan:
                clear_line_kecamatan = lineKecamatan.replace("\n", '').strip()
                id_kecamatan, value_kecamatan = clear_line_kecamatan.split(',')
                if value_kecamatan in [data_kecamatan]:
                    checkKecamatan.append(id_kecamatan)
        strIdKecamatan = ' '.join(checkKecamatan)
        if strIdKecamatan == '':
            strIdKecamatanFix = None
        else:
            strIdKecamatanFix = strIdKecamatan

        sql = "INSERT INTO tb_data_curanmor (laporan_dasar,id_waktu_lapor,id_waktu_kejadian,tkp,lat_tkp,lon_tkp,no_pol,no_ka,no_sin,id_pelapor,id_detail_merk_kendaraan,id_teknik_kejahatan,id_kecamatan) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (data_dasar_lp, strIdWaktuLaporFix, strIdWaktuKejadianFix, data_tkp, data_tkp_coordinate_lat, data_tkp_coordinate_lon, data_no_pol, data_no_ka, data_no_sin, id_data, id_data, strIdModusFix,strIdKecamatanFix)
        print(val)
        # cursor1.execute(sql, val)
        # db1.commit()

        print('\n')

def calculation_data():

    cursor1.execute("TRUNCATE TABLE tb_data_calculation")

    query = open("data_calculation_2.sql", "r")
    query = query.read()
    data = pd.read_sql(query, db1)

    for index, row in data.iterrows():
        dasar = row['dasar']
        waktu_lapor = row['waktu_lapor']
        waktu_kejadian = row['waktu_kejadian']
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
        kecamatan = row['kecamatan']
        jumlah_kejahatan = row['jumlah_kejahatan']
        status_jumlah_kejahatan = row['status_jumlah_kejahatan']

        dasar = dasar.split()
        dasarFix = dasar[1:6]
        strDasar = ' '.join(dasarFix)

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

        # Check Jumlah Kejahatan
        if math.isnan(jumlah_kejahatan):
            jumlah_kejahatan = None
        else:
            jumlah_kejahatan = jumlah_kejahatan

        sql = "INSERT INTO tb_data_calculation (dasar,waktu_lapor,waktu_kejadian,rentang_jam_kejadian,tkp,lat_tkp,lon_tkp,nama_pelapor,umur_pelapor,gender_pelapor,alamat_pelapor,merk_kendaraan,jenis_merk_kendaraan,no_pol,no_ka,no_sin,teknik_kejahatan,kecamatan,jumlah_kejahatan,status_jumlah_kejahatan) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (strDasar,waktu_lapor,waktu_kejadian,rentang_jam_kejadian,tkp,lat_tkp,lon_tkp,nama_pelapor,umur_pelapor,gender_pelapor,alamat_pelapor,merk_kendaraan,jenis_merk_kendaraan,no_pol,no_ka,no_sin,teknik_kejahatan,kecamatan,jumlah_kejahatan,status_jumlah_kejahatan)
        print(val)
        # cursor1.execute(sql, val)
        # db1.commit()


# Main
if __name__ == '__main__':
    print("Start")
    normalize_process()
    data_store_detail_merk()
    data_store()
    calculation_data()
