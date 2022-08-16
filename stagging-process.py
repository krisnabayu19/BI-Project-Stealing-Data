from csv import DictReader
from collections import Counter
import re
import datetime
import pymysql
import psycopg2

# Koneksi ke Database MySQL
db = pymysql.connect(host='localhost', user='root', password='', charset='utf8', db='db_curanmor_data')
cursor = db.cursor()

def stagging_process():

    # Kode Menghapus Semua Data di Database, Sebelum di Masukkan Data yang Baru
    cursor.execute("TRUNCATE TABLE tb_stagging_data_csv")

    # Kode Meng-Import Data CSV, dengan Nama File 'data_curanmor.csv'
    with open('data_curanmor_2.csv', 'r', encoding='unicode_escape') as read_obj:
        csv_dict_reader = DictReader(read_obj)

        # Proses Perulangan Untuk Import Data dari CSV, yang dimana Ada Sebanyak 779 Row
        for row in csv_dict_reader:

            # Inisiasi Nama Field / Column
            id = row['id']
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

            # Cek Data, Jika Ada Data yang Kosong Outputnya Adalah 'None', Selain itu Data nya Tetap
            if dasar == '' or dasar == ' ' or dasar is None:
                dasar = None
            else:
                dasar = dasar

            # Cek Data 'Waktu Lapor', Jika Ada Data yang Kosong Outputnya Adalah 'None'
            if waktu_lapor == '' or waktu_lapor == ' ' or waktu_lapor is None :
                datetime_waktu_lapor = None
            else:
                # Penyamaan Format Datetime 'Waktu Lapor', Sebelum dimasukkan ke Database
                waktu_lapor= waktu_lapor.replace('/',' ').replace(':',' ')
                waktu_lapor = waktu_lapor.split()
                date = waktu_lapor[2],waktu_lapor[0],waktu_lapor[1]
                strDate = '-'.join(date)
                time = waktu_lapor[3],waktu_lapor[4]
                strTime = ':'.join(time)
                temp_waktu_lapor = strDate,strTime
                datetime_waktu_lapor = ' '.join(temp_waktu_lapor)

            # Cek Data 'Waktu Kejadian', Jika Ada Data yang Kosong Outputnya Adalah 'None'
            if waktu_kejadian == '' or waktu_kejadian == ' ' or waktu_kejadian is None :
                datetime_waktu_kejadian = None
            else:
                # Penyamaan Format Datetime 'Waktu Lapor', Sebelum dimasukkan ke Database
                waktu_kejadian= waktu_kejadian.replace('/',' ').replace(':',' ')
                waktu_kejadian = waktu_kejadian.split()
                date_1 = waktu_kejadian[2],waktu_kejadian[0],waktu_kejadian[1]
                strDate_1 = '-'.join(date_1)
                time_1 = waktu_kejadian[3],waktu_kejadian[4]
                strTime_1 = ':'.join(time_1)
                temp_waktu_kejadian = strDate_1,strTime_1
                datetime_waktu_kejadian = ' '.join(temp_waktu_kejadian)

            # Cek Data 'TKP', Jika Ada Data yang Kosong Outputnya Adalah 'None', Selain itu Data 'TKP' tetap
            if tkp=='' or tkp == ' ' or tkp is None:
                tkp = None
            else:
                tkp = tkp

            # Cek Data 'Nama Pelapor', Jika Ada Data yang Kosong Outputnya Adalah 'None', Selain itu Data 'Nama Pelapor' tetap
            if nama_pelapor == '' or nama_pelapor == ' ' or nama_pelapor is None:
                nama_pelapor = None
            else:
                nama_pelapor = nama_pelapor

            # Cek Data 'Umur Pelapor', Jika Ada Data yang Kosong Outputnya Adalah 'None', Selain itu Data 'Umur Pelapor' tetap
            if umur_pelapor == '' or umur_pelapor == ' ' or umur_pelapor is None:
                umur_pelapor = None
            else:
                umur_pelapor = umur_pelapor

            # Cek Data 'Gender Pelapor', Jika Ada Data yang Kosong Outputnya Adalah 'None', Selain itu Data 'Gender Pelapor' tetap
            if gender_pelapor == '' or gender_pelapor == ' ' or gender_pelapor is None:
                gender_pelapor = None
            else:
                gender_pelapor = gender_pelapor

            # Cek Data 'Alamat Pelapor', Jika Ada Data yang Kosong Outputnya Adalah 'None', Selain itu Data 'Alamat Pelapor' tetap
            if alamat_pelapor == '' or alamat_pelapor == ' ' or alamat_pelapor is None:
                alamat_pelapor = None
            else:
                alamat_pelapor = alamat_pelapor

            # Cek Data 'No. Pol', Jika Ada Data yang Kosong Outputnya Adalah 'None', Selain itu Data 'No. Pol' tetap
            if no_pol == '' or no_pol == ' ' or no_pol is None:
                no_pol = None
            else:
                no_pol = no_pol

            # Cek Data 'No. Ka', Jika Ada Data yang Kosong Outputnya Adalah 'None', Selain itu Data 'No. Ka' tetap
            if no_ka == '' or no_ka == ' ' or no_ka is None:
                no_ka = None
            else:
                no_ka = no_ka

            # Cek Data 'No. Sin', Jika Ada Data yang Kosong Outputnya Adalah 'None', Selain itu Data 'No. Sin' tetap
            if no_sin == '' or no_sin == ' ' or no_sin is None:
                no_sin = None
            else:
                no_sin = no_sin

            # Cek Data 'Merk. Kendaraan', Jika Ada Data yang Kosong Outputnya Adalah 'None', Selain itu Data 'Merk. Kendaraan' tetap
            if merk_kendaraan == '' or merk_kendaraan == ' ' or merk_kendaraan is None:
                merk_kendaraan = None
            else:
                merk_kendaraan = merk_kendaraan

            # Cek Data 'Jenis Kendaraan', Jika Ada Data yang Kosong Outputnya Adalah 'None', Selain itu Data 'Jenis Kendaraan' tetap
            if jenis_merk_kendaraan == '' or jenis_merk_kendaraan == ' ' or jenis_merk_kendaraan is None:
                jenis_merk_kendaraan = None
            else:
                jenis_merk_kendaraan = jenis_merk_kendaraan

            # Cek Data 'Teknik Kejahatan', Jika Ada Data yang Kosong Outputnya Adalah 'None', Selain itu Data 'Teknik Kejahatan' tetap
            if teknik_kejahatan == '' or teknik_kejahatan == ' ' or teknik_kejahatan is None:
                teknik_kejahatan = None
            else:
                teknik_kejahatan = teknik_kejahatan


            # Kode Menyiman Data ke Database MySQL
            sql = "INSERT INTO tb_stagging_data_csv (dasar,waktu_lapor,waktu_kejadian,tkp,nama_pelapor,umur_pelapor,gender_pelapor,alamat_pelapor,merk_kendaraan,jenis_merk_kendaraan,no_pol,no_ka,no_sin,teknik_kejahatan) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            val = (dasar,datetime_waktu_lapor,datetime_waktu_kejadian,tkp,nama_pelapor,umur_pelapor,gender_pelapor,alamat_pelapor,merk_kendaraan,jenis_merk_kendaraan,no_pol,no_ka,no_sin,teknik_kejahatan)
            print(val)
            cursor.execute(sql, val)
            db.commit()

# Main
if __name__ == '__main__':
    print("Start")
    stagging_process()
