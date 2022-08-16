import time
import googlemaps  # pip install googlemaps
import pandas as pd  # pip install pandas
import pymysql
import math


def miles_to_meters(miles):
    try:
        return miles * 1_609.344
    except:
        return 0


API_KEY = ''

def place_nearby(lat,lng,id):
    map_client = googlemaps.Client(API_KEY)

    # address = 'Jl. Kamboja No. 9 Denpasar'
    # geocode = map_client.geocode(address=address)
    # (lat, lng) = map(geocode[0]['geometry']['location'].get, ('lat', 'lng'))
    # print(lat,lng)

    search_string = 'alfamart'
    distance = miles_to_meters(0.7)
    business_list = []

    response = map_client.places_nearby(
        location=(lat, lng),
        keyword=search_string,
        radius=distance
    )

    business_list.extend(response.get('results'))
    next_page_token = response.get('next_page_token')

    while next_page_token:
        time.sleep(2)
        response = map_client.places_nearby(
            location=(lat, lng),
            keyword=search_string,
            radius=distance,
            page_token=next_page_token
        )
        business_list.extend(response.get('results'))
        next_page_token = response.get('next_page_token')

    df = pd.DataFrame(business_list)
    # df['url'] = 'https://www.google.com/maps/place/?q=place_id:' + df['place_id']


    core_latitude = lat
    core_longitude = lng
    core_id = id
    if df.empty:
        print('None')
    else:
        name = df['name']
        business_status = df['business_status']
        location = df['geometry']
        print(location)
        # for j in location :
        for i, k, j in zip(name, business_status,location):
            value = list(j.items())
            valueLocation = value[0]
            strLocation = str(valueLocation).replace('(','').replace(')','').replace(':','').replace("'",'').replace('{','').replace(',','').replace('}','').replace('location','').replace('lat','').replace('lng','')
            strLocation_1 = strLocation.split()
            strName = i
            strBusinessStatus = k
            latitude = strLocation_1[0]
            longitude = strLocation_1[1]
            status = 'Titik Lokasi Berdampak'
            sql = "INSERT INTO tb_data_place_nearby_alfamart (name,business_status,latitude,longitude,status,core_latitude,core_longitude,core_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
            val = (strName,strBusinessStatus,latitude, longitude, status,core_latitude,core_longitude,core_id)
            cursor1.execute(sql, val)
            db1.commit()


# Main
if __name__ == '__main__':
    print("Start")

    db1 = pymysql.connect(host='localhost', user='root', password='', charset='utf8', db='db_curanmor_data')
    cursor1 = db1.cursor()


    query = "SELECT * FROM tb_data_curanmor ;"
    data = pd.read_sql(query, db1)

    cursor1.execute("TRUNCATE TABLE tb_data_place_nearby_alfamart")

    for index, row in data.iterrows():
        id = row['id']
        lat_tkp = row['lat_tkp']
        lon_tkp = row['lon_tkp']
        id_desa_kelurahan = row['id_desa_kelurahan']

        if math.isnan(lat_tkp) or math.isnan(lon_tkp) or math.isnan(id_desa_kelurahan):
            print('None Data')
        else:
            print(id,lat_tkp,lon_tkp)
            strName = 'Titik Lokasi Curanmor'
            strBusinessStatus = 'Titik Lokasi Curanmor'
            latitude = lat_tkp
            longitude = lon_tkp
            status = 'Titik Lokasi Curanmor'
            core_latitude = lat_tkp
            core_longitude = lon_tkp
            sql = "INSERT INTO tb_data_place_nearby_alfamart (name,business_status,latitude,longitude,status,core_latitude,core_longitude,core_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
            val = (strName,strBusinessStatus,latitude, longitude,status,core_latitude,core_longitude,id)
            cursor1.execute(sql, val)
            db1.commit()
            dataPlaceNearby = place_nearby(lat_tkp,lon_tkp,id)
