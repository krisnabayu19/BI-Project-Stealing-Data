select
	tdc.id as core_id
	,tdc.lat_tkp as latitude
	,tdc.lon_tkp as longitude
	,tdc.desa_kelurahan
	,tdc.kecamatan
	,tdc.angka_kejahatan
	,tdpnr2.jumlah_terdampak as jumlah_toko_terdampak
	,tdpnr3.jumlah_terdampak as jumlah_pasar_terdampak
	,tdpnr4.jumlah_terdampak as jumlah_rumahsakit_terdampak
	,tdpnr5.jumlah_terdampak as jumlah_swalayan_terdampak
from db_curanmor_data.tb_data_calculation tdc
left join(
	select
		tdpnr.core_id
		,kategori
		,count(distinct id) as jumlah_terdampak
	from db_curanmor_data.tb_data_place_nearby_rekap tdpnr
	where tdpnr.status != 'Titik Lokasi Curanmor'
		and tdpnr.kategori = 'Toko'
	group by 1,2
)tdpnr2 on tdc.id = tdpnr2.core_id
left join(
	select
		tdpnr.core_id
		,kategori
		,count(distinct id) as jumlah_terdampak
	from db_curanmor_data.tb_data_place_nearby_rekap tdpnr
	where tdpnr.status != 'Titik Lokasi Curanmor'
		and tdpnr.kategori = 'Pasar'
	group by 1,2
)tdpnr3 on tdc.id = tdpnr3.core_id
left join(
	select
		tdpnr.core_id
		,kategori
		,count(distinct id) as jumlah_terdampak
	from db_curanmor_data.tb_data_place_nearby_rekap tdpnr
	where tdpnr.status != 'Titik Lokasi Curanmor'
		and tdpnr.kategori = 'Rumah Sakit'
	group by 1,2
)tdpnr4 on tdc.id = tdpnr4.core_id
left join(
	select
		tdpnr.core_id
		,kategori
		,count(distinct id) as jumlah_terdampak
	from db_curanmor_data.tb_data_place_nearby_rekap tdpnr
	where tdpnr.status != 'Titik Lokasi Curanmor'
		and tdpnr.kategori = 'Swalayan'
	group by 1,2
)tdpnr5 on tdc.id = tdpnr5.core_id
where tdc.lat_tkp is not null and tdc.lon_tkp is not null and tdc.desa_kelurahan is not null