select
	data2.*
	,case when data2.calculation < 11 then 'Lower Priority'
	when data2.calculation between 11 and 20 then 'Medium Priority'
	when data2.calculation > 20 then 'High Priority'
	end as priority_status
from (
	select
		data1.*
		,data1.jumlah_kejahatan*0.5 + data1.jumlah_toko_terdampak*0.125 + data1.jumlah_pasar_terdampak*0.125 + data1.jumlah_rumahsakit_terdampak*0.125 + data1.jumlah_swalayan_terdampak*0.125 as calculation
	from (
		select 
			tdcpn.core_id
			,tdcpn.latitude
			,tdcpn.longitude
			,tdcpn.desa_kelurahan
			,tdcpn.kecamatan
			,case when tdcpn.jumlah_kejahatan is null then 0 else tdcpn.jumlah_kejahatan end as jumlah_kejahatan
			,case when tdcpn.jumlah_toko_terdampak is null then 0 else tdcpn.jumlah_toko_terdampak end as jumlah_toko_terdampak
			,case when tdcpn.jumlah_pasar_terdampak is null then 0 else tdcpn.jumlah_pasar_terdampak end as jumlah_pasar_terdampak
			,case when tdcpn.jumlah_rumahsakit_terdampak is null then 0 else tdcpn.jumlah_rumahsakit_terdampak end as jumlah_rumahsakit_terdampak
			,case when tdcpn.jumlah_swalayan_terdampak is null then 0 else tdcpn.jumlah_swalayan_terdampak end as jumlah_swalayan_terdampak 
		from(
			select
				tdc.id as core_id
				,tdc.lat_tkp as latitude
				,tdc.lon_tkp as longitude
				,tdc.desa_kelurahan
				,tdc.kecamatan
				,tdc.angka_kejahatan as jumlah_kejahatan
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
		)tdcpn
	)data1
)data2