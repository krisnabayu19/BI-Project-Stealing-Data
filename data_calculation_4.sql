select
	tdc.id
	,tdc.laporan_dasar as dasar
	,tdc.waktu_lapor 
	,tdc.waktu_kejadian
	,hour(tdc.waktu_kejadian) as jam_kejadian
	,case when hour(tdc.waktu_kejadian) between 7 and 10 then 'Jam 07.00 - 10.00'
	when hour(tdc.waktu_kejadian) between 11 and 14 then 'Jam 11.00 - 14.00'
	when hour(tdc.waktu_kejadian) between 15 and 18 then 'Jam 15.00 - 18.00'
	when hour(tdc.waktu_kejadian) between 19 and 22 then 'Jam 19.00 - 22.00'
	when hour(tdc.waktu_kejadian) = 23 then 'Jam 23.00 - 02.00'
	when hour(tdc.waktu_kejadian) between 0 and 2 then 'Jam 23.00 - 02.00'
	when hour(tdc.waktu_kejadian) between 3 and 6 then 'Jam 03.00 - 06.00'
	end as rentang_jam_kejadian
	,concat(date_format(tdc.waktu_kejadian, '%Y-%m'),'-01') as bulan_tahun
	,year(tdc.waktu_kejadian) as tahun_category
	,tdc.tkp
	,tdc.lat_tkp
	,tdc.lon_tkp
	,tdc.no_pol
	,tdc.no_ka
	,tdc.no_sin
	,tp.nama_pelapor
	,tp.umur_pelapor
	,tp.gender_pelapor
	,tp.alamat_pelapor
	,tmk.merk_kendaraan
	,tmk.jenis_merk_kendaraan 
	,ttk2.teknik_kejahatan
	,tdkt.nama_desa_kelurahan as desa_kelurahan
	,tkt.nama_kecamatan as kecamatan
	,dtc.jumlah_kejahatan as angka_kejahatan
	,case when dtc.jumlah_kejahatan is null then null
	when dtc.jumlah_kejahatan between 1 and 2 then 'Jumlah Kasus 1 - 2'
	when dtc.jumlah_kejahatan between 3 and 4 then 'Jumlah Kasus 3 - 4'
	when dtc.jumlah_kejahatan >= 5 then 'Jumlah Kasus >=5'
	end as status_jumlah_kejahatan
from db_curanmor_data.tb_data_curanmor tdc
left join db_curanmor_data.tb_pelapor tp on tdc.id_pelapor = tp.id_pelapor
left join db_curanmor_data.tb_teknik_kejahatan ttk2 on tdc.id_teknik_kejahatan = ttk2.id_teknik_kejahatan
left join db_curanmor_data.tb_merk_kendaraan tmk on tdc.id_merk_kendaraan = tmk.id_merk_kendaraan
left join db_curanmor_data.tb_desa_kelurahan_tkp tdkt on tdc.id_desa_kelurahan = tdkt.id_desa_kelurahan
left join db_curanmor_data.tb_kecamatan_tkp tkt on tdkt.id_kecamatan = tkt.id_kecamatan
left join (
select
	tdc.lat_tkp
	,tdc.lon_tkp
	,count(distinct tdc.id) as jumlah_kejahatan
from db_curanmor_data.tb_data_curanmor tdc
where tdc.lat_tkp is not null and tdc.lon_tkp is not null
group by 1,2
) dtc on tdc.lat_tkp = dtc.lat_tkp and tdc.lon_tkp = dtc.lon_tkp