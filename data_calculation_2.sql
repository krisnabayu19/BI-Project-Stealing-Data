select
	tdc.id
	,tdc.laporan_dasar as dasar
	,twl.waktu_lapor
	,twk.waktu_kejadian
	,hour(twk.waktu_kejadian) as jam_kejadian
	,case when hour(twk.waktu_kejadian) between 0 and 3 then 'Jam 0 - 3'
	when hour(twk.waktu_kejadian) between 4 and 6 then 'Jam 4 - 6'
	when hour(twk.waktu_kejadian) between 7 and 9 then 'Jam 7 - 9'
	when hour(twk.waktu_kejadian) between 10 and 12 then 'Jam 10 - 12'
	when hour(twk.waktu_kejadian) between 13 and 15 then 'Jam 13 - 15'
	when hour(twk.waktu_kejadian) between 16 and 18 then 'Jam 16 - 18'
	when hour(twk.waktu_kejadian) between 19 and 21 then 'Jam 19 - 21'
	when hour(twk.waktu_kejadian) between 22 and 23 then 'Jam 22 - 23'
	end as rentang_jam_kejadian
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
	,tjmk.jenis_merk_kendaraan
	,ttk2.teknik_kejahatan
	,tk.kecamatan
	,dtc.jumlah_kejahatan
	,case when dtc.jumlah_kejahatan is null then null
	when dtc.jumlah_kejahatan = 1 then 'Jumlah Kasus 1'
	when dtc.jumlah_kejahatan = 2 then 'Jumlah Kasus 2'
	when dtc.jumlah_kejahatan = 3 then 'Jumlah Kasus 3'
	when dtc.jumlah_kejahatan > 3 then 'Jumlah Kasus > 3'
	end as status_jumlah_kejahatan
from db_curanmor.tb_data_curanmor tdc
left join db_curanmor.tb_waktu_lapor twl on tdc.id_waktu_lapor = twl.id_waktu_lapor
left join db_curanmor.tb_waktu_kejadian twk on tdc.id_waktu_kejadian = twk.id_waktu_kejadian
left join db_curanmor.tb_pelapor tp on tdc.id_pelapor = tp.id_pelapor
left join db_curanmor.tb_detail_merk_kendaraan tdmk on tdc.id_detail_merk_kendaraan = tdmk.id_detail_merk_kendaraan 
left join db_curanmor.tb_teknik_kejahatan ttk2 on tdc.id_teknik_kejahatan = ttk2.id_teknik_kejahatan
left join db_curanmor.tb_kecamatan tk on tdc.id_kecamatan = tk.id_kecamatan
left join db_curanmor.tb_merk_kendaraan tmk on tdmk.id_merk_kendaraan = tmk.id_merk_kendaraan
left join db_curanmor.tb_jenis_merk_kendaraan tjmk on tdmk.id_jenis_merk_kendaraan = tjmk.id_jenis_merk_kendaraan
left join (
	select 
		tdc.lat_tkp
		,tdc.lon_tkp
		,count(distinct tdc.id) as jumlah_kejahatan
	from db_curanmor.tb_data_curanmor tdc
	where tdc.lat_tkp is not null and tdc.lon_tkp is not null
	group by 1,2
) dtc on tdc.lat_tkp = dtc.lat_tkp and tdc.lon_tkp = dtc.lon_tkp