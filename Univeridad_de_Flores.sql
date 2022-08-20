SELECT 
	universidad,carrera, fecha_de_inscripcion,name, sexo,fecha_nacimiento, codigo_postal , correo_electronico, to_date(fecha_de_inscripcion, 'YYYY/MM/DD') AS fecha_inscripcion_date
FROM 
	flores_comahue
WHERE 
	universidad LIKE '%FLORES%' 
AND 
	to_date(fecha_de_inscripcion, 'YYYY/MM/DD') BETWEEN '2020-09-01' AND  '2021-02-01'
ORDER BY 
	fecha_inscripcion_date