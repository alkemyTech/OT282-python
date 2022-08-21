SELECT 
	universidad, carrera,fecha_de_inscripcion, nombre, sexo,fecha_nacimiento, localidad, email, fecha_de_inscripcion :: date AS fecha_inscripcion_date
FROM 
	salvador_villa_maria
WHERE
	universidad LIKE '%VILLA%'
AND 
	fecha_de_inscripcion :: date between '2020-09-01' and  '2021-02-01'
ORDER BY 
	fecha_inscripcion_date