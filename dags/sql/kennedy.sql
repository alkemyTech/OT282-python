-- Script que muestra la tabla que contiene datos pedidos.

-- Datos esperados:
-- university
-- career
-- inscription_date
-- first_name
-- last_name
-- gender
-- age
-- postal_code
-- location
-- email

-- Obtener los datos de las pesonas anotadas en entre las fechas 01/9/2020 al 01/02/2021

-- Universidad J. F. Kennedy
select
	uk.universidades as university,
	uk.carreras as career,
	to_date(uk.fechas_de_inscripcion, 'YY-Mon-DD') as inscription_date,
	uk.nombres as first_last_name,
	uk.sexo as gender,
	date_part ('year',age(current_date, to_date(uk.fechas_nacimiento, 'YY-Mon-DD'))) as _age,
	uk.codigos_postales as postal_code,
	uk.emails as email	
from public.uba_kenedy uk
where to_date(uk.fechas_de_inscripcion, 'YY-Mon-DD') between '2020-09-01' and '2021-02-01';
