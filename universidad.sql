-- Script que muestra dos tablas que contienen datos pedidos.

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

-- Facultad Latinoamericana De Ciencias Sociales
-- cte con principales datos facultad sociales
with facultad_sociales as (
	select
		l.universities as university,
		l.careers as career,
		to_date(l.inscription_dates, 'DD-MM-YYYY') as inscription_date,
		l.names as first_last_name,
		l.sexo as gender,
		age(current_date, to_date(l.birth_dates, 'DD-MM-YYYY')) as _age,
		l.locations as _location,
		l.emails as email	
	from lat_sociales_cine l
)
-- Muestra la tabla con los requisitos pedidos
select *
from facultad_sociales f
where f.inscription_date between '2020-09-01' and '2021-02-01';

-- Universidad J. F. Kennedy
-- cte con principales datos facultad kennedy
with facultad_kennedy as (
	select
		uk.universidades as university,
		uk.carreras as career,
		to_date(uk.fechas_de_inscripcion, 'YY-Mon-DD') as inscription_date,
		uk.nombres as first_last_name,
		uk.sexo as gender,
		age(current_date, to_date(uk.fechas_nacimiento, 'YY-Mon-DD')) as _age,
		uk.codigos_postales as postal_code,
		uk.emails as email	
	from public.uba_kenedy uk
)
-- Muestra la tabla con los requisitos pedidos
select *
from facultad_kennedy fk
where fk.inscription_date between '2020-09-01' and '2021-02-01';
