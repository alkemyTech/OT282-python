-- UNIVERSIDAD DEL COMAHUE
select
    universidad as university,
    carrera as career,
    fecha_de_inscripcion as inscription_date,
    name as full_name,
    sexo as gender,
    fecha_nacimiento as edad,
    codigo_postal as postal_code,
    correo_electronico as email
from
    flores_comahue fc
where
    fecha_de_inscripcion :: date >= '2020-09-01'
    and fecha_de_inscripcion :: date < '2021-02-01'
    and universidad = 'UNIV. NACIONAL DEL COMAHUE';

-- UNIVERSIDAD DEL SALVADOR
select
    universidad as university,
    carrera as career,
    fecha_de_inscripcion as inscription_date,
    nombre as full_name,
    sexo as gender,
    fecha_nacimiento as edad,
    localidad as location,
    email as email
from
    salvador_villa_maria svm
where
    fecha_de_inscripcion :: date >= '02-Sep-2020'
    and fecha_de_inscripcion :: date < '01-Feb-2021'
    and universidad = 'UNIVERSIDAD_DEL_SALVADOR';