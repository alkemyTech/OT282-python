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