--Extraigo columnas
SELECT universidad, careers, CAST (fecha_de_inscripcion AS DATE),names, sexo, AGE(CAST (birth_dates AS DATE)),codigo_postal, correos_electronicos
FROM public.palermo_tres_de_febrero 
WHERE CAST (fecha_de_inscripcion AS DATE) BETWEEN '2020-09-01' AND '2021-02-01'