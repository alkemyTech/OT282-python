--Extraigo columnas de la tabla jujuy_utn
SELECT university, career, CAST (inscription_date AS DATE),nombre, sexo, AGE(CAST (birth_date AS DATE)), location, email
FROM public.jujuy_utn 
WHERE CAST (inscription_date AS DATE) BETWEEN '2020-09-01' AND '2021-02-01'

