


--Tarea Tiempo entre pagos


with customer_payments as (
	select c.customer_id, c.first_name ||' '|| c.last_name as name , p.payment_date ,
	row_number() over(partition by c.customer_id order by c.customer_id) as fila
	,lag (p.payment_date) over(partition by c.customer_id order by c.customer_id)
	from public.payment p join public.customer c using(customer_id)
 
),--tiempo entre pagos por cliente
payment_age as (
	select cp.customer_id,  age(cp.payment_date,lag) as inter_pago from customer_payments cp 
),--Promedio tiempo entre pagos por cliente
inter_pagos_mean as (
	select pa.customer_id, avg( pa.inter_pago)
	from payment_age pa
	group by pa.customer_id
),--frecuencia de dias para obtener la distr
freq_days as (
	select extract('days' from avg ::interval) as dias, count(*) as freq from inter_pagos_mean
	group by dias 
), 
total_freqdays as(
	select sum(freq) as total from freq_days
),--Distribución de los dias entre pagos
distr_dias as (
	select fd.dias,fd.freq, round((fd.freq/tf.total) * 100, 2) from freq_days fd, total_freqdays tf order by fd.dias
	--Por lo que no es normal 
),
--tiempo entre rentas por cliente
rental_age as (
	select c.customer_id, c.first_name ||' '|| c.last_name as name ,
	--row_number() over(partition by c.customer_id order by c.customer_id,r.rental_date)
	age( r.rental_date ,
		lag (r.rental_date ) over(partition by c.customer_id order by c.customer_id,r.rental_date) )
	from public.rental r join public.customer c using(customer_id)
),
inter_rentas_mean as (
	select ra.customer_id, ra.name, avg( ra.age) as media_rentas
	from rental_age ra
	group by ra.customer_id, ra.name order by customer_id
)
--Diferencia promedio entre tiempo de pago y renta


select mr.name, mr.media_rentas,mp.avg as media_pagos, mr.media_rentas - mp.avg as diferencia
from inter_rentas_mean mr join  inter_pagos_mean mp using(customer_id)

--No hay diferencias



