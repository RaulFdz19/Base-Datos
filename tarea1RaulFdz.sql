--Tarea Raúl Fdz

--1.-Cómo obtenemos todos los nombres y correos de nuestros clientes canadienses para una campaña

select c.first_name ,c.last_name ,c.email, co.country from public.customer c join public.address a using(address_id) 
join public.city ci using(city_id) join public.country co using (country_id)
where co.country ='Canada';

--2.-Qué cliente ha rentado más de nuestra sección de adultos?

select c.first_name, c.last_name ,f.rating , count(customer_id) as total_rentas
from public.customer c join public.rental r using (customer_id)
join public.inventory i using (inventory_id) join public.film f using(film_id) 
where f.rating ='NC-17'
group by c.customer_id , f.rating 
order by total_rentas desc limit 1;

--3.-Qué películas son las más rentadas en todas nuestras stores?

select distinct on(store_id) f.film_id ,f.title, i.store_id, count(f.film_id) as total_rentas 
from public.rental r
join public.inventory i using(inventory_id) 
join public.film f using(film_id)
group by f.film_id ,i.store_id 
order by i.store_id ,total_rentas desc;

--3.-Cuál es nuestro revenue por store?

select s.store_id ,sum(p.amount) as revenue from public.payment p 
join public.staff s using (staff_id)
group by store_id ;


