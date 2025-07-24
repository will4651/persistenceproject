--1) Average Total cost per Month-Day

select to_char(lpep_dropoff, 'MM-DD') as month_day, avg(total_amount) as avg_total_amount
from taxi_data
group by 1
order by 1;

--2) Average Trip_distance per month-day

select to_char(lpep_dropoff, 'MM-DD') as month_day, avg(trip_distance) as avg_trip_distance
from taxi_data
group by 1
order by 1;

--3) Count of rides per month-day by pickup location

select to_char(lpep_pickup, 'MM-DD') as month_day, zl.zone, count(*) as total_rides
from taxi_data inner join zone_lookup zl on pu_locationid = zl.locationid
group by 1, 2
order by 1, 2;

--4) count of payment_type by pickup location per month-day

select to_char(lpep_pickup, 'MM-DD') as month_day, pt.payment_type, count(*) as total
from taxi_data td inner join payment_types pt on td.payment_type = pt.id
group by 1, 2
order by 1, 2;

--5) average percent of tip vs total amount by pickup location

select zl.zone, avg(tip_amount / total_amount) * 100 as average_tip
from taxi_data inner join zone_lookup zl on pu_locationid = zl.locationid
where total_amount > 0
group by 1;

--6) average tip by rounded down trip_distance per month-day

select to_char(lpep_dropoff, 'MM-DD') as month_day, cast(trip_distance as int) as trip_distance, avg(tip_amount) as avg_tip
from taxi_data
group by 1, 2
order by 1, 2;

--7) average duration of rides per month-day

select to_char(lpep_dropoff, 'MM-DD') as month_day,
       avg(extract(epoch from lpep_dropoff) - extract(epoch from lpep_pickup)) / 60 as avg_durantion
from taxi_data
group by 1
order by 1, 2;

--8) average tax by trip_distance per month-day
--assuming mta_tax

select to_char(lpep_dropoff, 'MM-DD') as month_day, cast(trip_distance as int) as trip_distance, avg(mta_tax) as avg_mta_tax
from taxi_data
group by 1, 2
order by 1, 2;

--9) average trip_distance vs average duration of ride per month-day

select to_char(lpep_dropoff, 'MM-DD') as month_day, avg(trip_distance) as avg_trip_distance,
       avg(extract(epoch from lpep_dropoff) - extract(epoch from lpep_pickup)) / 60 as avg_duration
from taxi_data
group by 1
order by 1;

--10) pickup location (name) to drop location (name): show count of rides and average duration by month-day

select to_char(lpep_dropoff, 'MM-DD') as month_day,
       zl1.zone::text || ' -> ' || zl2.zone::text as pickup_droppoff,
       count(*) as total_rides,
       avg(extract(epoch from td.lpep_dropoff - td.lpep_pickup) / 60) as avg_duration
from taxi_data td
    inner join zone_lookup zl1 on pu_locationid = zl1.locationid
    inner join zone_lookup zl2 on do_locationid = zl2.locationid
group by 1, 2
having count(*) > 10
order by 2, 1;

--11) average passenger count by month-day

select to_char(lpep_dropoff, 'MM-DD') as month_day, avg(passenger_count) as avg_passenger_count
from taxi_data
group by 1
order by 1;

--12) average tip amount by trip_distance greater than 2 by month-day

select to_char(lpep_dropoff, 'MM-DD') as month_day, avg(tip_amount) as avg_tip_amount
from taxi_data
where trip_distance > 10
group by 1
order by 1;

--13) pickup location (name) to drop location (name): show count of each payment_type (name) by month-day

select to_char(td.lpep_dropoff, 'MM-DD') as month_day,
       zl1.zone::text || ' -> ' || zl2.zone::text,
       pt.payment_type as pickup_dropoff, count(*) as total
from taxi_data td
    inner join zone_lookup zl1 on pu_locationid = zl1.locationid
    inner join zone_lookup zl2 on do_locationid = zl2.locationid
    inner join payment_types pt on td.payment_type = pt.id
group by 1, 2, 3
order by 1;

--14) average miles per hour by month-day

select
     to_char(lpep_dropoff, 'MM-DD') as month_day,
     avg((trip_distance / ((round(extract(epoch from td.lpep_dropoff - td.lpep_pickup) / 60, 2) )/ 60))) as avg_mph
from taxi_data td
where extract(epoch from td.lpep_dropoff - td.lpep_pickup) > 0
group by 1
order by 1;

--15) most expensive (fare_amount) drop location by month-day

select to_char(lpep_dropoff, 'MM-DD') as month_day, zl.zone, max(fare_amount) as max_fare
from taxi_data inner join zone_lookup zl on pu_locationid = zl.locationid
group by 1, 2
order by 1, 2;