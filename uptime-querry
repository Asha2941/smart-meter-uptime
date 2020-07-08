------------
#PER DAY/GRID querried against the queen database.
------------
#multiplication is the rate which data is sent over network.
select site,dt,uptime_secs, round((uptime_secs::float/(
    (select count(*) from ph_model_queen as q where q.grid_id = gid)*24*60*60)::float)::numeric * 100,2) percent_uptime from
(select site, gid,date(dt) dt,
sum(case when prev is not null then
         case when uptime_secs >= prev then uptime_secs - prev else uptime_secs end
        else 900 end) uptime_secs from
(select dt, site,gid,queen, uptime_secs,
    lag(uptime_secs) over
        (partition by site,queen order by dt) as prev from
(SELECT queenmonitor."collectTime"  as dt,
    queen.number AS queen,
    replace(replace(grid.name,' site ',' '),'-CF','')  AS site,
    grid.id gid,
    queenmonitor."fwUptime" uptime_secs
FROM ph_model_queenmonitor  AS queenmonitor
LEFT JOIN ph_model_queen  AS queen ON (queenmonitor."queen_id") = (queen."id")
LEFT JOIN ph_model_grid  AS grid ON (queen."grid_id") = (grid."id")
WHERE DATE(queenmonitor."collectTime" ) between  date(now() - interval '7 day') and date(now() - interval '1 day') AND
grid.operational_date < now()
--and grid.name='235 A' and queen.number = '1'
order by site, queen, dt
) as x
) as y
group by 1,2,3) as z
order by 1,2

-----------
#WEEKLY/GRID
-----------
SELECT site, week, SUM(percent_uptime)/7 as weekly_pc FROM
  (select site, week, dt,uptime_secs, round((uptime_secs::float/(
    (select count(*) from ph_model_queen as q where q.grid_id = gid)*24*60*60)::float)::numeric * 100,2) percent_uptime from
(select site, gid, extract(week FROM dt) as week, date(dt) dt,
sum(case when prev is not null then
         case when uptime_secs >= prev then uptime_secs - prev else uptime_secs end
        else 900 end) uptime_secs from
(select dt, site,gid,queen, uptime_secs,
    lag(uptime_secs) over
        (partition by site,queen order by dt) as prev from
(SELECT queenmonitor."collectTime"  as dt,
    queen.number AS queen,
    replace(replace(grid.name,' site ',' '),'-CF','')  AS site,
    grid.id gid,
    queenmonitor."fwUptime" uptime_secs
FROM ph_model_queenmonitor  AS queenmonitor
LEFT JOIN ph_model_queen  AS queen ON (queenmonitor."queen_id") = (queen."id")
LEFT JOIN ph_model_grid  AS grid ON (queen."grid_id") = (grid."id")
WHERE DATE(queenmonitor."collectTime" ) between '2018-06-04' AND '2018-11-18' AND
grid.operational_date < now()
--and grid.name='235 A' and queen.number = '1'
order by site, queen, dt
) as x
) as y
group by 1,2,3,4) as z) as fin
  GROUP BY 1,2
order by 1,2
