-- Version 1
select t.*
  from (select rownum, id, date, value
          from tab
	     where date = (select max(date) from tab)
         order by value desc) t
 where t.rownum = 1;

-- Version 2
with mdv as
  (select id, date, value
     from tab
	where date = (select max(date) from tab)
    order by value desc)
select distinct id, value, date
  from tab, 
       mdv
 where (id, date, value) in (select id, date, value from tab where rownum = 1);

 