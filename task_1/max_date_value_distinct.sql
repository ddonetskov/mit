drop table task_1;

create table task_1 (
  id         number,
  value      number,
  vdate      date);

alter session set nls_date_format = 'YYYY-MM-DD';

insert into task_1 values (1, 5, '2017-01-01');
insert into task_1 values (1, 6, '2017-01-01');
insert into task_1 values (2, 2, '2017-01-02');
insert into task_1 values (3, 5, '2017-05-06');
insert into task_1 values (3, 6, '2017-06-06');

-- TBC generator
begin
  for i in 1..200 loop
    null;
  end loop;
end;
/

commit;

select * from task_1;

-- The row with max date and max value, version 1
select t.*
  from (select rownum, id, vdate, value
          from task_1
         where vdate = (select max(vdate) from task_1)
         order by value desc) t
 where rownum = 1;

-- The row with max date and max value, version 2
with mdv as
  (select id, vdate, value
     from task_1
    where vdate = (select max(vdate) from task_1)
    order by value desc)
select distinct id, value, vdate
  from task_1 t1
 where (t1.id, t1.vdate, t1.value) in (select id, vdate, value from mdv where rownum = 1);

