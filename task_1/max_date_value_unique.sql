/**********************************************************************************************************************
* «адача: 
*   ¬ывести строку с максимальной датой, если есть несколько таких записей, то вывести одну с максимальнмм значением value.
*   ¬ывод не должен содержать дубликаты.
* –ешение:
*   1. —оздаетс€ таблица task_1, наполн€етс€ тестовыми данными пор€дка ста тыс€ч строк
*   2. ѕровер€етс€, что в тестовых данных действительно содержатс€ дубликаты
*   3. ¬ыбираетс€ искома€ строка двум€ способами (см. последние два запроса ниже)
**********************************************************************************************************************/

-----------------------------------------------------------------------------------------------------------------------
-- Create the table
-----------------------------------------------------------------------------------------------------------------------

-- drop table task_1;

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
insert into task_1 values (3, 8, '2017-06-06');

-----------------------------------------------------------------------------------------------------------------------
-- Fill in the table with test data
-----------------------------------------------------------------------------------------------------------------------
begin
  dbms_random.seed(13);
  for i in 1..100000 loop
    insert into task_1 values (
      round(100*dbms_random.value), 
      round(10*dbms_random.value), 
      to_date('2017-01-01', 'YYYY-MM-DD') + round(365*dbms_random.value));
  end loop;
end;
/

commit;

-- optional index to make the query run faster
create index task_1_n1 on task_1 (vdate);

-----------------------------------------------------------------------------------------------------------------------
-- Validating there are duplicates
-----------------------------------------------------------------------------------------------------------------------
select * 
  from task_1;

-- checking duplicates, to make sure we've got plenty of them
select vdate, value, count(*) c
  from task_1
 group by vdate, value
 order by vdate desc, value desc, c desc;

-----------------------------------------------------------------------------------------------------------------------
-- The row with max date and max value, version 1
-----------------------------------------------------------------------------------------------------------------------
select vdate, value
  from (select rownum, id, vdate, value
          from task_1
             where vdate = (select max(vdate) from task_1)
         order by value desc) t
 where rownum = 1;

-----------------------------------------------------------------------------------------------------------------------
-- The row with max date and max value, version 2
-----------------------------------------------------------------------------------------------------------------------
with mdv as
  (select id, vdate, value
     from task_1
        where vdate = (select max(vdate) from task_1)
    order by value desc)
select value, vdate
  from task_1 t1
 where (id, value, vdate) in (select id, value, vdate from mdv where rownum = 1);
