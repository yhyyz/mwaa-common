BEGIN;
create table fruit_tmp(like fruit);
insert into fruit_tmp select 1, 'Banana', 'Yellow';
drop table fruit;
alter table fruit_tmp rename to fruit;
END;