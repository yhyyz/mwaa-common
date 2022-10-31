drop table if exists filtered_fruit;
CREATE TABLE filtered_fruit AS SELECT * FROM fruit WHERE color = '{{ params.color }}';