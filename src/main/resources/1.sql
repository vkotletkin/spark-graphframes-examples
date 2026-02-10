create table public.ingestion_registry
(
    id             serial primary key,
    source_name    varchar(255) not null,
    query          text         not null,
    address        text         not null,
    username       varchar(100),
    password       text,
    num_partitions integer default 1,
    active         boolean default true
);


insert into ingestion_registry (source_name, query, address, username, password, num_partitions)
values ('db1_source', 'SELECT * FROM src_table', 'localhost:5432', 'postgres', 'postgres', 8);