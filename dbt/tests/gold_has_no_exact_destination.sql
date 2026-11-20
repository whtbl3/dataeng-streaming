-- AC3 (NFR-6): a customer's exact destination never reaches gold; only dest_area (~150 m).
select
    table_name,
    column_name
from {{ target.catalog }}.information_schema.columns
where
    table_schema = 'gold'
    and column_name in ('dest_lat', 'dest_lon')
