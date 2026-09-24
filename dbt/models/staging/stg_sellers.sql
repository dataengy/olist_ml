{#  Продавцы маркетплейса, 3 095 строк. Чистая таблица без сюрпризов.  #}

select
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state

from {{ raw_table('sellers') }}
