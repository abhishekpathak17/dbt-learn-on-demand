{%- set payment_methods = ['bank_transfer', 'coupon', 'credit_card', 'gift_card'] -%}

with payments as (
    select * from {{ ref('stg_payments') }}
),

pivoted as (
    select
        order_id,
        {% for pay_mtd in payment_methods -%}
        sum(case when payment_method = '{{ pay_mtd }}' then amount else 0 end) as {{pay_mtd}}_amount {%- if not loop.last -%} , {% endif %}
        {% endfor -%}
    from payments
    where status = 'success'
    group by 1
)

select * from pivoted