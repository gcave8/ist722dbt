with stg_sales as (
    select
        OrderID,  
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey,
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey,  
        replace(to_date(orderdate)::varchar, '-', '')::int as orderdatekey
    from {{ source('northwind', 'Orders') }}
),
stg_order_details as (
    select 
        orderid,
        productid as productkey,
        quantity,
        (quantity * unitprice) as extendedpriceamount, 
        discount
    from {{ source('northwind', 'Order_Details') }}
)
select  
    s.orderid,
    s.customerkey,
    s.employeekey,
    s.orderdatekey,
    od.productkey,
    od.quantity,
    od.extendedpriceamount,
    od.extendedpriceamount * od.discount as discountamount,
    od.extendedpriceamount - discountamount as soldamount
from stg_sales s
join stg_order_details od on s.orderid = od.orderid
