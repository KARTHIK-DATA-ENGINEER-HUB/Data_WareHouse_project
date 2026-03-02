 ----checking the foreign key integration in the gold layer
 select * from gold.fact_sales s
 left join gold.dim_products p
 on s.product_key = p.product_key
 where p.product_key is null


  select * from gold.fact_sales s
 left join gold.dim_customers c
 on s.customer_key = c.customer_key
 where c.customer_key is null
