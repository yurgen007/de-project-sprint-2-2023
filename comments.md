#### Комментарии и вопросы

Доброго времени суток. Да, на "ты" отлично.  
Я новичок  в специальности, поэтому делаю проект по образцу, который мы уже выполнили 
для витрины по мастерам. Но к этому образцу у меня есть пара вопросов. Речь идет о скрипте инкрементального обновления 
витрины данных incremental_update_customer.sql

Первый вопрос:  

При вычислении dwh_delta мы фильтурем данные в том числе по дате загрузки для таблицы фактов fo.load_dttm. 
Здесь у нас фильтр по четырем условиям на даты загрузки.
```sql
...
dwh_delta AS 
( 
    SELECT dcs.customer_id AS customer_id,
...
           dc.load_dttm AS craftsman_load_dttm,
           dcs.load_dttm AS customers_load_dttm,
           dp.load_dttm AS products_load_dttm
           ...
    WHERE (fo.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart))
          OR (dc.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart)) 
          OR (dcs.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart))
          OR (dp.load_dttm > (SELECT COALESCE(MAX(load_dttm),'1900-01-01') FROM dwh.load_dates_customer_report_datamart))
),
...
```
Но в дальнейшем мы дату загрузки fo.load_dttm уже не используем, и для вычисления даты очередного изменения данных в источниках используем уже только три условия. 
```sql
...
insert_load_date AS 
( 
    INSERT INTO dwh.load_dates_customer_report_datamart (load_dttm)
    SELECT GREATEST(COALESCE(MAX(craftsman_load_dttm), NOW()), 
                    COALESCE(MAX(customers_load_dttm), NOW()), 
                    COALESCE(MAX(products_load_dttm), NOW())) 
    FROM dwh_delta
)
...
```
А почему?  

Второй вопрос:  
При вычислении dwh_delta_insert_result мы опираемся на dwh_delta, поскольку она содержит новые данные, которых еще нет в витрине. Ок. 

```sql
...
dwh_delta_insert_result AS 
(
     SELECT t5.customer_id AS customer_id,
     ...
         FROM (SELECT *,
         ...
              FROM (SELECT t1.customer_id AS customer_id,
              ...
                     FROM dwh_delta AS t1
              ...
              INNER JOIN (SELECT dd.customer_id AS customer_id_for_product_type,      
                           ...
                           FROM dwh_delta AS dd
               ...
              INNER JOIN (SELECT dd.customer_id AS customer_id_for_craftsman,
                           ...
                           FROM dwh_delta AS dd
...
```

При вычислении dwh_delta_update_result мы вынуждены заменить dwh_delta на подзапрос t1, поскольку для уже существующих заказчиков в хранилище есть данные, которые не вошли в dwh_delta. 
Однако, в подзапросах, вычисляющих данные для  определения самой популярной категория товаров и самого популярно мастера, мы снова используем dwh_delta. Хотя, по идее, надо бы использовать такой же подзапрос t1. 
```sql
dwh_delta_update_result AS 
( 
    SELECT t5.customer_id AS customer_id,
     ...
         FROM (SELECT *,
         ...
              FROM (SELECT t1.customer_id AS customer_id,
              ...
                    FROM (SELECT dcs.customer_id AS customer_id,   
                                 dcs.customer_name AS customer_name,
                                 dcs.customer_address AS customer_address,
                                 dcs.customer_birthday AS customer_birthday,
                                 dcs.customer_email AS customer_email,
                                 fo.order_id AS order_id,
                                 dp.product_id AS product_id,
                                 dp.product_price AS product_price,
                                 dp.product_type AS product_type,
                                 fo.order_completion_date - fo.order_created_date AS diff_order_date, 
                                 fo.order_status AS order_status,
                                 TO_CHAR(fo.order_created_date, 'yyyy-mm') AS report_period,
                                 dc.craftsman_id AS craftsman_id
                          FROM dwh.f_order fo 
                               INNER JOIN dwh.d_craftsman dc ON fo.craftsman_id = dc.craftsman_id 
                               INNER JOIN dwh.d_customer dcs ON fo.customer_id = dcs.customer_id 
                               INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id
                               INNER JOIN dwh_update_delta ud ON fo.customer_id = ud.customer_id
                         ) AS t1              
                     ...
              INNER JOIN (SELECT dd.customer_id AS customer_id_for_product_type,      
                           ...
                           FROM dwh_delta AS dd
               ...
              INNER JOIN (SELECT dd.customer_id AS customer_id_for_craftsman,
                           ...
                           FROM dwh_delta AS dd
...
```