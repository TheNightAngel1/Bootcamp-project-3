/*select
    employee_id
from warehouse_northwind.staging.orders
where employee_id > (select count(*) from warehouse_northwind.staging.employees) and employee_id < 1*/
