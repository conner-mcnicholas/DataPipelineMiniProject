#!/bin/bash

echo 'ticket_id,trans_date,event_id,event_name,event_date,event_type,event_city,customer_id,price,num_tickets' > third_party_sales_header.csv

cat third_party_sales_header.csv third_party_sales.csv > third_party_sales_full.csv

