create DATABASE saasgtm;
USE saasgtm;

CREATE TABLE raw_customers(
customer_id VARCHAR(10) NOT NULL,
signup_date VARCHAR(20) NULL,
segment VARCHAR(20) NULL,
country CHAR(2) NULL,
is_enterprise VARCHAR(10) NULL
);

SELECT * FROM raw_customers;

CREATE TABLE raw_subscriptions(
subscription_id VARCHAR(10) NOT NULL,
customer_id VARCHAR(10) NOT NULL,
start_date varchar(20) NULL,
end_date VARCHAR(20) NULL,
monthly_price VARCHAR(10) NOT NULL,
status VARCHAR(20) NULL
);

CREATE TABLE raw_events(
event_id VARCHAR(50),
customer_id VARCHAR(50),
event_type VARCHAR(50),
event_date VARCHAR(20),
source VARCHAR(50),
INDEX idx_customer_id (customer_id),
INDEX idx_event_type (event_type)
);
    
    



 


