-- /////////////////////////////////////////////////////////////////----------//////////////////////////////////////////////////////////////////////////////////////
-- --------------------////////-----------------------///////////// CLEANING //////////////-----------------////----------------------//////------------------------
-- ////////////////////////////////////////////////////////////////------------/////////////////////////////////////////////////////////////////////////////////////
--  CLEANING CUSTOMERS DATA
CREATE TABLE cleaned_customers AS
SELECT 
	customer_id, 
	STR_TO_DATE(NULLIF(TRIM(signup_date),''), '%Y-%m-%d') AS singup_date,
	COALESCE(NULLIF(TRIM(segment),''),'Unknown') AS segment,
	UPPER(TRIM(country)) AS country,
	CASE
		WHEN LOWER(TRIM(is_enterprise))= 'true' THEN 1
		WHEN LOWER(TRIM(is_enterprise))= 'false' THEN 0
		ELSE 0
	END AS is_enterprise
FROM raw_customers;
    
ALTER TABLE cleaned_customers
ADD PRIMARY KEY (customer_id),
ADD INDEX idx_segment(segment);

SELECT * FROM cleaned_customers;

--  CLEANING SUBSCRIPTIONS DATA
CREATE TABLE cleaned_subscriptions AS
SELECT
	subscription_id,
    customer_id,
    STR_TO_DATE(NULLIF(TRIM(start_date),''), '%Y-%m-%d') AS start_date,
    STR_TO_DATE(NULLIF(TRIM(end_date),''), '%Y-%m-%d') AS end_date,
    CAST(NULLIF(TRIM(monthly_price),'') AS UNSIGNED) AS monthly_price,
    LOWER(TRIM(status)) AS status
FROM raw_subscriptions;

ALTER TABLE cleaned_subscriptions
	ADD PRIMARY KEY (subscription_id),
	ADD INDEX idx_sub_cust (customer_id),
	ADD CONSTRAINT fk_sub_customer
		FOREIGN KEY(customer_id) REFERENCES cleaned_customers(customer_id) ;

SELECT * FROM cleaned_subscriptions;

-- CLEANING EVENTS DATA

CREATE TABLE cleaned_events AS
SELECT *
FROM(
	SELECT 
		event_id, customer_id, event_type,
		STR_TO_DATE(NULLIF(TRIM(event_date),''), '%Y-%m-%d') AS event_date,
		source,
		ROW_NUMBER() OVER(PARTITION BY customer_id, event_type, event_date ORDER BY event_id) AS rn
    FROM raw_events
) t WHERE rn=1;

ALTER TABLE cleaned_events
	ADD PRIMARY KEY (event_id),
    ADD CONSTRAINT fk_evt_customer
		FOREIGN KEY (customer_id) REFERENCES cleaned_customers(customer_id);
        
SELECT * FROM cleaned_events;