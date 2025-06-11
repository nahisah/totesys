DROP DATABASE IF EXISTS mock_warehouse;
CREATE DATABASE mock_warehouse;

\c mock_warehouse


CREATE TABLE dim_date (
  date_id DATE PRIMARY KEY NOT NULL,
  year INT NOT NULL,
  month INT NOT NULL,
  day INT NOT NULL,
  day_of_week INT NOT NULL,
  day_name VARCHAR NOT NULL,
  month_name VARCHAR NOT NULL,
  quarter INT NOT NULL
);

CREATE TABLE dim_staff (
  staff_id INT PRIMARY KEY NOT NULL,
  first_name VARCHAR NOT NULL,
  last_name VARCHAR NOT NULL,
  department_name VARCHAR NOT NULL,
  location VARCHAR NOT NULL,
  email_address VARCHAR NOT NULL
);

CREATE TABLE dim_location (
  location_id INT PRIMARY KEY NOT NULL,
  address_line_1 VARCHAR NOT NULL,
  address_line_2 VARCHAR,
  district VARCHAR,
  city VARCHAR NOT NULL,
  postal_code VARCHAR NOT NULL,
  country VARCHAR NOT NULL,
  phone VARCHAR NOT NULL
);

CREATE TABLE dim_currency (
  currency_id INT PRIMARY KEY NOT NULL,
  currency_code VARCHAR NOT NULL,
  currency_name VARCHAR NOT NULL
);

CREATE TABLE dim_design (
  design_id INT PRIMARY KEY NOT NULL,
  design_name VARCHAR NOT NULL,
  file_location VARCHAR NOT NULL,
  file_name VARCHAR NOT NULL
);

CREATE TABLE dim_counterparty (
  counterparty_id INT PRIMARY KEY NOT NULL,
  counterparty_legal_name VARCHAR NOT NULL,
  counterparty_legal_address_line_1 VARCHAR NOT NULL,
  counterparty_legal_address_line_2 VARCHAR,
  counterparty_legal_district VARCHAR,
  counterparty_legal_city VARCHAR NOT NULL,
  counterparty_legal_postal_code VARCHAR NOT NULL,
  counterparty_legal_country VARCHAR NOT NULL,
  counterparty_legal_phone_number VARCHAR NOT NULL
);

CREATE TABLE fact_sales_order (
  sales_record_id SERIAL PRIMARY KEY NOT NULL,
  sales_order_id INT NOT NULL,
  created_date DATE REFERENCES dim_date(date_id) NOT NULL,
  created_time TIME NOT NULL DEFAULT CURRENT_TIME,
  last_updated_date DATE REFERENCES dim_date(date_id) NOT NULL,
  last_updated_time TIME NOT NULL DEFAULT CURRENT_TIME,
  sales_staff_id INT REFERENCES dim_staff(staff_id) NOT NULL,
  counterparty_id INT REFERENCES dim_counterparty(counterparty_id) NOT NULL,
  units_sold INT NOT NULL,  -- Note: value 1000 - 100000
  unit_price NUMERIC(10, 2) NOT NULL,  -- Note: value 2.00 - 4.00
  currency_id INT REFERENCES dim_currency(currency_id) NOT NULL,
  design_id INT REFERENCES dim_design(design_id) NOT NULL,
  agreed_payment_date DATE REFERENCES dim_date(date_id) NOT NULL,  -- Format: yyyy-mm-dd
  agreed_delivery_date DATE REFERENCES dim_date(date_id) NOT NULL,  -- Format: yyyy-mm-dd
  agreed_delivery_location_id INT REFERENCES dim_location(location_id) NOT NULL
);