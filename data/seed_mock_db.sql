DROP DATABASE IF EXISTS mock_totesys;
CREATE DATABASE mock_totesys;

\c mock_totesys


CREATE Table currency (
  currency_id SERIAL PRIMARY KEY not null,
  currency_code varchar(3) not null, --note: 'three letter code eg GBP'
  created_at timestamp not null DEFAULT CURRENT_TIMESTAMP,
  last_updated timestamp not null DEFAULT CURRENT_TIMESTAMP
);

CREATE Table department (
  department_id SERIAL PRIMARY KEY not null,
  department_name varchar not null,
  location varchar, --[note: 'nullable']
  manager varchar, --[note: 'nullable']
  created_at timestamp not null DEFAULT CURRENT_TIMESTAMP,
  last_updated timestamp not null DEFAULT CURRENT_TIMESTAMP
);

CREATE Table address (
  address_id SERIAL PRIMARY KEY not null,
  address_line_1 varchar not null,
  address_line_2 varchar, --[note: 'nullable']
  district varchar, --[note: 'nullable']
  city varchar not null,
  postal_code varchar not null,
  country varchar not null,
  phone varchar not null, --note: 'valid phone number'
  created_at timestamp not null DEFAULT CURRENT_TIMESTAMP,
  last_updated timestamp not null DEFAULT CURRENT_TIMESTAMP
);

CREATE Table counterparty (
  counterparty_id SERIAL PRIMARY KEY not null,
  counterparty_legal_name varchar not null,
  legal_address_id int not null REFERENCES address(address_id),
  commercial_contact varchar, --note: 'person name, nullable'
  delivery_contact varchar, --[note: 'person name, nullable']
  created_at timestamp not null DEFAULT CURRENT_TIMESTAMP,
  last_updated timestamp not null DEFAULT CURRENT_TIMESTAMP
);

CREATE Table staff (
  staff_id SERIAL PRIMARY KEY not null,
  first_name varchar not null,
  last_name varchar not null,
  department_id int REFERENCES department(department_id) not null,
  email_address varchar not null, --note: 'must be valid email address']
  created_at timestamp not null DEFAULT CURRENT_TIMESTAMP,
  last_updated timestamp not null DEFAULT CURRENT_TIMESTAMP
);

CREATE Table design (
  design_id int PRIMARY KEY not null,
  created_at timestamp not null DEFAULT CURRENT_TIMESTAMP,
  last_updated timestamp not null DEFAULT CURRENT_TIMESTAMP,
  design_name varchar not null,
  file_location varchar not null, --note: 'directory location'
  file_name varchar not null --note: 'file name'
);

CREATE TABLE sales_order (
  sales_order_id SERIAL PRIMARY KEY  NOT NULL,
  created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  last_updated timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  design_id int REFERENCES design(design_id) NOT NULL,
  staff_id int REFERENCES staff(staff_id) not null,
  counterparty_id int REFERENCES counterparty(counterparty_id) not null,
  units_sold int not null, --note: 'value 1000 - 100000'
  unit_price numeric not null, -- note: 'value 2.00 - 4.00'
  currency_id int REFERENCES currency(currency_id) not null,
  agreed_delivery_date varchar not null, -- note: 'format is yyyy-mm-dd'
  agreed_payment_date varchar not null, --note: 'format is yyyy-mm-dd'
  agreed_delivery_location_id int REFERENCES address(address_id) not null
);

INSERT INTO currency
(currency_id , currency_code , created_at, last_updated)
VALUES 
(2, 'USD', '2022-11-03 14:20:49.962', '2022-11-03 14:20:49.962'),
(3, 'EUR', '2022-11-03 14:20:49.962', '2022-11-03 14:20:49.962');

INSERT INTO address 
(address_id, address_line_1, address_line_2, district, city, postal_code, country, phone, created_at, last_updated)
VALUES 
(16, '511 Orin Extension', 'Cielo Radial', 'Buckinghamshire', 'South Wyatt', '04524-5341' , 'Iceland', '2372 180716', '2022-11-03 14:20:49.962',' 2022-11-03 14:20:49.962'),
(8, '0579 Durgan Common','' , 'Suffolk', '', '56693-0660', 'United Kingdom', '8935 157571', '2022-11-03 14:20:49.962', '2022-11-03 14:20:49.962'),
(19, '0283 Cole Corner', 'Izabella Ways', 'Buckinghamshire', 'West Briellecester', '01138', 'Sierra Leone', '1753 158314', '2022-11-03 14:20:49.962', '2022-11-03 14:20:49.962'),
(29, '37736 Heathcote Lock', 'Noemy Pines', '', 'Bartellview', '42400-5199', 'Congo', '1684 702261', '2022-11-03 14:20:49.962', '2022-11-03 14:20:49.962'),
(15 , '605 Haskell Trafficway', 'Axel Freeway', '', 'East Bobbie', '88253-4257', 'Heard Island and McDonald Islands' , '9687 937447' , '2022-11-03 14:20:49.962' , '2022-11-03 14:20:49.962'),
(14 , '84824 Bryce Common', 'Grady Turnpike', '', 'Maggiofurt', '50899-1522', 'Iraq', '3316 955887', '2022-11-03 14:20:49.962', '2022-11-03 14:20:49.962');

INSERT INTO counterparty
(counterparty_id, counterparty_legal_name, legal_address_id, commercial_contact, delivery_contact, created_at, last_updated)
VALUES 
(8,' Grant - Lakin', 16, 'Emily Orn', 'Veronica Fay', '2022-11-03 14:20:51.563', '2022-11-03 14:20:51.563'),
(4, 'Kohler Inc',  29 , 'Taylor Haag', 'Alfredo Cassin II', '2022-11-03 14:20:51.563' , '2022-11-03 14:20:51.563'),
(16, 'Hartmann, Franecki and Ratke', 14, 'Alberta Franecki DDS', 'Tracy Quigley', '2022-11-03 14:20:51.563', '2022-11-03 14:20:51.563');


INSERT INTO department
(department_id, department_name, location, manager, created_at, last_updated)
VALUES 
(2, 'Purchasing', 'Manchester', 'Naomi Lapaglia', '2022-11-03 14:20:49.962', '2022-11-03 14:20:49.962')
;

INSERT INTO staff
(staff_id, first_name, last_name, department_id, email_address, created_at, last_updated)
VALUES 
(19, 'Pierre', 'Sauer', 2, 'pierre.sauer@terrifictotes.com',' 2022-11-03 14:20:51.563', '2022-11-03 14:20:51.563'),
(10 , 'Jazmyn', 'Kuhn', 2 , 'jazmyn.kuhn@terrifictotes.com', '2022-11-03 14:20:51.563' , '2022-11-03 14:20:51.563');

INSERT INTO design 
(design_id, created_at, design_name, file_location, file_name, last_updated)
VALUES 
(3,' 2022-11-03 14:20:49.962', 'Steel', '/System',' steel-20210621-13gb.json', '2022-11-03 14:20:49.962'),
(4, '2022-11-03 14:20:49.962', 'Granite', '/usr/local/src', 'granite-20220430-l5fs.json', '2022-11-03 14:20:49.962');

INSERT INTO sales_order 
(sales_order_id, created_at,last_updated, design_id, staff_id,counterparty_id, units_sold, unit_price, 
currency_id, agreed_delivery_date, agreed_payment_date, agreed_delivery_location_id)
VALUES (2, '2022-11-03 14:20:52.186','2022-11-03 14:20:52.186', 3, 19, 8, 42972, 3.94, 2 ,'2022-11-07 ','2022-11-08', 8),
(3, '2022-11-03 14:20:52.188', '2022-11-03 14:20:52.188' , 4 , 10, 4 , 65839, 2.91 , 3 , '2022-11-06' , '2022-11-07', 19),
(4, '2022-11-03 14:20:52.188', '2022-11-03 14:20:52.188', 4, 10, 16, 32069, 3.89, 2, '2022-11-05', '2022-11-07', 15);

