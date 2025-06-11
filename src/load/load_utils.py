import awswrangler as wr
import boto3

from src.utils.db_connection import close_conn, create_conn


def access_files_from_processed_bucket(table_name, bucket_name):
    """
    This function connects to the s3 bucket with transformed data and returns all the information in the given table as a dataframe.

    # Arguments:
        table_name: a string representing the name of the table in the database that we want to extract.
        bucket_name: a string representing the name of the s3 bucket with transformed data to connect to.

    # Returns:
        A dataframe containing the most recent data for the given table in the bucket.

    # Raises:
        RuntimeError: An error occurred during data retrieval.
    """
    try:
        client = boto3.client("s3")
        listing_response = client.list_objects_v2(Bucket=bucket_name, Prefix=table_name)
        object_key = listing_response["Contents"][-1]["Key"]
        path = f"s3://{bucket_name}/{object_key}"
        response = wr.s3.read_parquet([path])
        return response

    except Exception as e:
        raise RuntimeError(f"Retrieval of data from processed bucket failed: {e}")


def load_dim_dates_into_warehouse(df):
    """
    Loads data from a dataframe into the warehouse's dim_dates table.

    # Arguments:
        df: a dataframe representing the contents of the dim_dates table.

    # Returns:
        None.
    """
    conn = create_conn()
    if conn:
        try:
            query = """
            INSERT INTO dim_date
            VALUES
            """

            for _, row in df.iterrows():
                query += f"('{row['date_id']}', {row['year']}, {row['month']}, {row['day']}, {row['day_of_week']}, '{row['day_name']}', '{row['month_name']}', {row['quarter']}), "
            query = query[:-2]
            query += "ON CONFLICT (date_id) DO NOTHING;"

            conn.run(query)

        except Exception as e:
            raise RuntimeError(f"Database query failed: {e}")

        finally:
            close_conn(conn)


def load_dim_staff_into_warehouse(df):
    """
    Loads data from a dataframe into the warehouse's dim_staff table.

    # Arguments:
        df: a dataframe representing the contents of the dim_staff table.

    # Returns:
        None.
    """
    conn = create_conn()
    if conn:
        try:
            query = """
            INSERT INTO dim_staff
            VALUES
            """

            for _, row in df.iterrows():
                query += f"({row['staff_id']}, $${row['first_name']}$$, $${row['last_name']}$$, '{row['department_name']}', '{row['location']}', $${row['email_address']}$$), "
            query = query[:-2]
            query += " ON CONFLICT (staff_id) DO NOTHING;"

            conn.run(query)

        except Exception as e:
            raise RuntimeError(f"Database query failed: {e}")

        finally:
            close_conn(conn)


def load_dim_location_into_warehouse(df):
    """
    Loads data from a dataframe into the warehouse's dim_location table.

    # Arguments:
        df: a dataframe representing the contents of the dim_location table.

    # Returns:
        None.
    """
    conn = create_conn()
    if conn:
        try:
            query = """
            INSERT INTO dim_location
            VALUES
            """

            for _, row in df.iterrows():
                query += f"({row['location_id']}, $${row['address_line_1']}$$, $${row['address_line_2']}$$, $${row['district']}$$, $${row['city']}$$, $${row['postal_code']}$$, $${row['country']}$$, $${row['phone']}$$), "
            query = query[:-2]
            query += " ON CONFLICT (location_id) DO NOTHING;"

            conn.run(query)

        except Exception as e:
            raise RuntimeError(f"Database query failed: {e}")

        finally:
            close_conn(conn)


def load_dim_currency_into_warehouse(df):
    """
    Loads data from a dataframe into the warehouse's dim_currency table.

    # Arguments:
        df: a dataframe representing the contents of the dim_currency table.

    # Returns:
        None.
    """
    conn = create_conn()
    if conn:
        try:
            query = """
            INSERT INTO dim_currency
            VALUES
            """

            for _, row in df.iterrows():
                query += f"({row['currency_id']}, $${row['currency_code']}$$, $${row['currency_name']}$$), "
            query = query[:-2]
            query += " ON CONFLICT (currency_id) DO NOTHING;"

            conn.run(query)

        except Exception as e:
            raise RuntimeError(f"Database query failed: {e}")

        finally:
            close_conn(conn)


def load_dim_design_into_warehouse(df):
    """
    Loads data from a dataframe into the warehouse's dim_design table.

    # Arguments:
        df: a dataframe representing the contents of the dim_design table.

    # Returns:
        None.
    """
    conn = create_conn()
    if conn:
        try:
            query = """
            INSERT INTO dim_design
            VALUES
            """

            for _, row in df.iterrows():
                query += f"({row['design_id']}, $${row['design_name']}$$, $${row['file_location']}$$, $${row['file_name']}$$), "
            query = query[:-2]
            query += " ON CONFLICT (design_id) DO NOTHING;"

            conn.run(query)

        except Exception as e:
            raise RuntimeError(f"Database query failed: {e}")

        finally:
            close_conn(conn)


def load_dim_counterparty_into_warehouse(df):
    """
    Loads data from a dataframe into the warehouse's dim_counterparty table.

    # Arguments:
        df: a dataframe representing the contents of the dim_counterparty table.

    # Returns:
        None.
    """
    conn = create_conn()
    if conn:
        try:
            query = """
            INSERT INTO dim_counterparty
            VALUES
            """

            for _, row in df.iterrows():
                query += f"({row['counterparty_id']}, $${row['counterparty_legal_name']}$$, $${row['counterparty_legal_address_line_1']}$$, $${row['counterparty_legal_address_line_2']}$$, $${row['counterparty_legal_district']}$$, $${row['counterparty_legal_city']}$$, $${row['counterparty_legal_postal_code']}$$, $${row['counterparty_legal_country']}$$, $${row['counterparty_legal_phone']}$$), "
            query = query[:-2]
            query += " ON CONFLICT (counterparty_id) DO NOTHING;"

            conn.run(query)

        except Exception as e:
            raise RuntimeError(f"Database query failed: {e}")

        finally:
            close_conn(conn)


def load_fact_sales_order_into_warehouse(df):
    """
    Loads data from a dataframe into the warehouse's fact_sales_order table. Updates to a sales order are stored as new rows, without overwriting previous data.

    # Arguments:
        df: a dataframe representing the contents of the fact_sales_order table.

    # Returns:
        None.
    """
    conn = create_conn()
    if conn:
        try:
            temp_create_query = """
            CREATE TEMP TABLE temp_sales_order (
                sales_record_id SERIAL PRIMARY KEY NOT NULL,
                sales_order_id INT NOT NULL,
                created_date DATE NOT NULL,
                created_time TIME NOT NULL DEFAULT CURRENT_TIME,
                last_updated_date DATE NOT NULL,
                last_updated_time TIME NOT NULL DEFAULT CURRENT_TIME,
                sales_staff_id INT NOT NULL,
                counterparty_id INT NOT NULL,
                units_sold INT NOT NULL,
                unit_price NUMERIC(10, 2) NOT NULL,
                currency_id INT NOT NULL,
                design_id INT NOT NULL,
                agreed_payment_date DATE NOT NULL,
                agreed_delivery_date DATE NOT NULL,
                agreed_delivery_location_id INT NOT NULL
                );
            """

            temp_insert_query = """
            INSERT INTO temp_sales_order
            (sales_order_id, created_date, created_time, last_updated_date, last_updated_time, sales_staff_id, counterparty_id, units_sold, unit_price, currency_id, design_id, agreed_payment_date, agreed_delivery_date, agreed_delivery_location_id)
            VALUES
            """

            for _, row in df.iterrows():
                temp_insert_query += f"({row['sales_order_id']}, '{row['created_date']}', $${row['created_time']}$$, '{row['last_updated_date']}', $${row['last_updated_time']}$$, {row['sales_staff_id']}, {row['counterparty_id']}, {row['units_sold']}, {row['unit_price']}, {row['currency_id']}, {row['design_id']}, '{row['agreed_payment_date']}', '{row['agreed_delivery_date']}', {row['agreed_delivery_location_id']}), "

            temp_insert_query = temp_insert_query[:-2]
            temp_insert_query += ";"

            query = """
            INSERT INTO fact_sales_order
            (sales_order_id, created_date, created_time, last_updated_date, last_updated_time, sales_staff_id, counterparty_id, units_sold, unit_price, currency_id, design_id, agreed_payment_date, agreed_delivery_date, agreed_delivery_location_id)

            SELECT
            sales_order_id, created_date, created_time, last_updated_date, last_updated_time, sales_staff_id, counterparty_id, units_sold, unit_price, currency_id, design_id, agreed_payment_date, agreed_delivery_date, agreed_delivery_location_id
            FROM temp_sales_order

            WHERE
            (sales_order_id, created_date, created_time, last_updated_date, last_updated_time, sales_staff_id, counterparty_id, units_sold, unit_price, currency_id, design_id, agreed_payment_date, agreed_delivery_date, agreed_delivery_location_id)

            NOT IN
            (SELECT
            sales_order_id, created_date, created_time, last_updated_date, last_updated_time, sales_staff_id, counterparty_id, units_sold, unit_price, currency_id, design_id, agreed_payment_date, agreed_delivery_date, agreed_delivery_location_id
            FROM fact_sales_order)
            ;
            """
            conn.run(temp_create_query)
            conn.run(temp_insert_query)
            conn.run(query)

        except Exception as e:
            raise RuntimeError(f"Database query failed: {e}")

        finally:
            close_conn(conn)
