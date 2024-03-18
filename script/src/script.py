# %% [markdown]
# Maak de connectie aan tot de server.

# %%
import pandas as pd
import pyodbc
import sqlite3 as sql
from datetime import datetime
from settings import settings, logger



    

def process():
    DB = {
        'servername': r'LAPTOP-1LCT01QI\SQLEXPRESS',
        'database': 'GreatOutdoors'
    }

    # Establish the connection
    export_conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + DB['servername'] + ';DATABASE=' + DB['database'] + ';Trusted_Connection=yes')
    export_cursor = export_conn.cursor()

    def get_surrogate_key(table, primary_key_name, primary_key_value):
        if(primary_key_value == None):
            return None

        export_cursor.execute(f"SELECT MAX(SURROGATE_KEY) FROM {table} WHERE {primary_key_name} = ?", (primary_key_value,))
        key_result = export_cursor.fetchone()
        max_key = key_result[0] if key_result[0] is not None else -1
        return max_key


    go_crm_connection = sql.connect(str(settings.processeddir / 'go_crm.sqlite'))
    go_sales_connection = sql.connect(str(settings.processeddir / 'go_sales.sqlite'))
    go_staff_connection = sql.connect(str(settings.processeddir / 'go_staff.sqlite'))


    product_sql = pd.read_sql_query("SELECT * from product", go_sales_connection)
    product_type_sql = pd.read_sql_query("SELECT * from product_type", go_sales_connection)
    product_line_sql = pd.read_sql_query("SELECT * from product_line", go_sales_connection)

    product = pd.DataFrame(product_sql)
    product_type = pd.DataFrame(product_type_sql)
    product_line = pd.DataFrame(product_line_sql)

    productmrg2 = pd.merge(product_type, product_line, on='PRODUCT_LINE_CODE')
    productmrg = pd.merge(productmrg2, product, on='PRODUCT_TYPE_CODE')


    for index, row in productmrg.iterrows():
        try:
            query = (
                "INSERT INTO Product(PRODUCT_NUMBER, PRODUCT_IMAGE, PRODUCT_DESCRIPTION, PRODUCT_INTRODUCTION_DATE, PRODUCT_TYPE_CODE, PRODUCT_LANGUAGE, PRODUCT_TYPE_EN, PRODUCT_LINE_CODE, PRODUCT_LINE_EN, PRODUCT_COST, PRODUCT_MARGIN) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )

            date_str = row['INTRODUCTION_DATE']  # The date string from your row
            correct_format_date = datetime.strptime(date_str, '%d-%m-%Y').strftime('%Y-%m-%d')

            values = (
               int(row['PRODUCT_NUMBER']),
                row['PRODUCT_IMAGE'],
                row['DESCRIPTION'],
                correct_format_date,
                row['PRODUCT_TYPE_CODE'],
                row['LANGUAGE'],
                row['PRODUCT_TYPE_EN'],
                row['PRODUCT_LINE_CODE'],
                row['PRODUCT_LINE_EN'],
                row['PRODUCTION_COST'],
                row['MARGIN']
            )

            export_cursor.execute(query, values)

        except Exception as e:
            print(" ")
            print("Exception: ", str(e))
            print("Query: ", str(query.count('?')) + " " + str(query))
            print("Values: ", str(len(values)) + " " + str(values))

    export_conn.commit()



    # %% [markdown]
    # Retailer

    # %%
    country_sql = pd.read_sql_query("SELECT * from country", go_crm_connection)
    country_sql2 = pd.read_sql_query("SELECT * from country", go_sales_connection)

    retailer_site_sql = pd.read_sql_query("SELECT * FROM retailer_site", go_crm_connection)
    retailer_sql = pd.read_sql_query("SELECT * FROM retailer", go_crm_connection)
    retailer_segment_sql = pd.read_sql_query("SELECT * FROM retailer_segment", go_crm_connection)
    retailer_headquarters_sql = pd.read_sql_query("SELECT * FROM retailer_headquarters", go_crm_connection)

    country = pd.DataFrame(country_sql)
    country2 = pd.DataFrame(country_sql2)
    retailer_site = pd.DataFrame(retailer_site_sql)
    retailer = pd.DataFrame(retailer_sql)
    retailer_segment = pd.DataFrame(retailer_segment_sql)
    retailer_headquarters = pd.DataFrame(retailer_headquarters_sql)

    retailer_site = retailer_site.rename(columns={
        'ADDRESS1': 'RETAILER_SITE_ADDRESS1',
        'ADDRESS2': 'RETAILER_SITE_ADDRESS2',
        'CITY': 'RETAILER_SITE_CITY',
        'REGION': 'RETAILER_SITE_REGION',
        'POSTAL_ZONE': 'RETAILER_SITE_POSTAL_ZONE',
        'COUNTRY_CODE': 'RETAILER_SITE_COUNTRY_CODE'
    })

    retailer_headquarters = retailer_headquarters.rename(columns={
        'ADDRESS1': 'RETAILER_HEADQUARTERS_ADDRESS1',
        'ADDRESS2': 'RETAILER_HEADQUARTERS_ADDRESS2',
        'CITY': 'RETAILER_HEADQUARTERS_CITY',
        'REGION': 'RETAILER_HEADQUARTERS_REGION',
        'POSTAL_ZONE': 'RETAILER_HEADQUARTERS_POSTAL_ZONE',
        'COUNTRY_CODE': 'RETAILER_HEADQUARTERS_COUNTRY_CODE'
    })


    retailer_segment = retailer_segment.rename(columns={
        'LANGUAGE': 'RETAILER_SEGMENT_LANGUAGE',
    })

    c = pd.merge(country, country2)
    rs = pd.merge(retailer_site, retailer, on='RETAILER_CODE', how='left')
    rsh = pd.merge(rs, retailer_headquarters, on='RETAILER_CODEMR', how='left')
    rshs = pd.merge(rsh, retailer_segment, on='SEGMENT_CODE', how='left')
    rshsc = pd.merge(rshs, c, left_on='RETAILER_SITE_COUNTRY_CODE', right_on='COUNTRY_CODE')


    #          row['FAX'],
    # #             row['SEGMENT_CODE'],
    # #             row['SEGMENT_NAME'],
    # #             row['SEGMENT_DESCRIPTION'],
    # #             row['RETAILER_SEGMENT_LANGUAGE']
    # table_name = 'Retailer'
    # sql_query = f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'"
    # sql_data_types = pd.read_sql(sql_query, export_conn)


    rshsc['RETAILER_SITE_CODE'] = rshsc['RETAILER_SITE_CODE'].fillna(-1).astype(int)
    rshsc['RETAILER_CODE'] = rshsc['RETAILER_CODE'].fillna(-1).astype(int)
    rshsc['RETAILER_CODEMR'] = rshsc['RETAILER_CODEMR'].fillna(-1).astype(int)

    df = rshsc.where(pd.notnull(rshsc), None)

    for index, row in df.iterrows():
        try:

            query = (
                """
                INSERT INTO Retailer(
                    SURROGATE_DATE, 
                    RETAILER_CODE,
                    RETAILER_CODEMR,
                    RETAILER_COMPANY_NAME,
                    RETAILER_HEADQUARTERS_NAME,
                    RETAILER_SITE_CODE,
                    RETAILER_ACTIVE_INDICATOR,
                    RETAILER_COUNTRY_EN,
                    COUNTRY_FLAG_IMAGE,
                    COUNTRY_CURRENCY_NAME,
                    RETAILER_SITE_REGION,
                    RETAILER_SITE_ADDRESS1,
                    RETAILER_SITE_ADDRESS2,
                    RETAILER_SITE_POSTAL_ZONE,
                    RETAILER_SITE_CITY,
                    RETAILER_PHONE,
                    RETAILER_FAX,
                    RETAILER_SEGMENT_CODE,
                    RETAILER_SEGMENT_NAME,
                    RETAILER_SEGMENT_DESCRIPTION,
                    RETAILER_SEGMENT_LANGUAGE
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            )

            RETAILER_CODEMR = int(row['RETAILER_CODEMR']) if str(row['RETAILER_CODEMR']).isdigit() else None
            RETAILER_SITE_CODE = int(row['RETAILER_SITE_CODE']) if str(row['RETAILER_SITE_CODE']).isdigit() else 0


            values = (
                datetime.now().strftime('%Y-%m-%d'),
                int(row['RETAILER_CODE']),
                row['RETAILER_CODEMR'],
                row['COMPANY_NAME'],
                row['RETAILER_NAME'],
                int(row['RETAILER_SITE_CODE']),
                int(row['ACTIVE_INDICATOR']),
                row['COUNTRY_EN'],
                row['FLAG_IMAGE'],
                row['CURRENCY_NAME'],
                row['RETAILER_SITE_REGION'],
                row['RETAILER_SITE_ADDRESS1'],
                row['RETAILER_SITE_ADDRESS2'],
                row['RETAILER_SITE_POSTAL_ZONE'],
                row['RETAILER_SITE_CITY'],
                row['PHONE'],
                row['FAX'],
                row['SEGMENT_CODE'],
                row['SEGMENT_NAME'],
                row['SEGMENT_DESCRIPTION'],
                row['RETAILER_SEGMENT_LANGUAGE']
            )

            export_cursor.execute(query, values)


        except Exception as e:
            print(row['COUNTRY_EN'], row['RETAILER_SITE_COUNTRY_CODE'])
            print(" ")
            print("Exception: ", str(e))
            print("Query: ", str(query.count('?')) + " " + str(query))
            print("Values: ", str(len(values)) + " " + str(values))

    export_conn.commit()







    # %% [markdown]
    # 

    # %% [markdown]
    # Order_method

    # %%
    order_method_sql = pd.read_sql_query("SELECT * from order_method", go_sales_connection)

    order_method = pd.DataFrame(order_method_sql)



    for index, row in order_method.iterrows():
        try:
            querymeth = (
                "INSERT INTO Order_method(ORDER_METHOD_CODE, ORDER_METHOD_EN) VALUES (?, ?)"
            )

            valuesmeth = (
                row['ORDER_METHOD_CODE'],
                row['ORDER_METHOD_EN']
            )

            export_cursor.execute(querymeth, valuesmeth)

        except Exception as e:
            print(" ")
            print("Exception: ", str(e))
            print("Query: ", str(query.count('?')) + " " + str(query))
            print("Values: ", str(len(values)) + " " + str(values))

    export_conn.commit()



    # %% [markdown]
    # Sales_staff

    # %%
    country_sql = pd.read_sql_query("SELECT * from country", go_crm_connection)
    country_sql2 = pd.read_sql_query("SELECT * from country", go_sales_connection)
    sales_staff_sql = pd.read_sql_query("SELECT * FROM sales_staff", go_staff_connection)
    sales_branch_sql = pd.read_sql_query("SELECT * from sales_branch", go_sales_connection)

    country = pd.DataFrame(country_sql)
    country2 = pd.DataFrame(country_sql2)
    sales_staff = pd.DataFrame(sales_staff_sql)
    sales_branch = pd.DataFrame(sales_branch_sql)

    c = pd.merge(country, country2)
    sb = pd.merge(sales_branch, sales_staff)
    sbc = pd.merge(sb, c)

    sbc['SALES_STAFF_FULL_NAME'] = sbc['FIRST_NAME'] + ' ' + sbc['LAST_NAME']


    sbc.fillna({'MANAGER_CODE': 0, 'SALES_BRANCH_CODE': 0}, inplace=True)


    for index, row in sbc.iterrows():
        try:

            query = (
                "INSERT INTO Sales_staff ("
                "SALES_STAFF_CODE, "
                "SALES_STAFF_FULL_NAME, "
                "SALES_STAFF_FNAME, "
                "SALES_STAFF_LNAME, "
                "SALES_STAFF_PHONE, "
                "SALES_STAFF_EXTENSION, "
                "SALES_STAFF_FAX, "
                "SALES_STAFF_EMAIL, "
                "SALES_BRANCH_CODE, "
                "SALES_BRANCH_COUNTRY_CODE, "
                "SALES_BRANCH_LANGUAGE, "
                "SALES_BRANCH_CURRENCY_NAME, "
                "SALES_BRANCH_COUNTRY_NAME, "
                "SALES_BRANCH_FLAG_IMAGE, "
                "SALES_BRANCH_REGION, "
                "SALES_BRANCH_CITY, "
                "SALES_BRANCH_POSTAL_ZONE, "
                "SALES_BRANCH_ADDRESS1, "
                "SALES_MANAGER_CODE, "
                "SALES_MANAGER_SURROGATE_KEY"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            )

            staff_surrogate_key = get_surrogate_key('Sales_staff', 'SALES_MANAGER_CODE', row['MANAGER_CODE'])

            values = (
                int(row['SALES_STAFF_CODE']),
                row['SALES_STAFF_FULL_NAME'],
                row['FIRST_NAME'],
                row['LAST_NAME'],
                row['WORK_PHONE'],
                row['EXTENSION'],
                row['FAX'],
                row['EMAIL'],
                int(row['SALES_BRANCH_CODE']),
                int(row['COUNTRY_CODE']),
                row['LANGUAGE'],
                row['CURRENCY_NAME'],
                row['COUNTRY_EN'],
                row['FLAG_IMAGE'],
                row['REGION'],
                row['CITY'],
                row['POSTAL_ZONE'],
                row['ADDRESS1'],
                int(row['MANAGER_CODE']),
                staff_surrogate_key
            )

            export_cursor.execute(query, values)
        except Exception as e:
            print(" ")
            print("Exception: ", str(e))
            print("Query: ", str(query.count('?')) + " " + str(query))
            print("Values: ", str(len(values)) + " " + str(values))




    # %% [markdown]
    # sales_demographic

    # %%
    sales_demo_sql = pd.read_sql_query("SELECT * from sales_demographic", go_crm_connection)
    age_group_sql = pd.read_sql_query("SELECT * from age_group", go_crm_connection)

    sales_demo = pd.DataFrame(sales_demo_sql)
    age_group = pd.DataFrame(age_group_sql)

    sales_demo_merge = pd.merge(sales_demo, age_group, on='AGE_GROUP_CODE')



    for index, row in sales_demo_merge.iterrows():
        try:

            query = (
                "INSERT INTO Sales_demographic(DEMOGRAPHIC_ID, UPPER_AGE, LOWER_AGE, AGE_GROUP_CODE, RETAILER_CODEMR, SALES_PERCENTAGE, RETAILER_SURROGATE_KEY) VALUES (?, ?, ?, ?, ?, ?, ?)"
            )

            retailer_surrogate_key = get_surrogate_key('Retailer', 'RETAILER_CODEMR', row['RETAILER_CODEMR']) 

            values = (
                int(row['DEMOGRAPHIC_CODE']),
                int(row['UPPER_AGE']),
                int(row['LOWER_AGE']),
                row['AGE_GROUP_CODE'],
                int(row['RETAILER_CODEMR']),
                row['SALES_PERCENT'],
                retailer_surrogate_key
            )

            export_cursor.execute(query, values)

        except Exception as e:
            print(" ")
            print("Exception: ", str(e))
            print("Query: ", str(query.count('?')) + " " + str(query))
            print("Values: ", str(len(values)) + " " + str(values))

    export_conn.commit()

    # %% [markdown]
    # Sales_staff

    # %% [markdown]
    # ORDER_HEADER

    # %%
    order_header_sql = pd.read_sql_query("SELECT * from order_header", go_sales_connection)
    order_details_sql = pd.read_sql_query("SELECT * from order_details", go_sales_connection)

    order_header = pd.DataFrame(order_header_sql)
    order_details = pd.DataFrame(order_details_sql)


    order = pd.merge(order_header, order_details)
    order['ORDER_NUMBER'] = order['ORDER_NUMBER'].astype(int)
    order['ORDER_DETAIL_CODE'] = order['ORDER_DETAIL_CODE'].astype(int)

    order['ORDER_DATE'] = pd.to_datetime(order['ORDER_DATE'])

    order['ORIGINAL_TURN_OVER'] = order['UNIT_PRICE'].astype(float) * order['QUANTITY'].astype(int)

    order['ORDER_TURN_OVER'] = order['UNIT_SALE_PRICE'].astype(float) * order['QUANTITY'].astype(int)
    order['ORDER_PROFIT'] = order['ORDER_TURN_OVER'].astype(float) - (order['QUANTITY'].astype(int) * order['UNIT_COST'].astype(float))
    order['ORDER_DISCOUNT'] = round((order['ORDER_TURN_OVER'] - order['ORIGINAL_TURN_OVER']) / order['ORIGINAL_TURN_OVER'], 2) * -100
    order['ORDER_MARGIN'] = round(order['ORDER_PROFIT'].astype(float) / order['ORDER_TURN_OVER'].astype(float), 2) * 100



    for index, row in order.iterrows():
        try:

            retailer_surrogate_key = get_surrogate_key('Retailer', 'RETAILER_SITE_CODE', row['RETAILER_SITE_CODE']) 
            staff_surrogate_key = get_surrogate_key('Sales_staff', 'SALES_STAFF_CODE', row['SALES_STAFF_CODE'])
            or_surrogate_key = get_surrogate_key('Order_method', 'ORDER_METHOD_CODE', row['ORDER_METHOD_CODE'])
            product_surrogate_key = get_surrogate_key('Product', 'PRODUCT_NUMBER', row['PRODUCT_NUMBER'])

            query = """
                INSERT INTO Order_header (
                    ORDER_NUMBER, 
                    ORDER_DETAIL_CODE, 
                    ORDER_PRODUCT_NUMBER, 
                    ORDER_QUANTITY, 
                    ORDER_UNIT_COST, 
                    ORDER_UNIT_SALE_PRICE,  
                    ORDER_UNIT_PRICE, 
                    ORDER_PROFIT, 
                    ORDER_DISCOUNT, 
                    ORDER_MARGIN, 
                    ORDER_TURN_OVER, 
                    ORDER_DATE, 
                    SALES_BRANCH_CODE, 
                    RETAILER_SITE_CODE, 
                    SALES_STAFF_CODE, 
                    RETAILER_NAME, 
                    ORDER_METHOD_CODE,
                    ORDER_PRODUCT_SURROGATE_KEY,
                    RETAILER_SITE_SURROGATE_KEY,
                    SALES_STAFF_SURROGATE_KEY,
                    ORDER_METHOD_SURROGATE_KEY
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?,?,?,?)
            """

            values = (
                row['ORDER_NUMBER'],
                row['ORDER_DETAIL_CODE'],
                row['PRODUCT_NUMBER'],
                row['QUANTITY'],
                row['UNIT_COST'],
                row['UNIT_SALE_PRICE'],
                row['UNIT_PRICE'],
                row['ORDER_PROFIT'],
                row['ORDER_DISCOUNT'],
                row['ORDER_MARGIN'],
                row['ORDER_TURN_OVER'],
                row['ORDER_DATE'],
                row['SALES_BRANCH_CODE'],
                row['RETAILER_SITE_CODE'],
                row['SALES_STAFF_CODE'],
                row['RETAILER_NAME'],
                row['ORDER_METHOD_CODE'],
                product_surrogate_key,
                retailer_surrogate_key,
                staff_surrogate_key,
                or_surrogate_key
            )

            export_cursor.execute(query, values)

        except Exception as e:
            print(" ")
            print("Exception: ", str(e))
            print("Query: ", str(query.count('?')) + " " + str(query))
            print("Values: ", str(len(values)) + " " + str(values))


    export_conn.commit()
    export_conn.close()
