import mysql.connector
from mysql.connector import Error
import time
from helper import configure_settings, write_log_file, value_to_float
import sys

# pyinstaller command: pyinstaller --onefile --name=GoldPriceCalculation main.py

class UpdateData:
    def __init__(self):
        # settings
        self.config = configure_settings()
        self.host = self.config['host']
        self.database = self.config['database']
        self.user = self.config['user']
        self.password = self.config['password']
        self.check_time = self.config['check_time']
        self.store_markup = (self.config['markup'] / 100) + 1
        self.weight_table_name = self.config['weight_table_name']

        self.mysql_conn = None
        self.last_changes = 0
        self.is_mysql_connected = False
        self.connect_mysql()
        self.add_last_update_column()


    def connect_mysql(self):
        try:
            self.mysql_conn = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
            )
        except Error as e:
            write_log_file(f"Can't connect to the MySQL. {e}")
            self.is_mysql_connected = False
            return False
        else:
            self.is_mysql_connected = True
            return True

    def add_last_update_column(self):
        try:
            mysql_cursor = self.mysql_conn.cursor()
            # Check if column already exists
            check_query = """
SELECT COUNT(*) 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = %s 
AND TABLE_NAME = %s 
AND COLUMN_NAME = 'pur_last_update'
"""

            mysql_cursor.execute(check_query, (self.database, 'doc_purchases'))
            column_exists = mysql_cursor.fetchone()[0]

            if column_exists > 0:
                write_log_file("Column 'pur_last_update' already exists!")
                return

            # Add the last_update column
            alter_query = """
ALTER TABLE doc_purchases 
ADD COLUMN pur_last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
ON UPDATE CURRENT_TIMESTAMP
"""

            mysql_cursor.execute(alter_query)
            self.mysql_conn.commit()
            write_log_file("Column 'last_update' added successfully!")

        except mysql.connector.Error as err:
            write_log_file(f"Error: {err}")
            self.mysql_conn.rollback()


    def check_mysql_changes(self):
        try:
            mysql_cursor = self.mysql_conn.cursor()
            mysql_cursor.execute("RESET QUERY CACHE")
            query_check_item = """
SELECT pur_last_update, pur_id, pur_performed, pur_object FROM easytrade_db.doc_purchases 
ORDER BY pur_last_update DESC
LIMIT 1
"""
            mysql_cursor.execute(query_check_item)
            last_changed_document = mysql_cursor.fetchone()

        except Error as e:
            write_log_file(f"Can't connect to the MySQL. {e}")
            self.is_mysql_connected = False
            return {"ok": False, "error": f"Can't connect to the MySQL. {e}"}

        else:
            if not last_changed_document:
                write_log_file("No data was found!")
                return {"ok": False, "error": "No data was found!"}

            if last_changed_document[2] == 0:
                return {"ok": False, "error": "Document wasn't performed!"}

            timestamp_last_operation = last_changed_document[0].timestamp()
            document_id = last_changed_document[1]
            object_id = last_changed_document[3]

            if self.last_changes < timestamp_last_operation:
                self.last_changes = timestamp_last_operation
                mysql_cursor.close()
                return {"ok": True, "document_id": document_id, "object_id": object_id}

            else:
                mysql_cursor.close()
                return {"ok": False, "error": "Document wasn't changed!"}

    def fetch_doc_data(self, document_id: int, cur) -> dict:
        try:
            cur.execute("RESET QUERY CACHE")
            query_get_goods = f"""
SELECT 
    o.opr_id,
    o.opr_good,
    o.opr_quantity,
    o.opr_document,
    g.{self.weight_table_name},
    v.vat_value,
    g.gd_name
FROM easytrade_db.operations o
LEFT JOIN dir_goods g ON o.opr_good = g.gd_id
LEFT JOIN dir_vat v ON g.gd_vat = v.vat_id
WHERE o.opr_type = 1 AND o.opr_document = %s
"""
            cur.execute(query_get_goods, (document_id, ))
            result = cur.fetchall()

        except Error as e:
            error_msg = f"Error {e}"
            write_log_file(error_msg )
            self.is_mysql_connected = False
            return {"ok": False, "error": error_msg}

        else:
            return {"ok": True, "data": result}

    def get_gold_price(self, cur):
        try:
            query = " SELECT cur_exchange_rate FROM easytrade_db.dir_currency WHERE cur_code = 'USD'"
            cur.execute("RESET QUERY CACHE")
            cur.execute(query)
            result = cur.fetchone()
            if not result:
                return {"ok": False, "error": "No data was found!"}
        except Error as e:
            error_msg = f"Error {e}"
            write_log_file(error_msg)
            return {"ok": False, "error": error_msg}

        else:
            return {"ok": True, "data": result[0]}


    def get_price_type(self, object_id, cur):
        try:
            query = "SELECT obj_price_type FROM easytrade_db.dir_objects WHERE obj_id = %s"
            cur.execute(query, (object_id,))
            result = cur.fetchone()
            return {"ok": True, "price_type": result[0]}

        except Error as e:
            error_msg = f"Error {e}"
            write_log_file(error_msg)
            return {"ok": False, "error": error_msg}


    def update_operation_prop(self, oap_cost, oap_cost_cur, oap_exchange_rate,
                              oap_price1, oap_price2, oap_operation, cur):
        query = """
UPDATE operations_additional_prop
SET oap_cost = %s,
    oap_cost_cur = %s,
    oap_exchange_rate = %s,
    oap_price1 = %s,
    oap_price2 = %s
WHERE oap_operation = %s
"""
        cur.execute(query, (oap_cost, oap_cost_cur, oap_exchange_rate, oap_price1, oap_price2, oap_operation))

    def update_or_insert_prices(self, prc_value, prc_value_cur, prc_type, prc_good, cur):
        query_update = """
UPDATE dir_prices
SET prc_value = %s,
    prc_value_cur = %s,
    prc_recalculate = 1
WHERE prc_id = %s
"""
        query_insert = """
INSERT INTO dir_prices (prc_type, prc_good, prc_value, prc_value_cur, prc_recalculate, prc_deleted)
VALUES (%s, %s, %s, %s, %s, %s)
"""
        query_select = "SELECT prc_id FROM dir_prices WHERE prc_type = %s AND prc_good = %s"
        cur.execute(query_select, (prc_type, prc_good))
        price_info = cur.fetchone()
        if not price_info:
            cur.execute(query_insert, (prc_type, prc_good, prc_value, prc_value_cur, 1, 0))
        else:
            price_id = price_info[0]
            cur.execute(query_update, (prc_value, prc_value_cur, price_id))

    def update_cost(self, avgc_good: int, avgc_object: int, avgc_value: float, avgc_value_cur: float, cur):
        cur.execute(
            "UPDATE dir_avg_cost SET avgc_object = %s, avgc_value = %s, avgc_value_cur = %s WHERE avgc_good = %s",
            (avgc_object, avgc_value, avgc_value_cur, avgc_good)
        )

    def update_prices_and_costs(self):
        try:
            mysql_cursor = self.mysql_conn.cursor()

            check_data = self.check_mysql_changes()

            if not check_data.get("ok"):
                write_log_file(check_data.get("error", "Something went wrong!"))
                return {"ok": False, "error": check_data.get("error", "Something went wrong!")}

            doc_id = check_data["document_id"]
            object_id = check_data["object_id"]

            result = self.fetch_doc_data(doc_id, cur=mysql_cursor)
            if not result.get("ok"):
                return {"ok": False, "error": result.get("error")}

            gold_price_data = self.get_gold_price(cur=mysql_cursor)
            if not gold_price_data.get("ok"):
                return {"ok": False, "error": gold_price_data.get("error")}

            gold_exchange_rate = gold_price_data["data"]

            document_data = result.get("data")

            price_type_data = self.get_price_type(object_id, cur=mysql_cursor)
            if not price_type_data.get("ok"):
                write_log_file(price_type_data.get("error"))
                return {"ok": False, "error": price_type_data.get("error")}

            price_type_id = price_type_data.get("price_type")

            # Prepare batch updates
            operation_updates = []
            price_updates = []
            cost_updates = []

            for row in document_data:
                operation_id = row[0]
                operation_good = row[1]
                weight = value_to_float(s=row[4])
                vendor_markup = float(row[5])

                # fast math (mul instead of div)
                gold_cost_with_markup = weight * (1 + vendor_markup * 0.01)
                gold_cost_in_local_currency = gold_cost_with_markup * gold_exchange_rate
                gold_price = gold_cost_with_markup * self.store_markup
                gold_price_local_currency = gold_price * gold_exchange_rate

                # Collect data for bulk update
                operation_updates.append((
                    gold_cost_in_local_currency,
                    gold_cost_with_markup,
                    gold_exchange_rate,
                    gold_price_local_currency,
                    gold_price,
                    operation_id
                ))

                price_updates.append((
                    price_type_id,
                    operation_good,
                    gold_price_local_currency,
                    gold_price
                ))

                cost_updates.append((
                    gold_cost_in_local_currency,
                    gold_cost_with_markup,
                    object_id,
                    operation_good
                ))

            # Bulk updates
            mysql_cursor.executemany("""
                UPDATE operations_additional_prop
                SET oap_cost=%s, oap_cost_cur=%s, oap_exchange_rate=%s, oap_price1=%s, oap_price2=%s
                WHERE oap_operation=%s
            """, operation_updates)

            mysql_cursor.executemany("""
                INSERT INTO dir_prices (prc_type, prc_good, prc_value, prc_value_cur, prc_recalculate, prc_deleted)
                VALUES (%s, %s, %s, %s, 1, 0)
                ON DUPLICATE KEY UPDATE
                    prc_value = VALUES(prc_value),
                    prc_value_cur = VALUES(prc_value_cur),
                    prc_recalculate = 1
            """, price_updates)

            mysql_cursor.executemany("""
                UPDATE dir_avg_cost
                SET avgc_value=%s, avgc_value_cur=%s
                WHERE avgc_object=%s AND avgc_good=%s
            """, cost_updates)

            # Commit once
            self.mysql_conn.commit()
            write_log_file(f"Price updated successfully! - doc: {doc_id}")

        except Error as e:
            error_msg = f"Error {e}"
            write_log_file(error_msg)
            self.mysql_conn.rollback()

        else:
            mysql_cursor.close()


update_data = UpdateData()

while True:
    try:
        if not update_data.is_mysql_connected:
            update_data.connect_mysql()

        else:
            update_data.update_prices_and_costs()

        time.sleep(update_data.check_time)
    except Exception as e:
        write_log_file(f"Service error: {str(e)}")
        time.sleep(60)  # Wait a bit before retrying if there's an error
