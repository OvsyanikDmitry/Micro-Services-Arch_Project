import os
from datetime import datetime
from lib.pg import PgConnect
from datetime import datetime
from lib.pg import PgConnect



pg_warehouse_host = str(os.getenv('PG_WAREHOUSE_HOST') or "")
pg_warehouse_port = int(str(os.getenv('PG_WAREHOUSE_PORT') or 0))
pg_warehouse_dbname = str(os.getenv('PG_WAREHOUSE_DBNAME') or "")
pg_warehouse_user = str(os.getenv('PG_WAREHOUSE_USER') or "")
pg_warehouse_password = str(os.getenv('PG_WAREHOUSE_PASSWORD') or "")



def pg_warehouse_db():
    return PgConnect(
        pg_warehouse_host,
        pg_warehouse_port,
        pg_warehouse_dbname,
        pg_warehouse_user,
        pg_warehouse_password
    )


class CdmRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db

    def update_mart(mart_name: str,
                   columns: list,
                   values: list,
                   ) -> None:
        print(f'...загрузка строк в витрину {mart_name}')
        with pg_warehouse_db().connection() as conn:
            values_ = []
            for v in values:
                v_ = f"'{str(v)}'"
                values_.append(v_)
            with conn.cursor() as cur:
                query = f"""
                        INSERT INTO cdm.{mart_name}
                        ({','.join(columns)})
                        VALUES({','.join(values_)})
                        ON CONFLICT ({columns[0]}, {columns[1]})
                        DO UPDATE 
                        SET order_cnt= cdm.{mart_name}.order_cnt + EXCLUDED.order_cnt;

                    """
                print(query)
                cur.execute(query)
            conn.commit()
            """Метод для обновления таблицы витрины. Метод принимает параметры, 
            такие как название витрины, список столбцов и список значений. 
            Затем в методе производится подключение к базе данных и формируется запрос на вставку новых данных в таблицу витрины.
              Если строки, которые нужно вставить, уже присутствуют в таблице витрины, 
            то выполняется обновление столбца order_cnt (количества заказов) таблицы витрины"""