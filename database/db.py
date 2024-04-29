import json
import mysql.connector
from datetime import datetime


with open("config.json", "r") as f:
    config_DB = f.read()
    config_DB = json.loads(config_DB)

class APP_MySQL:
    def __init__(self):
        self.api = None
        self.conn = None
        self.cursor = None

    def conn_database(self, database):
        
        try:
            # print(" ------------- config_DB ------------- ")
            # print(config_DB)
            # print(type(config_DB))

            conn = mysql.connector.connect(
                host = config_DB["database"][database]["host"],
                user = config_DB["database"][database]["username"],
                password = config_DB["database"][database]["password"],
                database = database,
            )
            print(f" ### SUCCESS CONNECT DATABASE ### ")
            return conn
        except Exception as e:
            print(f""" ERROR CONN DATABASE | ERROR: {e}""")
            return None
        
    def close_conn_database(self, object):
        try:
            object.close()
            object = None
            print(f" ### SUCCESS DISCONNECT DATABASE | object: {object} ### ")
            return True
        except Exception as e:
            print(f" ### ERROR DISCONNECT DATABASE | ERROR: {e} ### ")
            return None
    
    def query_generic(self, database, table_name, list_cols_names, expression_query=" ;"):

        list_columns_name = list()
        self.conn = self.conn_database(database=database)
        result = None
        if self.conn is not None:
            self.cursor = self.conn.cursor()

            self.cursor.execute(f"SHOW COLUMNS from {table_name}")
            columns_table = self.cursor.fetchall()

            if len(columns_table) > 0:
                for col in columns_table:
                    list_columns_name.append(col[0])

            # print("\n\n ------------- columns_table ------------- ")
            # print(list_columns_name)

            if "*" in list_cols_names:
                comand = f"""
                    SELECT * FROM {table_name}{expression_query}
                """
            else:
                list_columns_name = list_cols_names
                comand = f"""
                    SELECT {", ".join(list_cols_names)} FROM {table_name}{expression_query}
                """
            print(comand)

            self.cursor.execute(comand)
            result = self.cursor.fetchall()
            print(f"\n\n\n ### TT RESULT: {len(result)}")

        self.close_conn_database(object=self.conn)
        self.close_conn_database(object=self.cursor)
        return {"result": result, "columns_name": list_columns_name}
    
    def save_lote_IR(self, database, table_name, arr_code_IR, date_payment, dificuldade_lote, status_paymanet_lote, status_smart_IR_lote, info_forma_pagamento):

        list_columns_name = list()
        self.conn = self.conn_database(database=database)


        print(f"\n\n ---------------- conn database | {database}")
        print(self.conn)

        result = None
        try:
            if self.conn is not None:
                self.cursor = self.conn.cursor()

                arr_code_IR = ",".join(arr_code_IR)
                comand = f"""
                    UPDATE {table_name}
                        SET
                            dt_pagamento_IR = "{date_payment}",
                            status_pagamento_IR = "{status_paymanet_lote}",
                            dificuldade = "{dificuldade_lote}",
                            status_smart_IR = "{status_smart_IR_lote}",
                            info_forma_pagamento = "{info_forma_pagamento}"
                        WHERE
                            id in({arr_code_IR});
                    """
                print(comand)
                self.cursor.execute(comand)
                self.conn.commit()

            self.close_conn_database(object=self.conn)
            self.close_conn_database(object=self.cursor)
            return {
                "code": 200,
                "msg": "success update database lote"
            }
        except Exception as e:
            print(e)
            return {
                "code": 400,
                "msg": "error update database lote"
            }
    
    def update_company_code_JB(self, database, table_name, username:str, id_acessorias:str, company_code_JB:str):


        self.conn = self.conn_database(database=database)

        print(f"\n\n ---------------- conn database | {database}")
        print(self.conn)

        result = None
        updated_at = datetime.now()
        try:
            if self.conn is not None:
                self.cursor = self.conn.cursor()

                comand_query = f"""
                    SELECT * from {table_name} WHERE id_acessorias = "{id_acessorias}"
                """
                self.cursor.execute(comand_query)
                result = self.cursor.fetchall()



                msg = None
                if len(result) > 0:
                    comand = f"""
                        UPDATE {table_name}
                            SET
                                updated_by = "{username}",
                                company_code = "{company_code_JB}",
                                updated_at = "{updated_at}"
                            WHERE
                                id > 0 and id_acessorias = {id_acessorias};
                        """
                    print(comand)
                    self.cursor.execute(comand)
                    self.conn.commit()
                    msg = "success updade company code JB"
                else:
                    comand = f"""
                        INSERT INTO {table_name}
                            (id_acessorias, updated_by, company_code, updated_at)
                            VALUES ("{id_acessorias}", "{username}", "{company_code_JB}","{updated_at}")
                        """
                    print(comand)
                    self.cursor.execute(comand)
                    self.conn.commit()
                    msg = "success insert company code JB"

            self.close_conn_database(object=self.conn)
            self.close_conn_database(object=self.cursor)
            return {
                "code": 200,
                "msg": msg
            }   
        except Exception as e:
            print(e)
            return {
                "code": 400,
                "msg": str(e)
            }
    
    
    def query_and_filter_data(self, database, table_name, list_cols_names, date_init, date_final, list_regime, list_type_company):
        self.conn = self.conn_database(database=database)
        result = None
        if self.conn is not None:
            self.cursor = self.conn.cursor()

            # ----------------------------------------------------------------------------------------------
            # ------------------------------------ verificação de datas ------------------------------------
            # ----------------------------------------------------------------------------------------------
            # list_regime = ["Simples", "Lucro Presumido", "Lucro Real", "Outros"]
            # list_type_company = ["MEI", "Normal"]
            
            expression_date = "data_apont <> '*'"
            expression_regime = f" regime_agrup <> '*'"
            expression_type_company = " tipo_empresa <> '*'"

            # date_init = "2024-03-01"
            # date_final = "2024-03-01"
            if date_init != "" and date_final != "":
                expression_date = f"STR_TO_DATE(data_apont, '%d/%m/%Y') >= '{date_init}' and STR_TO_DATE(data_apont, '%d/%m/%Y') <= '{date_final}'"
            elif date_init != "" and date_final == "":
                expression_date = f"STR_TO_DATE(data_apont, '%d/%m/%Y') >= '{date_init}'"
            elif date_init == "" and date_final != "":
                expression_date = f"STR_TO_DATE(data_apont, '%d/%m/%Y') <= '{date_final}'"

            # ----------------------------------------------------------------------
                
            if len(list_regime) > 0:
                expression_regime = f""" regime_agrup regexp "{'|'.join(list_regime)}" """

            # ----------------------------------------------------------------------
                
            if len(list_type_company) > 0:
                expression_type_company = f""" tipo_empresa regexp "{'|'.join(list_type_company)}" """


            expression_mysql = f"{expression_date} and {expression_regime} and {expression_type_company}"

            # list_cols_names = ["id", "data_apont", "horario_inicio", "horario_fim", "competencia", "codigo_empresa", "razao_social", "atividade", "observacao", "username", "setor", "mes", "ano", "tempo", "regime"]
            comand = f"""
                    SELECT {", ".join(list_cols_names)}
                        FROM base_controle_apontamentos_matriz
                    WHERE {expression_mysql};
                """
            print(comand)


            self.cursor.execute(comand)
            result = self.cursor.fetchall()
            print(f"\n\n\n ### TT RESULT: {len(result)}")

        self.close_conn_database(object=self.conn)
        self.close_conn_database(object=self.cursor)
        return result