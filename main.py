import json
import requests
import pandas as pd
from datetime import datetime
# ----
from typing import List
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from pydantic import BaseModel
# ----
from database.db import APP_MySQL
from api.prepare_data.prepare_data import PrepareData

app = FastAPI()


origins = [
    "*",
    # "http://app.contabilgaulke.com.br",
    # "http://192.168.0.1:8000",
    # "http://192.168.0.1:9000",
    # "http://127.0.0.1:8000",
    # "http://127.0.0.1:9000",
    # "http://127.0.0.1:9000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db = APP_MySQL()


class ItemQueryGeneric(BaseModel):
    database: str
    table_name: str
    list_cols_names: list
    order_by: list

class ItemQueryAllCompanies(BaseModel):
    query_version: str
    cols_names: list
    order_by: list

class ItemQueryFilterData(BaseModel):
    database: str
    table_name: str
    date_init: str
    date_final: str
    list_cols_names: list
    regime: list
    setor: list
    type_company: list

class Item_WssGoogleSheets_V1(BaseModel):
    planilha: str
    aba: str
    celulaEditada: str
    valorEditado: str

class Item_Report(BaseModel):
    report_version: str
    month_init: str
    year_init: str

class Item_SaveLoteIR(BaseModel):
    arr_code_IR: list
    date_payment: str
    dificuldade_lote: str
    status_paymanet_lote: str
    status_smart_IR_lote: str
    info_forma_pagamento: str

class ItemUpdateCompanyCodeJB(BaseModel):
    username: str
    id_acessorias: str
    company_code_JB: str


@app.post("/api/mysql/save-lote-IR/")
def saveIR_Lote(item: Item_SaveLoteIR):

    try:
        print(item)
        query = db.save_lote_IR(
            database="db_app_imposto_renda",
            table_name="app_imposto_de_renda_model_tb_imposto_de_renda",
            arr_code_IR=item.arr_code_IR,
            date_payment=item.date_payment,
            dificuldade_lote=item.dificuldade_lote,
            status_paymanet_lote=item.status_paymanet_lote,
            status_smart_IR_lote=item.status_smart_IR_lote,
            info_forma_pagamento=item.info_forma_pagamento,
            )
        
        return {
            "code": 200,
            "data": "success update lote IR"
        }
     
    except Exception as e:
        return {
            "code": 400,
            "msg": "error MySQL",
            "error": e,
        }

@app.post("/api/mysql/query-generic/")
def query_generic(item: ItemQueryGeneric):

    df = None
    try:

        query = db.query_generic(database=item.database, table_name=item.table_name, list_cols_names=item.list_cols_names)
        if query is not None:
            
            list_cols_names = query["columns_name"]
            df = PrepareData().convert_data_to_dataframe_generic(data=query["result"], list_cols_names=list_cols_names, order_by=item.order_by)
            df = df.to_dict(orient="index")

            print(" ----------------- convert_data_to_dataframe_generic / finish ----------------- ")
            # print(df)

        return {
            "code": 200,
            "data": df
        }
     
    except Exception as e:
        return {
            "code": 400,
            "msg": "error query MySQL",
            "error": e,
        }

@app.post("/api/mysql/query-all-companies/")
def query_all_companies(item: ItemQueryAllCompanies):
    
    try:

        query_all_companies = db.query_generic(database="db_gaulke_contabil", table_name="all_companies", list_cols_names=item.cols_names)
        query_all_code_JB_companies = db.query_generic(database="db_gaulke_contabil", table_name="config_accounts_client", list_cols_names=["*"])
        
        if query_all_companies is not None:
            # list_cols_all_companies = ["id_acessorias", "cnpj", "razao_social", "regime"]
            list_cols_names = query_all_companies["columns_name"]
            list_cols_names_code_JB = query_all_code_JB_companies["columns_name"]
            df_all_companies = PrepareData().convert_data_to_dataframe_generic(data=query_all_companies["result"], list_cols_names=list_cols_names, order_by=item.order_by)
            df_all_code_companies = PrepareData().convert_data_to_dataframe_generic(data=query_all_code_JB_companies["result"], list_cols_names=list_cols_names_code_JB, order_by=["id_acessorias"])
            
            df_all_companies["JB_CODE"] = None
            
            for i in df_all_companies.index:
                id_acessorias = df_all_companies["id_acessorias"][i]
                company_code = df_all_code_companies[df_all_code_companies["id_acessorias"] == id_acessorias]["company_code"]
                if len(company_code) > 0:
                    company_code = company_code.values[0]
                else:
                    company_code = "-"
                
                df_all_companies["JB_CODE"][i] = company_code
                print(f"""
                ---------------------------
                >> id_acessorias: {id_acessorias}
                """)

            print("\n ------------------ df_all_companies ------------------ ")
            print(df_all_companies)
            print(df_all_companies.info())
            print("\n ------------------ df_all_code_companies ------------------ ")
            print(df_all_code_companies)
            print(df_all_code_companies.info())



           
            all_companies = df_all_companies.to_json(orient="index")

            print(item)

            return {
                "code": 200,
                "data": all_companies,
                }
        return {
            "code": 200,
            "data": {},
        }
     
    except Exception as e:
        return {
            "code": 400,
            "msg": "error create report #1",
            "error": str(e),
            }
    
@app.post("/api/mysql/query-report/")
def query_report(item: Item_Report):

    df = None
    mes = item.month_init
    ano = int(item.year_init)
    print(f"""
        -----------------
        mes: {mes} | {type(mes)}
        ano: {ano} | {type(ano)}
    """)
    
    try:

        print("\n\n ----------------- data_months ----------------- ")

        # data_months = PrepareData().get_trimestral_month()
        # print(data_months)
        
        query_all_companies = db.query_generic(database="db_gaulke_contabil", table_name="all_companies", list_cols_names=["*"])
        query_all_apont_hours = db.query_generic(database="db_gaulke_contabil", table_name="base_controle_apontamentos_matriz", list_cols_names=["*"])

        
        if query_all_companies is not None and query_all_apont_hours is not None:
            
            list_cols_names_all_companies   = query_all_companies["columns_name"]
            list_cols_names_apont_hours     = query_all_apont_hours["columns_name"]


            list_cols_all_companies = ["id_acessorias", "cnpj", "razao_social", "regime"]
            # list_cols_apont_horas = ["codigo_empresa", "razao_social"]

            df_all_companies = PrepareData().convert_data_to_dataframe_generic(data=query_all_companies["result"], list_cols_names=list_cols_names_all_companies, order_by=["razao_social"])[list_cols_all_companies]
            df_all_apont_hours = PrepareData().convert_data_to_dataframe_generic(data=query_all_apont_hours["result"], list_cols_names=list_cols_names_apont_hours, order_by=["razao_social"])

            # print("\n ------------------ df_all_companies ------------------ ")
            # print(df_all_companies)
            # print(df_all_companies.info())

            # print("\n ------------------ df_all_apont_hours ------------------ ")
            # print(df_all_apont_hours)
            # print(df_all_apont_hours.info())

            df_all_apont_hours = PrepareData().filter_dataframe_to_trimestral( mes=mes, ano=ano, df=df_all_apont_hours )
            print(df_all_apont_hours)
            data_months = list(df_all_apont_hours["mes"].drop_duplicates(keep="last").values)
            data_years = list(df_all_apont_hours["ano"].drop_duplicates(keep="last").values)

            print(" ----------->>  data_months ----------- ")
            print(data_months)

            print(" ----------->>  data_years ----------- ")
            print(data_years)
            
            
            # -----------------------

            df_all_apont_hours['horario_inicio_aux'] = pd.to_datetime(df_all_apont_hours['horario_inicio'], format="%H:%M")
            df_all_apont_hours['horario_horario_fim_aux'] = pd.to_datetime(df_all_apont_hours['horario_fim'], format="%H:%M")
            df_all_apont_hours["tempo_apont"] = pd.to_timedelta( df_all_apont_hours['horario_horario_fim_aux'] - df_all_apont_hours['horario_inicio_aux'], unit="s")
            df_all_apont_hours["apont_decimal"] = [ t.total_seconds() for t in df_all_apont_hours["tempo_apont"] ]
            df_all_apont_hours["tempo_horas"] = "-"
            df_all_apont_hours["tempo_apont_decimal"] = 0
            
            print("\n\nprocessando cálculo apont horas...\n")
            for i in df_all_apont_hours.index:
                try:
                    df_all_apont_hours["tempo_horas"][i] = str(df_all_apont_hours["tempo_apont"][i]).split("days")[1].strip().replace("+", "").replace("-", "")
                    df_all_apont_hours["tempo_apont_decimal"][i] = int(df_all_apont_hours["apont_decimal"][i])
                except:
                    continue
            
            print("\n\nprocessando indexação de tempo a base de clientes...\n")
            
            list_new_cols = list()
            list_setores = ["fiscal", "contábil"]
            columns_apont = list(df_all_apont_hours["ano_mes"].drop_duplicates(keep="last").values)

            print("\n\n ------------------------------------------ columns_apont ------------------------------------------ ")
            print(columns_apont)


            for ano in data_years:
                for mes in data_months:
                    for setor in list_setores:
                        ano_mes = f"{setor}-{ano}/{mes}"
                        if ano_mes in columns_apont:
                            df_all_companies[ano_mes] = 0
            
            print(df_all_companies)
            print(df_all_companies.info())

            count_aux = 0
            tt_reg = len(df_all_companies.index)
            for i in df_all_companies.index:
                code_company = df_all_companies["id_acessorias"][i]
                for setor in list_setores:

                    for ano in data_years:
                        for mes in data_months:
                            ano_mes = f"{setor}-{ano}/{mes}"
                            if ano_mes in columns_apont:
                                check_ano_mes = df_all_apont_hours[ ( df_all_apont_hours["ano_mes"] == ano_mes ) ]
                                if len(check_ano_mes) > 0:
                                    sum_apont = df_all_apont_hours[
                                        ( df_all_apont_hours["ano_mes"] == ano_mes ) &
                                        (df_all_apont_hours["codigo_empresa"] == code_company ) &
                                        (df_all_apont_hours["setor"] == setor )
                                        ]["tempo_apont_decimal"].sum()
                                    sum_apont = PrepareData().converter_segundos_para_tempo(segundos=sum_apont)
                                    df_all_companies[ano_mes][i] = sum_apont

                                    # print(chr(27) + "[2J")
                                    print(f"""
                                        --------------------------------- reg: {tt_reg} / {count_aux}
                                        >> code_company: {code_company}
                                        >> ano_mes: {ano_mes}
                                        >> setor: {setor}
                                        -------- > {ano} / {mes} | soma: {sum_apont}
                                    """)

                count_aux += 1
            
            try:
                df_all_companies.to_csv("df_all_companies.csv", sep=";")
                df_all_apont_hours.to_csv("base_calculo_apont_horas.csv", sep=";")
            except Exception as e:
                print(f"\n\n ### ERROR CONVERT DF TO CSV | ERROR: {e}")
            
            hearders_table = list(df_all_companies.columns)
            report_json = df_all_companies.to_json(orient="values")

            return {
                "code": 200,
                "hearders_table": hearders_table,
                "data": report_json,
            }
        return {
                "code": 200,
                "hearders_table": [],
                "data": {},
            }
     
    except Exception as e:
        return {
            "code": 400,
            "msg": "error create report #1",
            "error": e,
        }

# ----

@app.post("/api/mysql/query-filter-data/")
def query_filter_data(item: ItemQueryFilterData):
    df = None
    print(" ------- item ------- ")
    print(item)

    try:

        query = db.query_and_filter_data(
            database=item.database,
            table_name=item.table_name,
            list_cols_names=item.list_cols_names,
            date_init=item.date_init,
            date_final=item.date_final,
            list_regime=item.regime,
            list_type_company=item.type_company
            )
        if query is not None:

            df = PrepareData().convert_data_to_dataframe(data=query, list_cols_names=item.list_cols_names)
            data_to_charts = {
                "todos": {},
                "fiscal": {},
                "contábil": {},
            }
            list_col_name_extract = ["atividade", "username", "codigo_empresa"]
            # list_col_setores = ["todos", "fiscal", "contábil"]
            PREPARE_DATA = PrepareData()
            for col_name_extract in list_col_name_extract:
                for name_setor in data_to_charts.keys():
                    data = PREPARE_DATA.prepare_resume_apont_horas_by_col_name(
                        dataframe=df,
                        col_name_extract=col_name_extract,
                        regime=item.regime,
                        setor=item.setor,
                        name_setor=name_setor
                    )
                    print(f" ----------------- query_generic / data / {col_name_extract} / {name_setor} ----------------- ")
                    data_to_charts[name_setor][col_name_extract] = data

            return {
                "code": 200,
                "msg": "success",
                "data": data_to_charts,
            }
        return {
            "code": 401,
            "msg": "error"
        }
     
    except Exception as e:
        return {
            "code": 400,
            "msg": "error query MySQL",
            "error": e,
        }

# ----

@app.post("/api/mysql/update-company-code-JB/")
def query_filter_data(item: ItemUpdateCompanyCodeJB):
    print("\n\n -------------- UPDATE COMPANY CODE JB -------------- ")
    print(item)

    try:
        update = db.update_company_code_JB(
            database="db_gaulke_contabil",
            table_name="config_accounts_client",
            username=item.username, id_acessorias=item.id_acessorias, company_code_JB=item.company_code_JB
        )
        return update
    except Exception as e:
        print(f"\n\n ### ERROR UPDATE COMPANY CODE JB | ERROR: {e}")
        return {
            "code": 500,
            "msg": str(e)
        }