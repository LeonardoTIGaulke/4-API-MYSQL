import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd
import warnings
warnings.simplefilter(action='ignore')

class PrepareData:
    def __init__(self):
        self.api = None

    def get_trimestral_month(self):
        # Obtenha a data atual
        data_atual = datetime.date.today()

        # Calcule o primeiro dia do mês atual
        primeiro_dia_mes_atual = data_atual.replace(day=1)

        # Calcule o último dia do mês anterior
        ultimo_dia_mes_anterior = primeiro_dia_mes_atual - datetime.timedelta(days=1)

        # Calcule o primeiro dia do terceiro mês anterior
        primeiro_dia_terceiro_mes_anterior = ultimo_dia_mes_anterior.replace(day=1) - datetime.timedelta(days=1)

        list_month = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
        mes_01 = list_month[primeiro_dia_terceiro_mes_anterior.month-1]
        mes_02 = list_month[ultimo_dia_mes_anterior.month-1]
        mes_03 = list_month[data_atual.month-1]
        list_month = {
            mes_01: primeiro_dia_terceiro_mes_anterior.month,
            mes_02: ultimo_dia_mes_anterior.month,
            mes_03: data_atual.month,
        }
        return {"months": list_month, "years": [data_atual.year]}

    
    def filter_dataframe_to_trimestral(self, ano, mes, df):
        # df.to_excel("base_calculo_apont.xlsx")
        data_months = {
            "janeiro"       : 1,
            "fevereiro"     : 2,
            "março"         : 3,
            "abril"         : 4,
            "maio"          : 5,
            "junho"         : 6,
            "julho"         : 7,
            "agosto"        : 8,
            "setembro"      : 9,
            "outubro"       : 10,
            "novembro"      : 11,
            "dezembro"      : 12,
        }

        try:
            mes = data_months[mes]
            min_year = ano
            if mes <= 6:
                min_year = min_year - 1

                if mes == 6:
                    mes_init = 1
                    mes_final = 6
                elif mes == 5:
                    mes_init = 12
                    mes_final = mes
                elif mes == 4:
                    mes_init = 11
                    mes_final = mes
                elif mes == 3:
                    mes_init = 10
                    mes_final = mes
                elif mes == 2:
                    mes_init = 9
                    mes_final = mes
                elif mes == 1:
                    mes_init = 8
                    mes_final = mes
            else:
                mes_init = mes - 5
                mes_final = mes
            
            dt_init     = datetime.datetime(year=min_year, month=mes_init, day=1)
            dt_final    = datetime.datetime(year=ano, month=mes_final, day=1)
            ultimo_dia = (dt_final + relativedelta(months=1)).replace(day=1) - datetime.timedelta(days=1)
            dt_final    = datetime.datetime(year=ano, month=mes_final, day=ultimo_dia.day)

            print(f"\n\n >>>>>>>> mes: {mes} | min_year: {min_year} | ano: {ano}")
            print(f"""\n\n
                -------------------------------
                >> dt_init: {dt_init}
                >> dt_final: {dt_final}
            """)
            
            df['data'] = pd.to_datetime(df['data_apont'], format='%d/%m/%Y')
            df = df[ ( df["data"] >= dt_init ) & ( df["data"] <= dt_final ) ]
            df.sort_values(by=["data"], inplace=True)
            df["ano_mes"] = "-"
            
            for i in df.index:
                df["ano_mes"][i] = f'{df["setor"][i]}-{df["ano"][i]}/{df["mes"][i]}'
            
            # print(df)
            # print(df.info())

            return df
        except Exception as e:
            print(e)
            return e


    def converter_segundos_para_tempo(self, segundos):
        horas = segundos // 3600
        minutos = (segundos % 3600) // 60
        segundos_restantes = segundos % 60
        return f"{horas:02d}:{minutos:02d}:{segundos_restantes:02d}"

    def convert_data_to_dataframe_generic(self, data, list_cols_names:list, order_by:list):
        
        try:
            # print(" ----------------->> list_cols_names")
            # print(list_cols_names)
            df = pd.DataFrame(data, columns=list_cols_names).sort_values(by=order_by, ascending=True)
            return df
        except Exception as e:
            print(f"\n\n ### ERROR CONVERT DATA TO DATAFRAME | ERROR: {e}")
            return None
    
    # ----
    
    def convert_data_to_dataframe(self, data, list_cols_names:list):
        
        try:

            df = pd.DataFrame(data, columns=list_cols_names)
            df["regime_agrupado"] = "Outros"
            df["horas"] = 0
            df["minutos"] = 0

            for i in df.index:
                tempo = df["tempo"][i].split(":")
                if len(tempo) == 2:
                    df["horas"][i] = int(tempo[0])
                    df["minutos"][i] = int(tempo[1])

                # ----

                regime = df["regime"][i]
                if "SIMPLES" in regime:
                    df["regime_agrupado"][i] = "Simples"
                elif "LUCRO REAL" in regime:
                    df["regime_agrupado"][i] = "Lucro Real"
                elif "LUCRO PRESUMIDO" in regime:
                    df["regime_agrupado"][i] = "Lucro Presumido"


            # print(df)
            # print(df.info())
            return df
        except Exception as e:
            print(f"\n\n ### ERROR CONVERT DATA TO DATAFRAME | ERROR: {e}")
            return None
    
    def prepare_resume_apont_horas_by_col_name(self, dataframe, col_name_extract, regime: list, setor: list, name_setor:str):
        data = None

        print(f"""
            -----------------
            regime: {regime}
            setor: {setor}
            col_name_extract: {col_name_extract}
            name_setor: {name_setor}
        """)

        try:

            list_extract = list(dataframe.drop_duplicates(subset=[col_name_extract], keep="last")[col_name_extract].values)

            print("\n\n ----------- list_extract ----------- ")
            print(list_extract)
            data = {}

            if name_setor != "todos":
                dataframe = dataframe[ dataframe["setor"] == name_setor ]
            
            for v in list_extract:
                print(f" -------------- df_filtered -------------- | v: {v}")
                df_filtered = dataframe[dataframe[col_name_extract] == v]
                
                if "" not in setor:
                    df_filtered = df_filtered[ df_filtered["setor"].isin(setor) ]

                # print(df_filtered)
                print(f" ------------ filter v: {v}")

                horas   = sum(df_filtered["horas"].values)
                minutos = sum(df_filtered["minutos"].values)
                tempo_total = 0
                media = 0.0
                tt_apont = len(df_filtered)

                if minutos >= 60:
                    horas += int(minutos/60)
                    minutos = int(minutos % 60)
                tempo_total = float(f"{horas}.{minutos}")

                if tempo_total > 0:

                    media = round(tempo_total/tt_apont, 2)
                    t_aux = str(media).split(".")

                    if int(t_aux[1]) >= 60:

                        m_horas     = int(t_aux[0])
                        m_minutos   = int(t_aux[1])

                        m_horas     += int(m_minutos/60)
                        m_minutos   = (m_minutos % 60)

                        media = float(f"{m_horas}.{m_minutos}")

                print(f"""
                    --------------------------
                    horas: {horas}
                    minutos: {minutos}
                    tempo_total: {tempo_total}
                    tt_apont: {tt_apont}
                    media: {media}
                    name: {v}
                    setor: {setor}
                """)

                data[v] = {
                    "horas": int(horas),
                    "minutos": int(minutos),
                    "tempo_total": tempo_total,
                    "tt_apont": tt_apont,
                    "media_apont": media,
                    "name": v
                }

            return data
        except Exception as e:
            print(f"\n\n ### ERROR CONVERT DATA BY COL TO DATAFRAME | ERROR: {e}")
            return None