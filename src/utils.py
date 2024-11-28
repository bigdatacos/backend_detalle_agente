import logging
import sys
import os
import yaml
import sqlalchemy as sa
from sqlalchemy import text, Engine, Connection, Table, VARCHAR
from pandas.io.sql import SQLTable
from urllib.parse import quote
import pandas as pd
from pandas import DataFrame
from datetime import datetime

act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir = os.path.join(act_dir, '..')
sys.path.append(proyect_dir)

log_dir = os.path.join(proyect_dir, 'log', 'logs_main.log')
yml_credentials_dir = os.path.join(proyect_dir, 'config', 'credentials.yml')

logging.basicConfig(
    level=logging.INFO,
    filename=(log_dir),
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt='%d-%b-%y %H:%M:%S'
)

with open(yml_credentials_dir, 'r') as f:

    try:
        config = yaml.safe_load(f)
        source1, source3, source4 = config['source1'], config['source3'], config['source4']
    except yaml.YAMLError as e:
        logging.error(str(e), exc_info=True)


class Engine_sql:

    def __init__(self, username: str, password: str, host: str, database: str, port: str) -> None:
        self.user = username
        self.passw = password
        self.host = host
        self.dat = database
        self.port = port

    def get_engine(self) -> Engine:
        return sa.create_engine(f"mysql+pymysql://{self.user}:{quote(self.passw)}@{self.host}:{self.port}/")

    def get_connect(self) -> Connection:
        return self.get_engine().connect()


class Engine_sql_db:

    def __init__(self, username: str, password: str, host: str, database: str, port: str) -> None:
        self.user = username
        self.passw = password
        self.host = host
        self.dat = database
        self.port = port

    def get_engine(self) -> Engine:
        return sa.create_engine(f"mysql+pymysql://{self.user}:{quote(self.passw)}@{self.host}:{self.port}/{self.dat}")

    def get_connect(self) -> Connection:
        return self.get_engine().connect()


engine_176 = Engine_sql(**source1)
engine_aws = Engine_sql(**source3)
engine_68 = Engine_sql_db(**source4)


def import_query(sql):

    with open(sql, 'r') as f_2:

        try:
            querys = f_2.read()
            query = text(querys)
            return query

        except yaml.YAMLError as e_2:
            logging.error(str(e_2), exc_info=True)


def extract_sql():
    try:
        with engine_176.get_connect() as conn:
            df_agentes_conectados = pd.read_sql_query(
                import_query(os.path.join(proyect_dir, "sql", "agentes_conectados.sql")), conn)
            df_agentes_en_llamada = pd.read_sql_query(
                import_query(os.path.join(proyect_dir, "sql", "agentes_en_llamada.sql")), conn)
            df_usersv2 = pd.read_sql_query(
                import_query(os.path.join(proyect_dir, "sql", "usersv2.sql")), conn)
            df_ultima_gestion = pd.read_sql_query(
                import_query(os.path.join(proyect_dir, "sql", "ultima_gestion.sql")), conn)
        with engine_aws.get_connect() as conn:
            df_agentes_en_pausa = pd.read_sql_query(
                import_query(os.path.join(proyect_dir, "sql", "agentes_en_pausa.sql")), conn)
            df_agentes_programados = pd.read_sql_query(
                import_query(os.path.join(proyect_dir, "sql", "agentes_programados.sql")), conn)
        return df_agentes_conectados, df_agentes_en_llamada, df_usersv2, df_agentes_en_pausa, df_agentes_programados, df_ultima_gestion
    except Exception as e:
        logging.info(e)


def cards(df_agentes_conectados, df_agentes_en_llamada, _, df_agentes_en_pausa, df_agentes_programados):

    agentes_disponibles = len(df_agentes_conectados) - \
        len(df_agentes_en_llamada)

    agentes_desconectados = len(
        df_agentes_programados) - len(df_agentes_conectados)

    card = {"agentes_conectados": [len(df_agentes_conectados)],
            "agentes_en_llamada": [len(df_agentes_en_llamada)],
            "agentes_disponibles": [agentes_disponibles],
            "agentes_en_pausa": [len(df_agentes_en_pausa)],
            "agentes_desconectados": [agentes_desconectados]}

    df_cards = pd.DataFrame(card)

    return df_cards


def details(df_agentes_conectados, df_agentes_en_llamada, df_usersv2, df_agentes_en_pausa, df_agentes_programados, df_ultima_gestion):

    try:

        df_join = pd.merge(df_agentes_en_llamada, df_usersv2,
                           left_on="user_id", right_on="id", how="left")

        df_join = pd.merge(df_join, df_agentes_programados,
                           left_on="Cedula", right_on="id_number", how="left")

        df_join["Estado"] = "En llamada"
        df_join["Tiempo"] = (
            datetime.now()-df_join["start"]).dt.total_seconds()
        df_join = df_join[["Cedula", "Nombre", "Usuario",
                           "extension", "Estado", "Tiempo"]]

        if len(df_agentes_en_pausa) > 0:

            df_agentes_en_pausa = pd.merge(
                df_agentes_en_pausa, df_usersv2, left_on="Cedula", right_on="Cedula", how="left")

            df_agentes_en_pausa["fecha_inicio_pausa"] = pd.to_datetime(
                df_agentes_en_pausa["fecha_inicio_pausa"])

            df_agentes_en_pausa["Tiempo"] = (
                datetime.now()-df_agentes_en_pausa["fecha_inicio_pausa"]).dt.total_seconds()

            df_agentes_en_pausa = df_agentes_en_pausa[["Cedula", "Nombre", "Usuario",
                                                       "extension", "Estado", "Tiempo"]]

            df_join = pd.concat([df_join, df_agentes_en_pausa], axis=0)

        df_agentes_disponibles = df_agentes_conectados[~df_agentes_conectados["id"].isin(
            df_agentes_en_llamada["user_id"])]

        df_agentes_disponibles = pd.merge(
            df_agentes_disponibles, df_agentes_programados, left_on="Cedula", right_on="id_number", how="left")

        df_agentes_disponibles = pd.merge(
            df_agentes_disponibles, df_ultima_gestion, left_on="id", right_on="user_id", how="left")

        df_agentes_disponibles["Estado"] = "Disponible"

        df_agentes_disponibles["Tiempo"] = (
            datetime.now()-df_agentes_disponibles["start"]).dt.total_seconds()

        df_agentes_disponibles = df_agentes_disponibles[["Cedula", "Nombre", "Usuario",
                                                         "extension", "Estado", "Tiempo"]]

        df_join = df_join[~df_join["Cedula"].isin(
            ["123456789098", "1007469412", "1023965668", "1032399970"])]

        df_join = pd.concat([df_join, df_agentes_disponibles], axis=0)

        return df_join
    except Exception as e:
        logging.info(e)


def load(df: DataFrame, table: str) -> None:
    try:
        with engine_68.get_connect() as conn:
            df.to_sql(table, con=conn,
                      if_exists="replace", index=False)
    except Exception as e:
        logging.info(e)
