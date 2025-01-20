import logging
import sys
import os
import boto3
import yaml
import sqlalchemy as sa
from sqlalchemy import text, Engine, Connection, Table, VARCHAR
from pandas.io.sql import SQLTable
from urllib.parse import quote
import pandas as pd
from pandas import DataFrame
from datetime import datetime
import traceback
import gc

act_dir = os.path.dirname(os.path.abspath(__file__))
proyect_dir = os.path.join(act_dir, '..')
sys.path.append(proyect_dir)
ssl_cert = os.path.join(proyect_dir, "config", "miguel_hernandez.pem")

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
        source1, source3, source4, source5 = config['source1'], config[
            'source3'], config['source4'], config['source5']
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


class Engine_sql_aws:

    def __init__(self, username: str, password: str, host: str, database: str, port: str) -> None:
        self.user = username
        self.passw = password
        self.host = host
        self.dat = database
        self.port = port

    def get_engine(self) -> Engine:
        client = boto3.client("rds", region_name="us-east-1")
        token = client.generate_db_auth_token(
            DBHostname=self.host, Port=3306, DBUsername=self.user)
        return sa.create_engine(f"mysql+pymysql://{self.user}:{quote(token)}@{self.host}/", connect_args={"ssl": {"ca": ssl_cert}})

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
        return sa.create_engine(f"mysql+pymysql://{self.user}:{quote(self.passw)}@{self.host}:{self.port}/{self.dat}", isolation_level="SERIALIZABLE")

    def get_connect(self) -> Connection:
        return self.get_engine().connect()


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
        engine_176 = Engine_sql(**source1)
        engine_aws = Engine_sql(**source3)
        # engine_aws = Engine_sql_aws(**source5)
        with engine_176.get_connect() as conn:
            # logging.info(f"Conexion 176 {datetime.now()}")
            df_agentes_conectados = pd.read_sql_query(
                import_query(os.path.join(proyect_dir, "sql", "agentes_conectados.sql")), conn)
            # logging.info(f"df_agentes_conectados 176 {datetime.now()}")
            df_agentes_conectados.drop_duplicates(subset=["Cedula_usersv2"])
            df_agentes_en_llamada = pd.read_sql_query(
                import_query(os.path.join(proyect_dir, "sql", "agentes_en_llamada.sql")), conn)
            # logging.info(f"df_agentes_en_llamada 176 {datetime.now()}")
            df_usersv2 = pd.read_sql_query(
                import_query(os.path.join(proyect_dir, "sql", "usersv2.sql")), conn)
            # logging.info(f"df_usersv2 176 {datetime.now()}")
            df_ultima_gestion = pd.read_sql_query(
                import_query(os.path.join(proyect_dir, "sql", "ultima_gestion.sql")), conn)
            # logging.info(f"df_ultima_gestion 176 {datetime.now()}")
            df_operation = pd.read_sql_query(
                import_query(os.path.join(proyect_dir, "sql", "operation.sql")), conn)
            # logging.info(f"df_operation 176 {datetime.now()}")
        with engine_aws.get_connect() as conn:
            # logging.info(f"Conexion aws {datetime.now()}")
            df_agentes_en_pausa = pd.read_sql_query(
                import_query(os.path.join(proyect_dir, "sql", "agentes_en_pausa.sql")), conn)
            df_candidates = pd.read_sql_query(
                import_query(os.path.join(proyect_dir, "sql", "candidates.sql")), conn)

        del engine_176
        del engine_aws
        gc.collect()
        return df_agentes_conectados, df_agentes_en_llamada, df_usersv2, df_agentes_en_pausa, df_ultima_gestion, df_operation, df_candidates
    except Exception as e:
        logging.error("Ocurrió un error: %s", e)
        logging.error("Detalles del stack trace:\n%s", traceback.format_exc())


def cards(df_agentes_conectados, df_agentes_en_llamada, _, df_agentes_en_pausa):

    agentes_disponibles = len(df_agentes_conectados) - \
        len(df_agentes_en_llamada)

    card = {"agentes_conectados": [max(len(df_agentes_conectados), 0)],
            "agentes_en_llamada": [max(len(df_agentes_en_llamada), 0)],
            "agentes_disponibles": [max(agentes_disponibles, 0)],
            "agentes_en_pausa": [max(len(df_agentes_en_pausa), 0)],
            "agentes_en_break": [max(len(df_agentes_en_pausa[df_agentes_en_pausa["Estado"] == "Break"]), 0)],
            "agentes_en_baño": [max(len(df_agentes_en_pausa[df_agentes_en_pausa["Estado"] == "Baño"]), 0)],
            "agentes_en_almuerzo": [max(len(df_agentes_en_pausa[df_agentes_en_pausa["Estado"] == "Almuerzo"]), 0)]}

    df_cards = pd.DataFrame(card)

    return df_cards


def details(df_agentes_conectados, df_agentes_en_llamada,
            df_usersv2, df_agentes_en_pausa, df_ultima_gestion, df_operation, df_candidates):

    try:

        df_join = pd.merge(df_agentes_en_llamada, df_usersv2,
                           left_on="user_id", right_on="id", how="left")

        df_join = pd.merge(df_join, df_candidates,
                           left_on="rrhh_id", right_on="id_candidates", how="left")

        df_join["Estado"] = "En llamada"
        df_join["start"] = pd.to_datetime(df_join["start"], errors="coerce")
        df_join["Tiempo"] = (
            datetime.now()-df_join["start"]).dt.round('s').apply(lambda x: str(x).split(" ")[-1])

        df_join = df_join[["Cedula", "Nombre", "Usuario", "Campana", "Skill",
                           "extension", "Estado", "Telefono", "Tiempo"]]

        if len(df_agentes_en_pausa) > 0:

            df_agentes_en_pausa = pd.merge(
                df_agentes_en_pausa, df_usersv2, left_on="Cedula", right_on="Cedula_usersv2", how="left")

            df_agentes_en_pausa = pd.merge(
                df_agentes_en_pausa, df_operation, left_on="id", right_on="user_id_operation", how="left")

            df_agentes_en_pausa["fecha_inicio_pausa"] = pd.to_datetime(
                df_agentes_en_pausa["fecha_inicio_pausa"])

            df_agentes_en_pausa["Skill"] = ""

            df_agentes_en_pausa["fecha_inicio_pausa"] = pd.to_datetime(
                df_agentes_en_pausa["fecha_inicio_pausa"], errors="coerce")

            df_agentes_en_pausa["Tiempo"] = (
                datetime.now()-df_agentes_en_pausa["fecha_inicio_pausa"]).dt.round('s').apply(lambda x: str(x).split(" ")[-1])

            df_agentes_en_pausa["Telefono"] = "-"

            df_agentes_en_pausa = df_agentes_en_pausa[["Cedula", "Nombre", "Usuario", "Campana", "Skill",
                                                       "extension", "Estado", "Telefono", "Tiempo"]]

            df_join = pd.concat([df_join, df_agentes_en_pausa], axis=0)

        df_agentes_disponibles = df_agentes_conectados[~df_agentes_conectados["id"].isin(
            df_agentes_en_llamada["user_id"])]

        df_agentes_disponibles = pd.merge(
            df_agentes_disponibles, df_ultima_gestion, left_on="id", right_on="user_id", how="left")

        df_agentes_disponibles = pd.merge(
            df_agentes_disponibles, df_candidates, left_on="rrhh_id", right_on="id_candidates", how="left")

        df_agentes_disponibles = pd.merge(
            df_agentes_disponibles, df_operation, left_on="id", right_on="user_id_operation", how="left")

        df_agentes_disponibles["Estado"] = "Disponible"
        df_agentes_disponibles["Skill"] = ""
        df_agentes_disponibles["Telefono"] = "-"

        df_agentes_disponibles["Tiempo"] = (
            datetime.now()-df_agentes_disponibles["start"])

        for index, row in df_agentes_disponibles.iterrows():
            if str(row["Tiempo"]) == "NaT":
                df_agentes_disponibles.at[index, 'Tiempo'] = (
                    datetime.now()-row["updated_at_users"])

        df_agentes_disponibles["Tiempo"] = df_agentes_disponibles["Tiempo"].dt.round(
            's').apply(lambda x: str(x).split(" ")[-1])

        df_agentes_disponibles = df_agentes_disponibles[["Cedula", "Nombre", "Usuario", "Campana", "Skill",
                                                         "extension", "Estado", "Telefono", "Tiempo"]]

        df_join = df_join[~df_join["Cedula"].isin(
            ["123456789098", "1007469412", "1023965668", "1032399970"])]

        df_join = pd.concat([df_join, df_agentes_disponibles], axis=0)

        df_join = df_join.drop_duplicates(
            subset="Cedula", keep="first")

        return df_join

    except Exception as e:
        logging.error("Ocurrió un error: %s", e)
        logging.error("Detalles del stack trace:\n%s", traceback.format_exc())


def load(df: DataFrame, table: str) -> None:
    try:
        engine_68 = Engine_sql_db(**source4)
        with engine_68.get_connect().execution_options(isolation_level="SERIALIZABLE") as conn:
            with conn.begin():
                conn.execute(text(f"LOCK TABLES {table} WRITE"))
                conn.execute(text(f"TRUNCATE {table}"))
                df.to_sql(table, con=conn,
                          if_exists="append", index=False)

                conn.execute(text("UNLOCK TABLES"))

        del engine_68
        gc.collect()
    except Exception as e:
        logging.error("Ocurrió un error: %s", e)
        logging.error("Detalles del stack trace:\n%s", traceback.format_exc())
