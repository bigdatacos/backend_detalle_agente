#!/usr/bin/python

from src.utils import *

if __name__ == "__main__":

    try:
        agentes_conectados, df_agentes_en_llamada, df_usersv2, df_agentes_en_pausa, df_agentes_programados, df_ultima_gestion = extract_sql()

        load(cards(agentes_conectados, df_agentes_en_llamada, df_usersv2,
                   df_agentes_en_pausa, df_agentes_programados), "tb_cards")
        load(details(agentes_conectados, df_agentes_en_llamada, df_usersv2,
                     df_agentes_en_pausa, df_agentes_programados, df_ultima_gestion), "tb_details")
    except Exception as e:
        logging.info(e)
