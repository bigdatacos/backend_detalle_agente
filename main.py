#!/usr/bin/python

from src.utils import *

if __name__ == "__main__":

    while True:

        try:
            agentes_conectados, df_agentes_en_llamada, df_usersv2, df_agentes_en_pausa, df_ultima_gestion, df_operation, df_candidates = extract_sql()

            load(cards(agentes_conectados, df_agentes_en_llamada, df_usersv2,
                       df_agentes_en_pausa), "tb_cards")
            load(details(agentes_conectados, df_agentes_en_llamada, df_usersv2,
                         df_agentes_en_pausa, df_ultima_gestion, df_operation, df_candidates), "tb_details")
            time.sleep(1)
        except Exception as e:
            logging.info(e)
