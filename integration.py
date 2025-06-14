
import json
import time

from modules.invenzi import Invenzi
from db_handlers.oracle import OracleDBManager
from db_handlers.sql import SQLServerDBManager
from utils.tracer import trace, report_exception
from utils.local_cache import LocalCache


CHTYPES = [2, 3]
ACCCESS_LEVELS = [1, 2]


# db_config_windows = {
#     'server': 'RPH-SRV',
#     'database': 'MPPR',
#     'integrated_security': True  # Windows Authentication
# }
# mppr_db = SQLServerDBManager(**db_config_windows)

invenzi = Invenzi()
mppr_db = OracleDBManager()
local_cache = LocalCache(mppr_db)

# local_cache.clear_cache_completely()

script = """
SELECT
    num_cpf,
    tipo,
    nome_pessoa,
    num_rg,
    desc_funcao,
    status,
    cod_regime
FROM {table}
"""


def get_all_wxs_users(mppr_users_list):
    wxs_users_dict = {}
    include_tables = "Cards,CHAccessLevels"
    try:
        if len(mppr_users_list) > 30:
            all_wxs_users = invenzi.get_all_users(ch_types=CHTYPES, include_tables=include_tables)
            print(len(all_wxs_users))

            for user in all_wxs_users:
                if user["IdNumber"] in wxs_users_dict:
                    trace(f"Duplicate IdNumber found: {user['IdNumber']}")
                    continue
                elif not user["IdNumber"]:
                    trace("User has no IdNumber, skipping...")
                    continue

                wxs_users_dict[user["IdNumber"]] = user
            trace(f"Total users retrieved: {len(all_wxs_users)}")

        else:
            mppr_cpf_list = [x[0] for x in mppr_users_list['data']]
            for _cpf in mppr_cpf_list:
                _wxs_user = invenzi.get_user_by_idnumber(_cpf, include_tables=include_tables)
                wxs_users_dict[_cpf] = _wxs_user

        return wxs_users_dict

    except Exception as e:
        report_exception(e, "Error retrieving WXS users")
        return wxs_users_dict


def main():
    wxs_users_dict = {}
    # ret = mppr_db.execute_query(script)
    result = local_cache.process_select(script)
    result_count = len(result['data'])
    print(f"Changed lines: {result_count}")

    wxs_users_dict = get_all_wxs_users(result)

    for cpf, tipo, nome, rg, funcao, status, regime in result["data"]:
        try:
            chtype = None
            if tipo.upper() == 'MEMBRO':
                chtype = 7
            elif tipo.upper() == 'SERVIDOR':
                chtype = 2
            else:
                trace(f"CHType Invalido, usuario {nome}, cpf={cpf} | Tipo={tipo}")
                continue

            mppr_user = {
                "IdNumber": cpf,
                "FirstName": nome,
                "CHType": chtype,
                "CHState": 0 if status.upper() == "ATIVO" else 1,
                "AuxText02": rg,
                "AuxText03": funcao,
                "AuxText07": regime,
                "CompanyID": 21
            }

            if wxs_user := wxs_users_dict.get(mppr_user["IdNumber"]):
                fields_to_compare = ["FirstName", "CHType", "CHState", "AuxText02", "AuxText03", "AuxText07", "CompanyID"]
                fields_with_difference = [field for field in fields_to_compare if wxs_user[field] != mppr_user[field]]

                if fields_with_difference:
                    for upd_field in fields_with_difference:
                        wxs_user[upd_field] = mppr_user[upd_field]

                    invenzi.update_user(wxs_user)
                else:
                    trace(f"No changes for user {nome} (CPF: {cpf})")

                if not wxs_user.get('Cards'):
                    invenzi.assign_card(wxs_user)

            else:
                wxs_user = invenzi.create_user(mppr_user)
                invenzi.assign_card(wxs_user)

            user_access_levels = [x["AccessLevelID"] for x in wxs_user["CHAccessLevels"]]
            if pending_access_levels := [item for item in ACCCESS_LEVELS if item not in user_access_levels]:
                invenzi.assign_access_level(wxs_user["CHID"], pending_access_levels)

        except Exception as e:
            report_exception(e, f"Error processing user {nome} (CPF: {cpf})")
            continue


if __name__ == '__main__':
    trace("Starting MPPR Integration")
    while True:
        main()
        trace("Integration cycle completed, sleeping for 30 seconds...")
        time.sleep(5)
