
import json
import time
import schedule
import hashlib
import os

from datetime import datetime

from modules.invenzi import Invenzi
from db_handlers.oracle import OracleDBManager
from db_handlers.sql import SQLServerDBManager
from utils.tracer import trace, report_exception
from utils.local_cache import LocalCache


CHTYPE = 2
ACCCESS_LEVELS = [21]

wxs_genero_list = {
    'F': 0,
    'M': 1,
    '--': 2
}


invenzi = Invenzi()
sc_db = OracleDBManager(
    'USRSCMSUSUVR35',
    'tb67574#EHgY#yjtGHJ',
    '10.99.1.5:1521/pdb_scmsc.sub08211821591.vcnscsaocarlos.oraclevcn.com',
    oracle_client_lib_dir=r'C:\Program Files (x86)\Invenzi\Invenzi W-Access\Web Application\Action Programs\ronda_integration\instantclient'
)
local_cache = LocalCache(sc_db)
local_cache.clear_cache_completely()

script = """
SELECT
    CPF,
    CODIGO,
    nome,
    Genero,
    email,
    celular,
    admissao,
    demissao,
    centro_de_custo,
    SITAFA
FROM (
    SELECT CPF, CODIGO, nome, Genero, email, celular, admissao, demissao, centro_de_custo, SITAFA,
           ROW_NUMBER() OVER (PARTITION BY CPF ORDER BY CODIGO DESC) AS rn
    FROM VETPROD.vw_usu_vr35
    -- WHERE CODIGO IN ('38166', '40568', '6660049', '84800', '84931', '84939', '85065', '85072', '85315', '85325', '85332', '86339', '86350', '86354', '86355')
)
WHERE rn = 1
AND (
    demissao IS NULL
    OR TRIM(demissao) = ''
    OR demissao = 'NULL'
    OR TO_DATE(demissao, 'DD/MM/YYYY') >= SYSDATE - 30
)
"""


def get_all_wxs_users(import_users_list):
    wxs_users_dict = {}
    include_tables = "Cards,CHAccessLevels,Groups"
    try:
        if len(import_users_list['data']) > 30:
            # all_wxs_users = invenzi.get_all_users(ch_types=CHTYPES, include_tables=include_tables)
            all_wxs_users = invenzi.get_all_users(include_tables=include_tables)
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
            import_cpf_list = [x[0] for x in import_users_list['data']]
            for _cpf in import_cpf_list:
                _wxs_user = invenzi.get_user_by_idnumber(_cpf, include_tables=include_tables)
                wxs_users_dict[_cpf] = _wxs_user

        return wxs_users_dict

    except Exception as e:
        report_exception(e, "Error retrieving WXS users")
        return wxs_users_dict


def get_cc_list():
    cc_items = {}
    for item in invenzi.combo_fields_get_items(field_id="AuxLst01", chtype=CHTYPE):
        cc_items[item.strLanguage2] = item.ComboIndex

    return cc_items


def get_groups():
    wxs_groups = {}
    for group in invenzi.groups_get_group():
        wxs_groups[group["GroupName"]] = group["GroupID"]
    return wxs_groups


def check_visitor_state(wxs_user):
    try:
        if not (_visitor := invenzi.get_user_by_idnumber(wxs_user["IdNumber"], include_tables="Cards,CHAccessLevels,ActiveVisit")):
            return None

        if _visitor.get('ActiveVisit'):
            invenzi.end_visit()

        return _visitor

    except Exception as ex:
        report_exception(ex)
        return None


def format_to_invenzi_datetime(dt):
    try:
        if not dt:
            return None

        dte = datetime.strptime(dt, "%d/%m/%Y")
        # date_obj = dte.replace(hour=3, minute=0, second=0)
        return dte.strftime("%Y-%m-%dT%H:%M:%S")

    except ValueError as ex:
        report_exception(ex)
        return None


def get_user_photo(codigo):
    photos_path = r'C:\Fotos Validacao'
    extensions = ['jpg', 'jpeg', 'png', 'bmp', 'gif', 'JPG', 'JPEG', 'PNG', 'BMP', 'GIF']

    for ext in extensions:
        file_path = os.path.join(photos_path, f"{codigo}.{ext}")
        if os.path.exists(file_path):
            try:
                with open(file_path, 'rb') as file:
                    photo_binary = file.read()

                md5_hash = hashlib.md5(photo_binary).hexdigest()

                return {
                    'binary': photo_binary,
                    'md5': md5_hash.upper() if md5_hash else ""
                }

            except Exception as e:
                print(f"Erro ao ler arquivo {file_path}: {e}")
                continue

    return {}


def get_ronda_status(status_integra, demissao):
    if demissao and datetime.strptime(demissao, "%d/%m/%Y") < datetime.now():
        return 2

    if status_integra in ["5", "6", "8", "9", "10", "11", "23", "56", "58", "59", "60", "61", "504", "909"]:  # vide itens no final deste arquivo.
        return 4  # Licenciado

    return {
        "1": 0,  # Trabalhando
        "2": 3,  # Férias
        "7": 2,  # Demitido
    }.get(str(status_integra), 1)  # Use "Inactive" to another status


def get_cc_from_wxs_dict(cc_name: str, wxs_cc_dict: dict):
    try:
        if not cc_name or cc_name.strip() == '':
            return None

        cc_cleaned = cc_name.strip()[:50]

        if cc_cleaned in wxs_cc_dict:
            return wxs_cc_dict[cc_cleaned]

        try:
            new_combo_index = max(wxs_cc_dict.values()) + 1
            if invenzi.combo_fields_add_item("AuxLst01", CHTYPE, new_combo_index, cc_cleaned):
                wxs_cc_dict[cc_cleaned] = new_combo_index
                return new_combo_index
            else:
                return None

        except Exception as ex:
            report_exception(ex)

    except Exception as ex:
        report_exception(ex)


def get_user_group(centro_de_custo: str, wxs_groups: dict):
    try:
        if not centro_de_custo or centro_de_custo.strip() == '':
            return None

        cc_cleaned = centro_de_custo.strip()[:50]

        if cc_cleaned in wxs_groups:
            return wxs_groups[cc_cleaned]

        try:
            new_combo_index = max(wxs_groups.values()) + 1
            if invenzi.groups_create_group("AuxLst01", CHTYPE, new_combo_index, cc_cleaned):
                wxs_groups[cc_cleaned] = new_combo_index
                return new_combo_index
            else:
                return None

        except Exception as ex:
            report_exception(ex)

    except Exception as ex:
        report_exception(ex)


def main():
    wxs_users_dict = {}
    result = local_cache.process_select(script)
    result_count = len(result['data'])
    print(f"Changed lines: {result_count}")

    wxs_users_dict = get_all_wxs_users(result)
    wxs_centro_de_custo_obj = get_cc_list()

    for cpf, codigo_integra, nome, genero, email, celular, admissao, demissao, centro_de_custo, sitafa in result["data"]:
        try:
            photo_obj = get_user_photo(codigo_integra)

            sc_user = {
                "IdNumber": cpf,
                "FirstName": nome,
                "CHType": CHTYPE,
                "EMail": email,
                "CHState": get_ronda_status(sitafa, demissao),
                "AuxText01": str(codigo_integra),
                "AuxText02": celular,
                "AuxDte01": format_to_invenzi_datetime(admissao),
                "AuxDte02": format_to_invenzi_datetime(demissao),
                "AuxLst01": get_cc_from_wxs_dict(centro_de_custo, wxs_centro_de_custo_obj),
                "AuxLst02": wxs_genero_list.get(genero, 2),
                "AuxText10": photo_obj.get('md5'),
                "AuxText11": invenzi.groups_create_group(centro_de_custo.strip()[:50])
            }

            if wxs_user := wxs_users_dict.get(sc_user["IdNumber"]):
                fields_to_compare = ["FirstName", "CHType", "CHState", "AuxText01", "AuxText02", "AuxDte01", "AuxDte02",
                                     "EMail", "AuxLst01", "AuxLst02", "AuxText10", "AuxText11"]
                fields_with_difference = [field for field in fields_to_compare if wxs_user[field] != sc_user[field]]

                if fields_with_difference:
                    trace(f"fields_with_difference: {fields_with_difference}")
                    if "CHType" in fields_with_difference and wxs_user["CHType"] == 1:
                        wxs_user = check_visitor_state(wxs_user)

                    if "AuxText11" in fields_with_difference:
                        try:
                            if [x["GroupID"] for x in wxs_user["Groups"] if wxs_user["AuxText11"] is not None and int(x["GroupID"]) == int(wxs_user["AuxText11"])]:
                                invenzi.remove_user_from_group(wxs_user["CHID"], wxs_user["AuxText11"])

                        except Exception as ex:
                            report_exception(ex)

                        invenzi.add_user_to_group(wxs_user["CHID"], sc_user["AuxText11"])

                    for upd_field in fields_with_difference:
                        wxs_user[upd_field] = sc_user[upd_field]

                    if "AuxText10" in fields_with_difference and photo_obj.get('binary'):
                        invenzi.photo_update(wxs_user["CHID"], photo_obj.get('binary'))

                    invenzi.update_user(wxs_user)

                else:
                    trace(f"No changes for user {nome} (CPF: {cpf})")

                if not wxs_user.get('Cards'):
                    invenzi.assign_card(wxs_user)

            else:
                print('create user')
                wxs_user = invenzi.create_user(sc_user)
                invenzi.assign_card(wxs_user)

                if photo_obj.get('binary'):
                    invenzi.photo_update(wxs_user["CHID"], photo_obj.get('binary'))

                invenzi.add_user_to_group(wxs_user["CHID"], sc_user["AuxText11"])

        except Exception as e:
            report_exception(e, f"Error processing user {nome} (CPF: {cpf})")
            continue


def process_all_users():
    local_cache.clear_cache_completely()


schedule.every().day.at("04:00").do(process_all_users)


if __name__ == '__main__':
    trace("Starting Santa Casa Integration")
    while True:
        schedule.run_pending()
        main()
        trace("Integration cycle completed, sleeping for 60 seconds...")
        time.sleep(5)


# CPF: IdNumber
# nome: FirstName
# email: Email

# CODIGO: AuxText01
# celular: AuxText02

# admissao: AuxDte01
# demissao: AuxDte02

# centro_de_custo: AuxLst01
# Genero: AuxLst02


# ---------- Lista de Status Ronda ----------
# 1	Trabalhando
# 2	Férias
# 3	Auxílio Doença
# 4	Acidente Trabalho (INSS)
# 5	Serviço Militar
# 6	Licença Maternidade
# 7	Demitido
# 8	Lic. s/ Remuneração
# 9	Lic.Rem p/ Empresa
# 10	Lic. Rem. p/ Funcion.
# 11	Licença Paternidade
# 12	Férias Coletivas
# 13	Aviso Previo Trab.
# 14	Atestado
# 15	Faltas
# 16	Horas Extras N Autorizada
# 17	Amamentação
# 18	Amamentacao
# 19	Auxílio Maternidade INSS
# 20	Adicional Noturno
# 21	Férias Gozadas já Adiantadas
# 22	Aposentadoria
# 23	Licença acidente trabalho
# 24	ABONO HORARIO D VERÃO
# 25	Atestado Acidente Trabalho
# 51	Trabalho Noturno
# 52	Férias Noturnas
# 53	Auxílio Doença Noturno
# 54	Acidente Trabalho Noturno
# 55	Serviçoo Militar Noturno
# 56	Licença Maternidade Noturna
# 58	Lic. s/ Remuneração Noturna
# 59	Lic. Rem. p/ Empresa Noturna
# 60	Lic. Rem. p/ Funcion.Noturna
# 61	Licença Paternidade Noturna
# 62	Férias Coletivas Noturnas
# 63	Aviso Prévio Trab. Noturno
# 64	Atestado Noturno
# 65	Faltas Noturnas
# 66	Horas Extras N Autor.Noturna
# 67	Marcações em Cartão Manual
# 68	FERIAS INTERROMPIDAS
# 101	Saída Antecipada
# 102	Saída Antecipada Noturna
# 103	Atraso
# 104	Atraso Noturno
# 105	Saída Intermediária
# 106	Saída Intermediária Noturna
# 110	Aposentadoria por Invalidez
# 111	Treinamento Externo
# 135	MARCAÇÃO NO AFASTAMENTO
# 201	Saída Médico
# 202	Saída Médico Noturna
# 203	Viagem a Serviço
# 204	Viagem a Serviço Noturna
# 205	Horas Justificadas
# 206	Horas Justificadas Noturnas
# 250	Outros Afastamentos
# 301	Horas Extras 50%
# 302	Horas Extras 50% Noturnas
# 303	Horas Extras 100%
# 304	Horas Extras 100% Noturnas
# 305	Horas Extras 90%
# 306	Horas Extras 90% Noturnas
# 307	Horas Extras 75%
# 308	Horas Extras 75% Noturnas
# 500	Atestado (Obito)
# 501	Ausencia
# 502	Falta Hor.Refeição
# 503	Aux. Matern. Adoção 4 a 8 anos
# 504	Prorrogação Lic Mat Internação
# 505	Atestado Covid
# 898	Abono Acompanhante Cirurgia
# 899	Abono Acompanhante Menor
# 900	Abono Gestante Lei 14.151/21
# 901	Credito BH 50%
# 902	Debito Banco de Horas
# 903	Debito Banco de Horas (+2h)
# 904	Crédito BH 50% NOT
# 905	Crédito BH 90%
# 906	Crédito BH 90% NOT
# 907	Troca de Plantao
# 908	Abono de Horas
# 909	Licença Casamento
# 910	Abono Aniversario
# 911	BANCO DE HORAS PAGO
# 912	Credito BH DSR/Feriado
# 913	Credito BH DSR/Feriado Noturna
# 914	Crédito BH 100%
# 915	Crédito BH 100% NOT
# 916	Crédito BH Refeição 50%
# 917	Crédito BH Refeição 50% NOT
# 918	Crédito BH Refeição 90%
# 919	Crédito BH Refeição 90% NOT
# 920	Crédito BH Refeição 100%
# 921	Crédito BH Refeição 100% NOT
# 989	Trabalho em Folga 24H
# 990	Intrajornada
# 991	Interjornada
# 992	Extras Excedidas
# 993	Refeição Parcial
# 994	Não Cumprimento Integral Ref
# 995	Atraso
# 997	Suspensão
# 998	Folga 24h
# 999	Falta Marcação(Marc.Invalidas)
