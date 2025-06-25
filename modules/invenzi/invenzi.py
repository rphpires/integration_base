import requests
import os
import random
from dataclasses import dataclass
from typing import Optional, List, Dict, Any, Union
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv

from utils.tracer import trace, report_exception
from utils.functions import parse_date, remove_accents_from_string
from db_handlers.sql import SQLServerDBManager

from .class_invenzi import ComboField


load_dotenv()


db_server = os.getenv("DATABASE_URL")
db_user = os.getenv("DATABASE_USER") if os.getenv("DATABASE_USER") else "W-Access"
db_password = os.getenv("DATABASE_PASSWORD") if os.getenv("DATABASE_PASSWORD") else "W-Access"
db_name = os.getenv("DATABASE_NAME") if os.getenv("DATABASE_NAME") else "W-Access"


class Invenzi:
    """Invenzi API handler - ALL API OPERATIONS HERE"""

    def __init__(self, environment='prod'):
        self._session = None
        self.api_url = None
        self.api_user = None
        self.api_password = None
        self.wxs_db_handler = SQLServerDBManager(
            'SRV-WXS\\W_ACCESS',
            'W_Access',
            'W-Access',
            'db_W-X-S@Wellcare924_',
            driver='ODBC Driver 17 for SQL Server'
        )

        self.initialize_environment(environment)

    def initialize_environment(self, environment: str):
        if environment == 'prod':
            self.api_url = os.getenv("API_URL") if os.getenv("API_URL") else "http://localhost/W-AccessAPI/v1"
            self.api_user = os.getenv("API_USER") if os.getenv("API_USER") else "WAccessAPI"
            self.api_password = os.getenv("API_PASSWORD") if os.getenv("API_PASSWORD") else "#WAccessAPI#"
        else:
            self.api_url = "http://localhost/W-AccessAPI/v1"
            self.api_user = "WAccessAPI"
            self.api_password = "#WAccessAPI#"
            self._session = None

    def _get_session(self):
        """Get or create HTTP session"""
        if not self._session:
            self._session = requests.Session()
            # self._session.auth = (self.api_user, self.api_password)
            self._session.headers.update({
                'Content-Type': 'application/json',
                'WAccessAuthentication': f'{self.api_user}:{self.api_password}'
            })
        return self._session

    def _api_call(self, endpoint: str, method: str = 'GET', data: Dict = None, params: Dict = {}, files=None) -> tuple[bool, Dict]:
        """Make API call and return success and response data"""
        try:
            session = self._get_session()
            url = f"{self.api_url}/{endpoint}"

            if not params.get('CallAction'):
                params['CallAction'] = False

            rc = requests.codes

            if files:
                response = requests.put(
                    url,
                    files=files,
                    headers={'WAccessAuthentication': f'{self.api_user}:{self.api_password}'}
                )
            else:
                response = session.request(method, url, json=data, params=params)

            if success := response.status_code in [rc.ok, rc.created, rc.no_content]:
                try:
                    response_data = response.json() if response.status_code != rc.no_content else {}
                except Exception:
                    response_data = {}
            else:
                self.trace(f"API Error: {response.status_code} | {response.content}")
                response_data = {}

            return success, response_data
        except Exception as e:
            self.trace(f"API Exception: {e}")
            return False, {}

    def trace(self, msg: str):
        """Trace method for logging"""
        trace(msg)

    def get_all_users(self, **kwargs):
        """Get all users from API"""
        self.trace("Fetching all users from API...")
        limit = 2000
        offset = 0
        params = {'limit': limit, 'offset': offset}
        users = []

        if ch_types := kwargs.get('ch_types'):
            params["chType"] = ch_types

        if include_tables := kwargs.get('include_tables'):
            params["IncludeTables"] = include_tables

        while True:
            trace(f"Getting all users: {params=}")
            success, data = self._api_call("cardholders", method='GET', params=params)
            if not success:
                trace("Failed to retrieve users or no users found")
                break

            trace('Appending users to obj...')
            for user_data in data:
                users.append(user_data)

            trace('Checking limit')
            if len(data) < limit:
                break
            offset += limit
            params['offset'] = offset

        self.trace(f"Retrieved {len(users)} users")
        return users

    def __check_user_required_fieds(self, user: Dict) -> bool:
        """Check if user has required fields"""
        end_validity = datetime.now() + relativedelta(years=10)

        required_fields = {
            'PartitionID': 1,
            'CHState': 0,
            'CHType': 2,
            'FirstName': f'New Created User - {datetime.now().strftime("%Y-%m-%dT%H:%M:%S")}',
            'CHEndValidityDateTime': end_validity.strftime("%Y-%m-%dT%H:%M:%S")
        }
        for field in required_fields.keys():
            if field not in user:
                self.trace(f"User is missing required field: {field}, Settings default field value")
                user[field] = required_fields[field]

        return user

    def __check_user_end_validity(self, user: Dict) -> bool:
        try:
            current_validity_dte = parse_date(user.get('CHEndValidityDateTime'))
            if current_validity_dte < datetime.now():
                self.trace(f"User {user.get('CHID')} has expired validity date, updating to 10 years from now")
                user['CHEndValidityDateTime'] = (datetime.now() + relativedelta(years=10)).strftime("%Y-%m-%dT%H:%M:%S")

        except Exception:
            self.trace(f"Invalid date format for user {user.get('CHID')}, setting to 10 years from now")
            user['CHEndValidityDateTime'] = (datetime.now() + relativedelta(years=10)).strftime("%Y-%m-%dT%H:%M:%S")

        finally:
            return user

    def get_user_by_chid(self, chid: int):
        self.trace(f"Fetching user ID {chid} from API...")
        success, data = self._api_call(f"cardholders/{chid}", method='GET')

        if success and data:
            self.trace(f"Retrieved user: {data}")
            return data
        else:
            self.trace(f"User ID {chid} not found")
            return None

    def get_user_by_idnumber(self, id_number: str, **kwargs):
        self.trace(f"Searching user with IdNumber: {id_number}")

        params = {"idNumber": id_number}
        if include_tables := kwargs.get('include_tables'):
            params["IncludeTables"] = include_tables

        success, data = self._api_call(
            "cardholders",
            method='GET',
            params=params
        )

        if success and data:
            try:
                user = None
                if isinstance(data, list) and len(data) > 0:
                    user = data[0]

                    if len(data) > 1:
                        self.trace(f"Warning: Multiple users found with IdNumber {id_number}. Returning the first one.")

                    self.trace(f"Found user: CHID={user['CHID']}, {user['FirstName']} IdNumber={id_number}")
                    return user

                else:
                    self.trace(f"User with IdNumber {id_number} not found")
                    return None

            except Exception as e:
                report_exception(e, f"Error retrieving user with IdNumber {id_number}")
                return None

    def create_user(self, new_user):
        _new_user = self.__check_user_required_fieds(new_user)

        success, data = self._api_call("cardholders", method='POST', data=_new_user)
        if success and data:
            self.trace(f"New user created with CHID={data['CHID']}, Name={data['FirstName']}")
            return data
        else:
            self.trace("Failed to create new user")
            return None

    def update_user(self, user: dict):
        _user = self.__check_user_end_validity(user)
        try:
            self.trace(f"Updating user {user.get('CHID')} with data: {_user}")
            success, data = self._api_call(
                "cardholders",
                method='PUT',
                data=_user
            )
            if success:
                self.trace(f"User {user.get('CHID')} updated successfully: {data}")
            else:
                self.trace(f"Failed to update user {user.get('CHID')}")

        except Exception as e:
            report_exception(e, f"Error updating user {user.get('CHID')}")

    def delete_user(self, chid: int):
        success, data = self._api_call(
            f"cardholders/{chid}",
            method='DELETE'
        )
        if success:
            self.trace("User deleted")
            return data
        else:
            self.trace("Failed to delete")
            return None

    def photo_update(self, chid, photo, photo_num=1):
        self.trace(f"Atualizando foto do usuário CHID={chid}")
        success, _ = self._api_call(
            f"cardholders/{chid}/photos/{photo_num}",
            method='PUT',
            files=(('photoJpegData', photo),)
        )
        if success:
            self.trace("Foto atualizada com sucesso.")

    def assign_card(self, user: dict, new_card: dict = None):
        try:
            if new_card:
                self.trace(f"Card provided for user {user}")
                card = None

            else:
                card = self.create_random_card()

            self.trace(f"Assigning card {card} to user {user}")

            end_validity = datetime.now() + relativedelta(years=10)
            card["CardEndValidityDateTime"] = end_validity.strftime("%Y-%m-%dT%H:%M:%S")
            card["CardStartValidityDateTime"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

            success, data = self._api_call(
                f"cardholders/{user.get('CHID')}/cards",
                method='POST',
                data=card
            )
            if success and data:
                self.trace(f"Card {data} assigned to user {user}")
                return data
            else:
                self.trace(f"Failed to assign card {card} to user {user}")
                return None

        except Exception as e:
            report_exception(e, f"Error assigning card {card} to user {user}")

    def create_card(self, user: dict, card: dict):
        pass

    def create_random_card(self):
        MAX_CARDNUMER = 65534
        MIN_CARDNUMBER = 1000

        end_validity = datetime.now() + relativedelta(years=10)

        __card_created = False
        while not __card_created:
            card_number = random.randint(MIN_CARDNUMBER, MAX_CARDNUMER)
            self.trace(f"Creating random card with number {card_number}")
            success, data = self._api_call(
                "cards",
                method='POST',
                data={
                    "CardNumber": card_number,
                    "FacilityCode": 0,
                    "ClearCode": f"CARD_{card_number}",
                    "CardType": 0,
                    "CardState": 0,
                    "PartitionID": 0,
                    "CardEndValidityDateTime": end_validity.strftime("%Y-%m-%d %H:%M:%S")
                }
            )
            if success and data:
                self.trace(f"Card created successfully: {data}")
                __card_created = True
                return data

    def assign_access_level(self, chid: int, access_levels: List[int]):
        """Assign access level to user"""
        try:
            for access_level_id in access_levels:
                self.trace(f"Assigning access level {access_level_id} to user {chid}")
                success, _ = self._api_call(
                    f"cardholders/{chid}/accesslevels/{access_level_id}",
                    method='POST',
                    data={}
                )
                if success:
                    self.trace(f"Access level {access_level_id} assigned to user {chid}")
                else:
                    self.trace(f"Failed to assign access level {access_level_id} to user {chid}")

        except Exception as e:
            report_exception(e, f"Error assigning access level {access_level_id} to user {chid}")
            return False

    def start_visit(self):
        pass

    def end_visit(self, visitor):
        success, data = self._api_call(
            f"cardholders/{visitor['CHID']}/activeVisit",
            method='DELETE'
        )
        if success and data:
            self.trace(f"Visit ended to user: {visitor['FirstName']}")
            return data
        else:
            self.trace(f"Failed to end visit to user {visitor['FirstName']}")
            return None

    def combo_fields_get_items(self, field_id=None, chtype=None, combo_index=None):
        try:
            self.trace(f'Getting all items from combofield: {field_id}, CHTpe: {chtype}')
            params = {}
            if field_id:
                params["fieldID"] = f'lstBDA_{field_id}' if not field_id.startswith('lstBDA_') else field_id

            if chtype:
                params["chType"] = chtype

            if combo_index:
                params["comboIndex"] = combo_index

            success, items_list = self._api_call(
                "chComboFields",
                method='GET',
                params=params
            )
            if success:
                self.trace(f"Returning {len(items_list)} Items")
                return [ComboField.from_dict(x) for x in items_list if x is not None]
            else:
                return []

        except Exception as ex:
            report_exception(ex)

    def combo_fields_add_item(self, field_id: str, chtype: int, combo_index: int, name: str):
        try:
            new_item = {
                "FieldID": f'lstBDA_{field_id}' if not field_id.startswith('lstBDA_') else field_id,
                "CHType": int(chtype),
                "ComboIndex": int(combo_index),
                "strLanguage1": name,
                "strLanguage2": name,
                "strLanguage3": name,
                "strLanguage4": '-',
                "Sequence": 0
            }
            success, created_item = self._api_call(
                "chComboFields",
                method="PUT",
                data=new_item
            )
            if success:
                self.trace(f"New item create to comboField= {field_id} with ComboIndex={combo_index}")
                return success

        except Exception as ex:
            report_exception(ex)

    def groups_get_group(self, group_id=None):
        try:
            success, groups = self._api_call(
                'groups' if not group_id else f'groups/{group_id}',
                method="GET"
            )
            if success:
                self.trace(f"Returning {len(groups)} Groups")
                return groups
            else:
                return []

        except Exception as ex:
            report_exception(ex)

    def groups_create_group(self, group_name):
        try:
            # Check if group exists before create it.
            if ret := self.wxs_db_handler.execute_query(f"select top 1 GroupID from CfgCHGroups where GroupName = '{group_name}'"):
                self.trace(f"Group [{group_name}] already exisits with GroupID={ret}")
                return str(ret[0].get('GroupID'))

            self.wxs_db_handler.execute_dml(
                "insert into CfgCHGroups values(0, ?, null, null, null, null)",
                (group_name,)
            )

            if ret := self.wxs_db_handler.execute_query(f"select top 1 GroupID from CfgCHGroups where GroupName = '{group_name}'"):
                self.trace(f"Group [{group_name}] exisits with GroupID={ret}")
                group_id = ret[0].get('GroupID')

            self.wxs_db_handler.execute_dml(
                "insert into CfgCHRelatedGroups values(?, ?)",
                (group_id, 1)
            )

            return str(group_id)

        except Exception as ex:
            report_exception(ex)
            return None

    def add_user_to_group(self, chid, group_id):
        try:
            success, _ = self._api_call(
                f"cardholders/{chid}/groups/{group_id}",
                method="POST"
            )
            if success:
                self.trace("Usuário adicionado ao grupo com sucesso.")

        except Exception as ex:
            report_exception(ex)

    def remove_user_from_group(self, chid, group_id):
        try:
            success, _ = self._api_call(
                f"cardholders/{chid}/groups/{group_id}",
                method="DELETE"
            )
            if success:
                self.trace("Usuário removido do grupo com sucesso.")

        except Exception as ex:
            report_exception(ex)


if __name__ == '__main__':
    import sys
    from pathlib import Path

    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

    wxs = Invenzi()
    wxs.groups_create_group()
