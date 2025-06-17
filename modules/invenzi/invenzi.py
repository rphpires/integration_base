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

        self.initialize_environment(environment)

    def initialize_environment(self, environment: str):
        if environment == 'prod':
            self.api_url = os.getenv("API_URL") if os.getenv("API_URL") else "http://localhost/W-AccessAPI/v1"
            api_user = os.getenv("API_USER") if os.getenv("API_USER") else "WAccessAPI"
            api_password = os.getenv("API_PASSWORD") if os.getenv("API_PASSWORD") else "#WAccessAPI#"
        else:
            self.api_url = "http://localhost/W-AccessAPI/v1"
            self.api_user = api_user or "WAccessAPI"
            self.api_password = api_password or "#WAccessAPI#"
            self._session = None

    def _get_session(self):
        """Get or create HTTP session"""
        if not self._session:
            self._session = requests.Session()
            self._session.auth = (self.api_user, self.api_password)
            self._session.headers.update({'Content-Type': 'application/json'})
        return self._session

    def _api_call(self, endpoint: str, method: str = 'GET', data: Dict = None, params: Dict = {}) -> tuple[bool, Dict]:
        """Make API call and return success and response data"""
        try:
            session = self._get_session()
            url = f"{self.api_url}/{endpoint}"

            if not params.get('CallAction'):
                params['CallAction'] = False

            rc = requests.codes
            response = session.request(method, url, json=data, params=params)
            success = response.status_code in [rc.ok, rc.created, rc.no_content]

            if success:
                try:
                    response_data = response.json() if response.status_code != rc.no_content else {}
                except Exception:
                    response_data = {}
            else:
                self.trace(f"API Error: {response.status_code}")
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
            'FirstName': f'New Created User - {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}',
            'CHEndValidityDateTime': end_validity.strftime("%Y-%m-%d %H:%M:%S")
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
                user['CHEndValidityDateTime'] = (datetime.now() + relativedelta(years=10)).strftime("%Y-%m-%d %H:%M:%S")

        except ValueError:
            self.trace(f"Invalid date format for user {user.get('CHID')}, setting to 10 years from now")
            user['CHEndValidityDateTime'] = (datetime.now() + relativedelta(years=10)).strftime("%Y-%m-%d %H:%M:%S")
        
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
            self.trace(f"New user created: {data}")
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
            self.trace(f"User deleted")
            return data
        else:
            self.trace(f"Failed to delete")
            return None

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
