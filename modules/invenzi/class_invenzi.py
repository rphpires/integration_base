import requests
import os
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Union
from datetime import datetime
from dotenv import load_dotenv

from utils.tracer import trace, report_exception


load_dotenv()


db_server = os.getenv("DATABASE_URL")
db_user = os.getenv("DATABASE_USER") if os.getenv("DATABASE_USER") else "W-Access"
db_password = os.getenv("DATABASE_PASSWORD") if os.getenv("DATABASE_PASSWORD") else "W-Access"
db_name = os.getenv("DATABASE_NAME") if os.getenv("DATABASE_NAME") else "W-Access"


@dataclass
class Card:
    """Card information"""
    CardID: int = 0
    ClearCode: str = ""
    CardNumber: int = 0
    FacilityCode: int = 0
    CardType: int = 0
    CHID: int = 0
    CardDownloadRequired: bool = True
    CardState: int = 0
    PartitionID: int = 0
    CardStartValidityDateTime: Optional[datetime] = None
    CardEndValidityDateTime: Optional[datetime] = None
    TempCardLink: int = 0
    OriginalCardState: int = 0
    IPRdrUserID: int = 0
    IPRdrAlwaysEnabled: bool = True
    VisitorTemporaryCard: bool = True
    IsAutomaticCard: bool = True
    RequiresTotpCode: bool = True

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Card':
        if not data:
            return cls()

        card_data = data.copy()
        for date_field in ['CardStartValidityDateTime', 'CardEndValidityDateTime']:
            if date_field in card_data and card_data[date_field]:
                if isinstance(card_data[date_field], str):
                    try:
                        card_data[date_field] = datetime.fromisoformat(card_data[date_field].replace('Z', '+00:00'))
                    except ValueError:
                        card_data[date_field] = None

        valid_fields = set(cls.__dataclass_fields__.keys())
        filtered_data = {k: v for k, v in card_data.items() if k in valid_fields}
        return cls(**filtered_data)


@dataclass
class CHAccessLevel:
    """Access level information"""
    CHID: int = 0
    AccessLevelID: int = 0
    AccessLevelStartValidity: Optional[datetime] = None
    AccessLevelEndValidity: Optional[datetime] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CHAccessLevel':
        if not data:
            return cls()

        access_data = data.copy()
        for date_field in ['AccessLevelStartValidity', 'AccessLevelEndValidity']:
            if date_field in access_data and access_data[date_field]:
                if isinstance(access_data[date_field], str):
                    try:
                        access_data[date_field] = datetime.fromisoformat(access_data[date_field].replace('Z', '+00:00'))
                    except ValueError:
                        access_data[date_field] = None

        valid_fields = set(cls.__dataclass_fields__.keys())
        filtered_data = {k: v for k, v in access_data.items() if k in valid_fields}
        return cls(**filtered_data)


@dataclass
class BaseUser:
    # Core user fields
    CHID: int = 0
    CHType: int = 0
    FirstName: str = ""
    LastName: str = ""
    CompanyID: int = 0
    VisitorCompany: str = ""
    EMail: str = ""
    CHState: int = 0

    # Optional datetime fields
    GDPRSignatureDateTime: Optional[datetime] = None
    LastModifDateTime: Optional[datetime] = None
    CHStartValidityDateTime: Optional[datetime] = None
    CHEndValidityDateTime: Optional[datetime] = None

    # Boolean fields
    IsValidGDPRSignature: bool = True
    IsUndesirable: bool = False
    CHDownloadRequired: bool = True
    TraceCH: bool = False
    IgnoreTransitsCount: bool = False
    IgnoreMealsCount: bool = False
    IgnoreAntiPassback: bool = False
    IgnoreZoneCount: bool = False
    RequiresEscort: bool = False
    CanEscort: bool = False
    CanReceiveVisits: bool = False
    IgnoreRandomInspection: bool = False
    BdccIgnore: bool = False
    DisableAutoReturnVisCard: bool = False
    DisableAutoReturnTempCard: bool = False

    # String fields
    IsUndesirableReason1: str = ""
    IsUndesirableReason2: str = ""
    LastModifBy: str = ""
    TrustedLogin: str = ""
    IdNumber: str = ""
    PIN: str = ""
    CHFloor: str = ""
    BdccCompanies: str = ""

    # Numeric fields
    PartitionID: int = 0
    LastModifOnLocality: int = 0
    Trace_AlmP: int = 0
    Trace_Act: int = 0
    DefFrontCardLayout: int = 0
    DefBackCardLayout: int = 0
    MaxTransits: int = 0
    MaxMeals: int = 0
    SubZoneID: int = 0
    BdccState: int = 0
    IdNumberType: int = 0

    # Auxiliary fields
    AuxText01: str = ""
    AuxText02: str = ""
    AuxText03: str = ""
    AuxText04: str = ""
    AuxText05: str = ""
    AuxTextA01: str = ""
    AuxTextA02: str = ""
    AuxLst01: Optional[int] = None
    AuxLst02: Optional[int] = None
    AuxChk01: bool = False
    AuxChk02: bool = False
    AuxDte01: Optional[datetime] = None
    AuxDte02: Optional[datetime] = None

    # Related objects
    Cards: List[Card] = field(default_factory=list)
    CHAccessLevels: List[CHAccessLevel] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseUser':
        """Create User from dictionary"""
        if not data:
            return cls()

        user_data = data.copy()

        # Handle datetime fields
        datetime_fields = [
            'GDPRSignatureDateTime', 'LastModifDateTime',
            'CHStartValidityDateTime', 'CHEndValidityDateTime',
            'AuxDte01', 'AuxDte02'
        ]

        for date_field in datetime_fields:
            if date_field in user_data and user_data[date_field]:
                if isinstance(user_data[date_field], str):
                    try:
                        user_data[date_field] = datetime.fromisoformat(user_data[date_field].replace('Z', '+00:00'))
                    except ValueError:
                        user_data[date_field] = None

        # Handle nested objects
        nested_objects = {}

        if 'Cards' in user_data and user_data['Cards']:
            nested_objects['Cards'] = [Card.from_dict(card) for card in user_data['Cards']]

        if 'CHAccessLevels' in user_data and user_data['CHAccessLevels']:
            nested_objects['CHAccessLevels'] = [CHAccessLevel.from_dict(level) for level in user_data['CHAccessLevels']]

        # Remove nested objects from user_data
        for key in ['Cards', 'CHAccessLevels']:
            user_data.pop(key, None)

        valid_fields = set(cls.__dataclass_fields__.keys())
        filtered_data = {k: v for k, v in user_data.items() if k in valid_fields}
        filtered_data.update(nested_objects)

        return cls(**filtered_data)

    def to_dict(self, 
                include_none: bool = True, 
                datetime_format: str = 'iso', 
                include_nested: bool = True,
                exclude_fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Converte a instância BaseUser em um dicionário.
        
        Args:
            include_none (bool): Se deve incluir campos com valor None (padrão: True)
            datetime_format (str): Formato para datas ('iso', 'timestamp', 'string')
            include_nested (bool): Se deve incluir objetos aninhados (Cards, CHAccessLevels)
            exclude_fields (List[str], optional): Lista de campos para excluir
            
        Returns:
            Dict[str, Any]: Dicionário com os dados do usuário
        """
        exclude_fields = exclude_fields or []
        result = {}
        
        # Campos de datetime que precisam de tratamento especial
        datetime_fields = {
            'GDPRSignatureDateTime', 'LastModifDateTime',
            'CHStartValidityDateTime', 'CHEndValidityDateTime',
            'AuxDte01', 'AuxDte02'
        }
        
        # Iterar sobre todos os campos da dataclass
        for field_name, field_value in self.__dict__.items():
            # Pular campos excluídos
            if field_name in exclude_fields:
                continue
            
            # Pular valores None se não incluir_none for False
            if not include_none and field_value is None:
                continue
            
            # Tratar campos datetime
            if field_name in datetime_fields:
                if field_value is not None:
                    result[field_name] = self._format_datetime(field_value, datetime_format)
                else:
                    result[field_name] = None
            
            # Tratar listas de objetos aninhados
            elif field_name in ['Cards', 'CHAccessLevels'] and include_nested:
                if field_value:
                    # Assumindo que Card e CHAccessLevel também têm método to_dict
                    result[field_name] = [
                        item.to_dict() if hasattr(item, 'to_dict') else str(item)
                        for item in field_value
                    ]
                else:
                    result[field_name] = []
            
            # Tratar outros tipos de dados
            else:
                result[field_name] = field_value
        
        del result["_invenzi_api"]
        return result
    
    def _format_datetime(self, dt: datetime, format_type: str) -> Union[str, int, float]:
        """
        Formata datetime de acordo com o tipo especificado.
        
        Args:
            dt (datetime): Objeto datetime para formatar
            format_type (str): Tipo de formato ('iso', 'timestamp', 'string')
            
        Returns:
            Union[str, int, float]: Data formatada
        """
        if format_type == 'iso':
            return dt.isoformat()
        elif format_type == 'timestamp':
            return dt.timestamp()
        elif format_type == 'string':
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            return dt.isoformat()  # fallback para ISO

    # ===============================
    # DATA OPERATIONS ONLY (NO API CALLS)
    # ===============================

    def get_active_cards(self) -> List[Card]:
        """Get all active cards"""
        return [card for card in self.Cards if card.CardState == 0]

    def get_active_access_levels(self) -> List[CHAccessLevel]:
        """Get all active access levels"""
        now = datetime.now()
        return [level for level in self.CHAccessLevels
                if (not level.AccessLevelStartValidity or level.AccessLevelStartValidity <= now)
                and (not level.AccessLevelEndValidity or level.AccessLevelEndValidity >= now)]

    def get_card_by_number(self, card_number: int) -> Optional[Card]:
        """Get card by number"""
        return next((card for card in self.Cards if card.CardNumber == card_number), None)

    def has_access_level(self, access_level_id: int) -> bool:
        """Check if user has specific access level"""
        return any(level.AccessLevelID == access_level_id for level in self.get_active_access_levels())

    def is_access_valid(self) -> bool:
        """Check if user has valid access"""
        return self.CHState == 0 and len(self.get_active_access_levels()) > 0

    def is_active(self) -> bool:
        """Check if user is active"""
        return self.CHState == 0

    def __str__(self) -> str:
        full_name = f"{self.FirstName} {self.LastName}".strip() or "Unnamed User"
        status = "Active" if self.CHState == 0 else "Inactive" if self.CHState == 1 else "Blocked"
        return f"{full_name} (ID: {self.CHID}) - {status}"


@dataclass
class ComboField:
   FieldID: str
   CHType: int
   ComboIndex: int
   strLanguage1: str
   strLanguage2: str
   strLanguage3: str
   strLanguage4: str
   Sequence: Optional[int] = None
   
   @classmethod
   def from_dict(cls, data: dict) -> 'ComboField':
       """Cria instância a partir de dicionário"""
       return cls(**data)
   
   def to_dict(self) -> dict:
       """Converte para dicionário"""
       return {
           "FieldID": self.FieldID,
           "CHType": self.CHType,
           "ComboIndex": self.ComboIndex,
           "strLanguage1": self.strLanguage1,
           "strLanguage2": self.strLanguage2,
           "strLanguage3": self.strLanguage3,
           "strLanguage4": self.strLanguage4,
           "Sequence": self.Sequence
       }


# ===============================
# INVENZI USER (ADDS CONVENIENCE METHODS)
# ===============================


class InvenziUser(BaseUser):
    """InvenziUser with reference to Invenzi API"""

    def __init__(self, data: Dict[str, Any] = None, invenzi_api: 'Invenzi' = None):
        # Initialize parent dataclass fields
        if data:
            temp_user = BaseUser.from_dict(data)
            for field_name in BaseUser.__dataclass_fields__.keys():
                setattr(self, field_name, getattr(temp_user, field_name))
        else:
            temp_user = BaseUser()
            for field_name in BaseUser.__dataclass_fields__.keys():
                setattr(self, field_name, getattr(temp_user, field_name))

        self._invenzi_api = invenzi_api

    def assign_access_level(self, access_level_id: int) -> bool:
        """Assign access level - delegates to API"""
        if not self._invenzi_api:
            raise ValueError("No Invenzi API configured")

        return self._invenzi_api.assign_access_level_to_user(self.CHID, access_level_id, self)

    def revoke_access_level(self, access_level_id: int) -> bool:
        """Revoke access level - delegates to API"""
        if not self._invenzi_api:
            raise ValueError("No Invenzi API configured")

        return self._invenzi_api.revoke_access_level_from_user(self.CHID, access_level_id, self)

    def add_card(self, card_number: int, card_type: int = 0) -> bool:
        """Add card - delegates to API"""
        if not self._invenzi_api:
            raise ValueError("No Invenzi API configured")

        return self._invenzi_api.add_card_to_user(self.CHID, card_number, card_type, self)

    def deactivate_card(self, card_number: int) -> bool:
        """Deactivate card - delegates to API"""
        if not self._invenzi_api:
            raise ValueError("No Invenzi API configured")

        return self._invenzi_api.deactivate_user_card(self.CHID, card_number, self)

    def update_info(self, **kwargs) -> bool:
        """Update user info - delegates to API"""
        if not self._invenzi_api:
            raise ValueError("No Invenzi API configured")

        return self._invenzi_api.update_user(self.CHID, kwargs, self)

    def refresh_from_api(self) -> bool:
        """Refresh user data from API"""
        if not self._invenzi_api:
            raise ValueError("No Invenzi API configured")

        updated_user = self._invenzi_api.get_user_by_idnumber(self.CHID)
        if updated_user:
            # Update self with new data
            for field_name in BaseUser.__dataclass_fields__.keys():
                setattr(self, field_name, getattr(updated_user, field_name))
            return True
        return False

# ===============================
# INVENZI API CLASS (ALL API OPERATIONS)
# ===============================


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
                    response_data = response.json()
                except Exception:
                    response_data = {}
            else:
                print(f"API Error: {response.status_code}")
                response_data = {}

            return success, response_data
        except Exception as e:
            print(f"API Exception: {e}")
            return False, {}

    def trace(self, msg: str):
        """Trace method for logging"""
        trace(msg)

    # ===============================
    # USER RETRIEVAL METHODS
    # ===============================

    def get_all_users(self, **kwargs) -> List[InvenziUser]:
        """Get all users from API"""
        print("Fetching all users from API...")
        limit = 2000
        offset = 0
        params = {'limit': limit, 'offset': offset}
        users = []
        
        if ch_types := kwargs.get('ch_types'):
            params["chType"] = ch_types

        while True:
            success, data = self._api_call("cardholders", method='GET', params=params)
            if not success:
                trace("Failed to retrieve users or no users found")
                break

            for user_data in data:
                if kwargs.get('return_as_class_obj'):
                    user = InvenziUser(user_data, self)
                else:    
                    user = user_data
                users.append(user)

            if len(data) < limit:
                break
            offset += limit
            params['offset'] = offset

        print(f"Retrieved {len(users)} users")
        return users

    def get_user_by_chid(self, chid: int) -> Optional[InvenziUser]:
        print(f"Fetching user ID {chid} from API...")
        success, data = self._api_call(f"cardholders/{chid}", method='GET')

        if success and data:
            user = InvenziUser(data, self)
            print(f"Retrieved user: {user}")
            return user
        else:
            print(f"User ID {chid} not found")
            return None

    def get_user_by_idnumber(self, id_number: str, **kwargs) -> Optional[InvenziUser]:
        print(f"Searching user with IdNumber: {id_number}")
        success, data = self._api_call(
            "users/search",
            method='POST',
            data={"id_number": id_number}
        )

        if success and data:
            if kwargs.get('return_as_class_obj'):
                user = InvenziUser(data, self)
            else:
                user = data
            print(f"Found user: {user}")
            return user
        else:
            print(f"User with IdNumber {id_number} not found")
            return None

    def search_users(self, **criteria) -> List[InvenziUser]:
        """Search users by multiple criteria"""
        print(f"Searching users with criteria: {criteria}")
        success, data = self._api_call("users/search", method='POST', data=criteria)

        if success and 'users' in data:
            users = []
            for user_data in data['users']:
                user = InvenziUser(user_data, self)
                users.append(user)
            print(f"Found {len(users)} users matching criteria")
            return users
        else:
            print("Search failed or no users found")
            return []

    # ===============================
    # USER CREATION METHODS
    # ===============================

    def create_user(self, user_data: Dict[str, Any]) -> Optional[InvenziUser]:
        """Create new user via API"""
        print(f"Creating new user: {user_data.get('FirstName', 'Unknown')}")
        success, data = self._api_call("users", method='POST', data=user_data)

        if success and data:
            user = InvenziUser(data, self)
            print(f"User created: {user}")
            return user
        else:
            print("Failed to create user")
            return None

    def from_dict(self, data: Dict[str, Any]) -> InvenziUser:
        """Create InvenziUser from dictionary (local data, no API call)"""
        return InvenziUser(data, self)

    # ===============================
    # USER MODIFICATION METHODS
    # ===============================

    def assign_access_level_to_user(self, user_id: int, access_level_id: int,
                                    user_obj: InvenziUser = None) -> bool:
        print(f"Assigning access level {access_level_id} to user {user_id}")

        # Validation
        if user_obj and not user_obj.is_active():
            print(f"User {user_id} is not active")
            return False

        if user_obj and user_obj.has_access_level(access_level_id):
            print(f"User {user_id} already has access level {access_level_id}")
            return False

        success, data = self._api_call(f"users/{user_id}/access-levels",
                                       method='POST',
                                       data={"access_level_id": access_level_id})

        if success and user_obj:
            # Update local state
            new_access_level = CHAccessLevel(
                CHID=user_id,
                AccessLevelID=access_level_id,
                AccessLevelStartValidity=datetime.now()
            )
            user_obj.CHAccessLevels.append(new_access_level)
            print(f"Access level {access_level_id} assigned to user {user_id}")

        return success

    def revoke_access_level_from_user(self, user_id: int, access_level_id: int,
                                      user_obj: InvenziUser = None) -> bool:
        """Revoke access level from user"""
        print(f"Revoking access level {access_level_id} from user {user_id}")

        success, data = self._api_call(f"users/{user_id}/access-levels/{access_level_id}",
                                       method='DELETE')

        if success and user_obj:
            # Update local state
            for level in user_obj.CHAccessLevels:
                if level.AccessLevelID == access_level_id:
                    level.AccessLevelEndValidity = datetime.now()
                    break
            print(f"Access level {access_level_id} revoked from user {user_id}")

        return success

    def add_card_to_user(self, user_id: int, card_number: int, card_type: int = 0,
                         user_obj: InvenziUser = None) -> bool:
        """Add card to user"""
        print(f"Adding card {card_number} to user {user_id}")

        # Validation
        if user_obj and not user_obj.is_active():
            print(f"User {user_id} is not active")
            return False

        if user_obj and user_obj.get_card_by_number(card_number):
            print(f"Card {card_number} already exists for user {user_id}")
            return False

        success, data = self._api_call(f"users/{user_id}/cards",
                                       method='POST',
                                       data={"card_number": card_number, "card_type": card_type})

        if success and user_obj:
            # Update local state
            new_card = Card(
                CardID=len(user_obj.Cards) + 1,
                CardNumber=card_number,
                CardType=card_type,
                CHID=user_id,
                CardState=0,
                CardStartValidityDateTime=datetime.now()
            )
            user_obj.Cards.append(new_card)
            print(f"Card {card_number} added to user {user_id}")

        return success

    def deactivate_user_card(self, user_id: int, card_number: int,
                             user_obj: InvenziUser = None) -> bool:
        """Deactivate user's card"""
        print(f"Deactivating card {card_number} for user {user_id}")

        card = user_obj.get_card_by_number(card_number) if user_obj else None
        if user_obj and not card:
            print(f"Card {card_number} not found for user {user_id}")
            return False

        card_id = card.CardID if card else 0
        success, data = self._api_call(f"users/{user_id}/cards/{card_id}",
                                       method='DELETE')

        if success and card:
            # Update local state
            card.CardState = 1  # Inactive
            card.CardEndValidityDateTime = datetime.now()
            print(f"Card {card_number} deactivated for user {user_id}")

        return success

    def update_user(self, user_id: int, update_data: Dict[str, Any],
                    user_obj: InvenziUser = None) -> bool:
        """Update user information"""
        print(f"Updating user {user_id} with data: {update_data}")

        success, data = self._api_call(f"users/{user_id}",
                                       method='PUT',
                                       data=update_data)

        if success and user_obj:
            # Update local state
            for key, value in update_data.items():
                if hasattr(user_obj, key):
                    setattr(user_obj, key, value)
            print(f"User {user_id} updated successfully")

        return success

    # ===============================
    # SYSTEM-WIDE OPERATIONS
    # ===============================

    def get_all_access_levels(self) -> List[Dict[str, Any]]:
        """Get all available access levels"""
        print("Fetching all access levels...")
        success, data = self._api_call("access-levels", method='GET')

        if success and 'access_levels' in data:
            print(f"Retrieved {len(data['access_levels'])} access levels")
            return data['access_levels']
        return []

    def get_system_stats(self) -> Dict[str, Any]:
        """Get system statistics"""
        print("Fetching system statistics...")
        success, data = self._api_call("stats", method='GET')

        if success:
            print("System stats retrieved")
            return data
        return {}

# ===============================
# DEMO USAGE
# ===============================


def demo_separated_responsibilities():
    """Demo the separated responsibilities approach"""
    print("=== SEPARATED RESPONSIBILITIES DEMO ===\n")

    # Initialize Invenzi API
    invenzi = Invenzi()

    print("1. API-LEVEL OPERATIONS (without user objects):")

    # Get all users
    all_users = invenzi.get_all_users()
    print(f"   Total users in system: {len(all_users)}")

    # Search specific user
    user = invenzi.get_user_by_id_number("12345")
    if user:
        print(f"Found user by IdNumber: {user}")

    # Search users by criteria
    employees = invenzi.search_users(CompanyID=100, CHState=0)
    print(f"Active employees: {len(employees)}")

    # Get system info
    access_levels = invenzi.get_all_access_levels()
    print(f"Available access levels: {len(access_levels)}")

    stats = invenzi.get_system_stats()
    print(f"System stats: {stats}")
    print()

    print("2. USER-LEVEL OPERATIONS (with user objects):")

    # Create user from dict (your original request)
    user_data = {"FirstName": "João", "LastName": "Silva", "CHID": 123}
    user = invenzi.from_dict(user_data)
    print(f"Created user: {user}")

    # Use convenience methods on user object
    user.assign_access_level(1)
    user.add_card(12345)
    user.update_info(EMail="joao@newcompany.com")
    print(f"User after operations: {user}")
    print()

    print("3. MIXED OPERATIONS:")

    # API operations without user object
    invenzi.assign_access_level_to_user(456, 5)  # Direct API call

    # Then get the user and use convenience methods
    another_user = invenzi.get_user_by_idnumber(456)
    if another_user:
        another_user.add_card(67890)  # Convenience method


    return invenzi, user


if __name__ == "__main__":
    invenzi, user = demo_separated_responsibilities()


#  def to_dict_flat(self, separator: str = '_') -> Dict[str, Any]:
#         """
#         Converte para dicionário "achatado" (sem objetos aninhados).
#         Útil para inserção em bancos de dados relacionais.

#         Args:
#             separator (str): Separador para campos aninhados (padrão: '_')

#         Returns:
#             Dict[str, Any]: Dicionário achatado
#         """
#         result = {}
#         base_dict = self.to_dict(include_nested=False)

#         for key, value in base_dict.items():
#             if key not in ['Cards', 'CHAccessLevels']:
#                 result[key] = value

#         # Adicionar informações resumidas dos objetos aninhados
#         result[f'Cards{separator}count'] = len(self.Cards)
#         result[f'CHAccessLevels{separator}count'] = len(self.CHAccessLevels)

#         return result

#     def to_json(self, **kwargs) -> str:
#         """
#         Converte para string JSON.

#         Args:
#             **kwargs: Argumentos passados para to_dict()

#         Returns:
#             str: String JSON
#         """
#         dict_data = self.to_dict(**kwargs)
#         return json.dumps(dict_data, ensure_ascii=False, indent=2)

#     def to_dict_for_db(self) -> Dict[str, Any]:
#         """
#         Converte para dicionário otimizado para inserção em banco de dados.
#         Remove campos None, formata datas como string e exclui objetos aninhados.

#         Returns:
#             Dict[str, Any]: Dicionário pronto para DB
#         """
#         return self.to_dict(
#             include_none=False,
#             datetime_format='string',
#             include_nested=False
#         )

#     @classmethod
#     def from_dict(cls, data: Dict[str, Any]) -> 'BaseUser':
#         """Create User from dictionary"""
#         if not data:
#             return cls()

#         user_data = data.copy()

#         # Handle datetime fields
#         datetime_fields = [
#             'GDPRSignatureDateTime', 'LastModifDateTime',
#             'CHStartValidityDateTime', 'CHEndValidityDateTime',
#             'AuxDte01', 'AuxDte02'
#         ]

#         for date_field in datetime_fields:
#             if date_field in user_data and user_data[date_field]:
#                 if isinstance(user_data[date_field], str):
#                     try:
#                         user_data[date_field] = datetime.fromisoformat(
#                             user_data[date_field].replace('Z', '+00:00')
#                         )
#                     except ValueError:
#                         user_data[date_field] = None

#         # Handle nested objects (assumindo que existem essas classes)
#         nested_objects = {}
#         if 'Cards' in user_data and user_data['Cards']:
#             # Assumindo que Card tem método from_dict
#             nested_objects['Cards'] = [
#                 Card.from_dict(card) if hasattr(Card, 'from_dict') else card
#                 for card in user_data['Cards']
#             ]

#         if 'CHAccessLevels' in user_data and user_data['CHAccessLevels']:
#             # Assumindo que CHAccessLevel tem método from_dict
#             nested_objects['CHAccessLevels'] = [
#                 CHAccessLevel.from_dict(level) if hasattr(CHAccessLevel, 'from_dict') else level
#                 for level in user_data['CHAccessLevels']
#             ]

#         # Remove nested objects from user_data
#         for key in ['Cards', 'CHAccessLevels']:
#             user_data.pop(key, None)

#         # Filter only valid fields
#         valid_fields = set(cls.__dataclass_fields__.keys())
#         filtered_data = {k: v for k, v in user_data.items() if k in valid_fields}
#         filtered_data.update(nested_objects)

#         return cls(**filtered_data)



# # Exemplo de uso
# if __name__ == "__main__":
#     # Criar um usuário de exemplo
#     user = BaseUser(
#         CHID=12345,
#         FirstName="João",
#         LastName="Silva",
#         EMail="joao.silva@email.com",
#         CompanyID=100,
#         GDPRSignatureDateTime=datetime(2024, 1, 15, 10, 30, 0),
#         IsValidGDPRSignature=True,
#         Cards=[
#             Card(id=1, number="123456", active=True),
#             Card(id=2, number="789012", active=False)
#         ],
#         CHAccessLevels=[
#             CHAccessLevel(level_id=1, level_name="Basic", permissions=100),
#             CHAccessLevel(level_id=2, level_name="Advanced", permissions=200)
#         ]
#     )
    
#     print("=== Dicionário Completo ===")
#     dict_completo = user.to_dict()
#     print(json.dumps(dict_completo, indent=2, ensure_ascii=False, default=str))
    
#     print("\n=== Dicionário sem None ===")
#     dict_sem_none = user.to_dict(include_none=False)
#     print(json.dumps(dict_sem_none, indent=2, ensure_ascii=False, default=str))
    
#     print("\n=== Dicionário para DB ===")
#     dict_db = user.to_dict_for_db()
#     print(json.dumps(dict_db, indent=2, ensure_ascii=False))
    
#     print("\n=== Dicionário Achatado ===")
#     dict_flat = user.to_dict_flat()
#     print(json.dumps(dict_flat, indent=2, ensure_ascii=False, default=str))
    
#     print("\n=== JSON String ===")
#     json_string = user.to_json(include_none=False, datetime_format='string')
#     print(json_string)