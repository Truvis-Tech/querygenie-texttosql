from typing import Dict, Optional, List
from datetime import datetime, timedelta
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import Depends, HTTPException, status, Request
from pydantic import BaseModel
import jwt
import re
import logging

# Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# JWT Config
SECRET_KEY = "your-secret-key-change-this-in-production"
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 300

class MockLDAPAuthenticator:
    """Mock LDAP authenticator for testing."""
    MOCK_USERS = {
        "user1": {
            "password": "pass1",
            "attributes": {
                "cn": ["User One"],
                "uid": ["543531"],
                "mail": ["user1@example.com"],
                "displayName": ["User One"],
                "givenName": ["User"],
                # Remove market attribute
                "sn": ["One"],
                "department": ["Engineering"],
                "title": ["Software Engineer"],
                "manager": ["cn=Manager One,ou=users,dc=example,dc=com"],
                "memberOf": [
                    "cn=developers,ou=groups,dc=example,dc=com",
                    "cn=employees,ou=groups,dc=example,dc=com",
                    "cn=us_market,ou=markets,dc=example,dc=com",  # US market
                    "cn=uk_market,ou=markets,dc=example,dc=com"   # UK market
                ],
                "objectClass": ["inetOrgPerson", "organizationalPerson", "person"],
                "dn": "uid=user1,ou=users,dc=example,dc=com"  # Add DN here
            }
        },
        "user2": {
            "password": "pass2",
            "attributes": {
                "cn": ["User Two"],
                "uid": ["45463"],
                "mail": ["user2@example.com"],
                "displayName": ["User Two"],
                "givenName": ["User"],
                "sn": ["Two"],
                "department": ["Sales"],
                "title": ["Sales Manager"],
                "manager": ["cn=Manager Two,ou=users,dc=example,dc=com"],
                "memberOf": [
                    "cn=sales,ou=groups,dc=example,dc=com",
                    "cn=managers,ou=groups,dc=example,dc=com",
                    "cn=employees,ou=groups,dc=example,dc=com",
                    "cn=ca_market,ou=markets,dc=example,dc=com",  # CA market
                    "cn=de_market,ou=markets,dc=example,dc=com",   # DE market
                    "cn=jp_market,ou=markets,dc=example,dc=com"    # JP market
                ],
                "objectClass": ["inetOrgPerson", "organizationalPerson", "person"],
                "dn": "uid=user2,ou=users,dc=example,dc=com"  # Add DN here
            }
        },
        "admin": {
            "password": "admin123",
            "attributes": {
                "cn": ["Admin User"],
                "uid": ["364632"],
                "mail": ["admin@example.com"],
                "displayName": ["Administrator"],
                "givenName": ["Admin"],
                "sn": ["User"],
                "department": ["IT"],
                "title": ["System Administrator"],
                "manager": ["cn=CTO,ou=users,dc=example,dc=com"],
                "memberOf": [
                    "cn=admins,ou=groups,dc=example,dc=com",
                    "cn=it,ou=groups,dc=example,dc=com",
                    "cn=employees,ou=groups,dc=example,dc=com",
                    "cn=us_market,ou=markets,dc=example,dc=com",
                    "cn=uk_market,ou=markets,dc=example,dc=com",
                    "cn=ca_market,ou=markets,dc=example,dc=com",
                    "cn=de_market,ou=markets,dc=example,dc=com",
                    "cn=jp_market,ou=markets,dc=example,dc=com"
                ],
                "objectClass": ["inetOrgPerson", "organizationalPerson", "person"],
                "dn": "uid=admin,ou=users,dc=example,dc=com"  # Add DN here
            }
        }
    }
    def __init__(self, ldap_server: str = "ldap://mock.example.com", base_dn: str = "ou=users,dc=example,dc=com"):
        self.ldap_server = ldap_server
        self.base_dn = base_dn


security = HTTPBearer(auto_error=False)  # Don't auto-error to allow cookie fallback
ldap_auth = MockLDAPAuthenticator()

class TokenData(BaseModel):
    username: str
    markets: List[str]

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=15))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(request: Request, credentials: HTTPAuthorizationCredentials = Depends(security)):
    """
    Verify token from either:
    1. Cookie (preferred)
    2. Authorization header (Bearer token)
    """
    token = None
    
    try:
        # First try to get token from cookie
        token = request.cookies.get("access_token")
        
        # If no cookie, try Authorization header
        if not token and credentials:
            token = credentials.credentials
        
        if not token:
            logger.warning("TOKEN_VERIFICATION_FAILED - No token provided")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="No authentication token provided",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        # Decode and verify token
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        markets: List[str] = payload.get("markets", [])
        
        if not username:
            logger.warning("TOKEN_VERIFICATION_FAILED - No username in token")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid token: no username",
                headers={"WWW-Authenticate": "Bearer"},
            )
        
        logger.debug("TOKEN_VERIFICATION_SUCCESS - User: %s, Markets: %s", username, markets)
        return {"username": username, "markets": markets, "uid": payload.get("uid")}
        
    except jwt.ExpiredSignatureError:
        logger.warning("TOKEN_VERIFICATION_FAILED - Token expired")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except jwt.PyJWTError as e:
        logger.warning("TOKEN_VERIFICATION_FAILED - JWT error: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    except Exception as e:
        logger.exception("TOKEN_VERIFICATION_ERROR - Unexpected error: %s", str(e))
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error during token verification"
        )

def find_user_by_identifier(identifier: str) -> Optional[dict]:
    mock_users = [
        {
            "username": "john_doe",
            "email": "john@example.com",
            "name": "John Doe",
            "markets": ["US", "EU"]
        },
        {
            "username": "jane_smith",
            "email": "jane@example.com",
            "name": "Jane Smith",
            "markets": ["CA", "DE", "JP"]
        },
        {
            "username": "admin",
            "email": "admin@company.com",
            "name": "Administrator",
            "markets": ["US", "EU", "CA", "DE", "JP"]
        }
    ]
    for user in mock_users:
        if identifier.lower() in [user["username"].lower(), user["email"].lower()]:
            return user
    return None

def send_password_reset_email(user: dict, reset_token: str) -> bool:
    logger.info(f"Sending password reset email to {user['email']} with token {reset_token}")
    return True  # Mock success

def extract_markets_from_groups(member_of):
    markets = []
    market_prefix = "cn="
    market_suffix = "_market,ou=markets,"
    
    for group in member_of:
        if market_suffix in group:
            market = group.split(market_prefix)[1].split("_market")[0].upper()
            if market:
                markets.append(market)
    return markets

def authenticate(username: str, password: str) -> Optional[Dict]:
    user = ldap_auth.MOCK_USERS.get(username)
    if user and user["password"] == password:
        return {
            "username": username,
            "dn": user["attributes"]["dn"],
            "email": user["attributes"]["mail"][0],
            "displayName": user["attributes"]["displayName"][0],
            "department": user["attributes"]["department"][0],
            "title": user["attributes"]["title"][0],
            "groups": user["attributes"]["memberOf"],
            "attributes": user["attributes"],
            "authenticated_at": datetime.utcnow().isoformat()
        }
    return None

MARKET_GROUPS = {
    "US": "cn=us_market,ou=markets,dc=example,dc=com",
    "UK": "cn=uk_market,ou=markets,dc=example,dc=com",
    "CA": "cn=ca_market,ou=markets,dc=example,dc=com",
    "DE": "cn=de_market,ou=markets,dc=example,dc=com",
    "JP": "cn=jp_market,ou=markets,dc=example,dc=com"
}