from fastapi import APIRouter, HTTPException, Header, Request, Depends
from fastapi.responses import JSONResponse
from typing import List, Dict
from datetime import timedelta
import jwt
import base64

from src.utils.mock_ldap import (
    MockLDAPAuthenticator,
    create_access_token,
    find_user_by_identifier,
    send_password_reset_email,
    extract_markets_from_groups,
    authenticate,
    verify_token,
    SECRET_KEY,
    ALGORITHM,
    ACCESS_TOKEN_EXPIRE_MINUTES
)
from src.utils.logger import logger


auth_router = APIRouter(tags=["authentication"])
ldap_auth = MockLDAPAuthenticator()

def parse_basic_auth(authorization: str) -> tuple[str, str]:
    """Parse Basic Auth header and return username, password"""
    try:
        if not authorization.startswith('Basic '):
            raise ValueError("Invalid authorization header format")
        
        encoded_credentials = authorization[6:]
        decoded_credentials = base64.b64decode(encoded_credentials).decode('utf-8')
        username, password = decoded_credentials.split(':', 1)
        return username, password
    except Exception as e:
        logger.error("Failed to parse Basic Auth header: %s", str(e))
        raise HTTPException(status_code=401, detail="Invalid authorization header")

@auth_router.post("/login")
async def login(authorization: str = Header(..., description="Basic Auth header")):
    """Login endpoint expecting Basic Auth header"""
    username, password = parse_basic_auth(authorization)
    
    logger.info("LOGIN_START - Username: %s", username)
    
    try:
        user = authenticate(username, password)
        
        if user:
            logger.info("LDAP_AUTH_SUCCESS - User: %s", username)

            user_attributes: dict = user.get("attributes", {})
            member_of = user_attributes.get("memberOf", [])
            markets = extract_markets_from_groups(member_of)
            uid = user_attributes.get("uid", [None])[0]
            if not uid:
                logger.error("LOGIN_FAILED - Could not find UID for user: %s", username)
                raise HTTPException(status_code=500, detail="User configuration error: UID not found.")
            
            logger.info("MARKETS_EXTRACTED - User: %s, Markets: %s", username, markets)

            if not markets:
                logger.warning("LOGIN_FAILED - No markets found for user: %s", username)
                raise HTTPException(status_code=401, detail="No markets found for user")

            access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
            access_token = create_access_token(
                data={"sub": username, "markets": markets, "uid": uid}, 
                expires_delta=access_token_expires
            )
            
            logger.info("LOGIN_SUCCESS - User: %s, Markets: %s", username, markets)

            response_data = {
                "message": "Login successful",
                "user": {
                    "username": user["username"],
                    "email": user["email"],
                    "displayName": user["displayName"],
                    "department": user["department"],
                    "title": user["title"],
                    "uid": uid
                },
                "markets": markets,
                "token_type": "bearer"
            }
            
            response = JSONResponse(content=response_data)
            
            response.set_cookie(
                key="access_token",
                value=access_token,
                httponly=True,
                secure=True,
                samesite="strict",
                max_age=ACCESS_TOKEN_EXPIRE_MINUTES * 60
            )
            
            return response
        else:
            logger.warning("LOGIN_FAILED - Invalid credentials for user: %s", username)
            raise HTTPException(status_code=401, detail="Invalid credentials")
            
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("LOGIN_CRITICAL_ERROR - Username: %s, Error: %s", username, str(e))
        raise HTTPException(status_code=500, detail="Internal server error during login")

@auth_router.post("/logout")
async def logout():
    """Logout endpoint that clears the authentication cookie"""
    logger.info("LOGOUT_REQUEST")
    
    response = JSONResponse(content={"message": "Logged out successfully"})
    
    response.delete_cookie(
        key="access_token",
        httponly=True,
        secure=True,
        samesite="strict"
    )
    
    logger.info("LOGOUT_SUCCESS - Cookie cleared")
    return response

@auth_router.get("/me")
async def get_current_user(current_user: Dict = Depends(verify_token)):
    """
    Get current user info from a valid token.
    This is a protected endpoint.
    """
    
    logger.info("GET_CURRENT_USER - User: %s, UID: %s, Markets: %s", 
                current_user.get('username'), current_user.get('uid'), current_user.get('markets'))
    
    return {
        "username": current_user.get('username'),
        "uid": current_user.get('uid'),
        "markets": current_user.get('markets', []),
        "authenticated": True
    }

@auth_router.post("/forgot-password")
async def forgot_password(request: Request):
    try:
        body = await request.json()
        identifier = body.get("identifier")
        if not identifier:
            raise HTTPException(status_code=400, detail="Identifier is required")
    except Exception as e:
        logger.error("FORGOT_PASSWORD_BODY_ERROR: %s", str(e))
        raise HTTPException(status_code=400, detail="Invalid request body")
    
    logger.info("PASSWORD_RESET_START - Identifier: %s", identifier)
    
    try:
        user = find_user_by_identifier(identifier)
        if not user:
            logger.warning("PASSWORD_RESET_USER_NOT_FOUND - Identifier: %s", identifier)
            return {"message": "If the username or email exists, a password reset link has been sent."}
        
        logger.info("PASSWORD_RESET_USER_FOUND - Username: %s", user["username"])
        
        reset_token_expires = timedelta(hours=1)
        reset_token = create_access_token(
            data={"sub": user["username"], "type": "password_reset", "email": user["email"]},
            expires_delta=reset_token_expires
        )
        
        email_sent = send_password_reset_email(user, reset_token)
        
        if email_sent:
            logger.info("PASSWORD_RESET_EMAIL_SENT - Username: %s", user["username"])
            return {"message": "If the username or email exists, a password reset link has been sent."}
        else:
            logger.error("PASSWORD_RESET_EMAIL_FAILED - Username: %s", user["username"])
            raise HTTPException(status_code=500, detail="Failed to send password reset email. Please try again later.")
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("PASSWORD_RESET_CRITICAL_ERROR - Identifier: %s, Error: %s", identifier, str(e))
        raise HTTPException(status_code=500, detail="An error occurred while processing your request. Please try again later.")