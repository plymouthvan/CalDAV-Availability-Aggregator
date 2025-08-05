"""
Google OAuth 2.0 Device Flow implementation for CalDAV Mirror

Handles authentication with Google Calendar API using the device flow,
which is ideal for headless applications.
"""

import asyncio
import aiohttp
import json
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from urllib.parse import urlencode
import os
from utils.encryption import Encryption

logger = logging.getLogger(__name__)


class GoogleOAuth:
    """Google OAuth 2.0 Device Flow handler."""
    
    # Google OAuth 2.0 endpoints
    DEVICE_CODE_URL = "https://oauth2.googleapis.com/device/code"
    TOKEN_URL = "https://oauth2.googleapis.com/token"
    
    # Required scopes for Google Calendar
    SCOPES = [
        "https://www.googleapis.com/auth/calendar"
    ]
    
    def __init__(self, client_id: str, client_secret: str, encryption_key: str, database=None):
        self.client_id = client_id
        self.client_secret = client_secret
        self.database = database
        self.encryption = Encryption(encryption_key)
        self._access_token: Optional[str] = None
        self._refresh_token: Optional[str] = None
        self._token_expires_at: Optional[datetime] = None
    
    async def get_access_token(self) -> Optional[str]:
        """
        Get a valid access token, refreshing if necessary.
        
        Returns:
            Valid access token or None if authentication fails
        """
        # Try to load existing token from database
        if not self._access_token and self.database:
            await self._load_tokens_from_db()
        
        # Check if current token is still valid
        if self._access_token and self._token_expires_at:
            if datetime.utcnow() < self._token_expires_at - timedelta(minutes=5):
                return self._access_token
        
        # Try to refresh token
        if self._refresh_token:
            if await self._refresh_access_token():
                return self._access_token
        
        # Need to perform device flow authentication
        logger.info("No valid token found, starting device flow authentication...")
        if await self._perform_device_flow():
            return self._access_token
        
        return None
    
    async def _perform_device_flow(self) -> bool:
        """
        Perform the OAuth 2.0 device flow authentication.
        
        Returns:
            True if authentication successful, False otherwise
        """
        try:
            # Step 1: Get device code
            device_response = await self._request_device_code()
            if not device_response:
                return False
            
            device_code = device_response['device_code']
            user_code = device_response['user_code']
            verification_url = device_response['verification_url']
            interval = device_response.get('interval', 5)
            expires_in = device_response.get('expires_in', 1800)
            
            # Step 2: Display instructions to user
            logger.info("=" * 60)
            logger.info("GOOGLE CALENDAR AUTHENTICATION REQUIRED")
            logger.info("=" * 60)
            logger.info(f"1. Open this URL in your browser: {verification_url}")
            logger.info(f"2. Enter this code: {user_code}")
            logger.info("3. Complete the authorization process")
            logger.info("4. Return to this terminal and wait...")
            logger.info("=" * 60)
            
            # Step 3: Poll for token
            start_time = datetime.utcnow()
            while (datetime.utcnow() - start_time).seconds < expires_in:
                await asyncio.sleep(interval)
                
                token_response = await self._poll_for_token(device_code)
                if token_response:
                    if 'access_token' in token_response:
                        # Success!
                        await self._store_tokens(token_response)
                        logger.info("Authentication successful!")
                        return True
                    elif token_response.get('error') == 'authorization_pending':
                        # Still waiting for user
                        continue
                    elif token_response.get('error') == 'slow_down':
                        # Increase polling interval
                        interval += 5
                        continue
                    else:
                        # Other error
                        logger.error(f"Authentication error: {token_response.get('error_description', 'Unknown error')}")
                        return False
            
            logger.error("Authentication timed out")
            return False
            
        except Exception as e:
            logger.error(f"Device flow authentication failed: {e}")
            return False
    
    async def _request_device_code(self) -> Optional[Dict[str, Any]]:
        """Request device code from Google."""
        data = {
            'client_id': self.client_id,
            'scope': ' '.join(self.SCOPES)
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.DEVICE_CODE_URL,
                data=data,
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            ) as response:
                if response.status == 200:
                    return await response.json()
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to request device code: {response.status} - {error_text}")
                    return None
    
    async def _poll_for_token(self, device_code: str) -> Optional[Dict[str, Any]]:
        """Poll Google for access token."""
        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'device_code': device_code,
            'grant_type': 'urn:ietf:params:oauth:grant-type:device_code'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.TOKEN_URL,
                data=data,
                headers={'Content-Type': 'application/x-www-form-urlencoded'}
            ) as response:
                return await response.json()
    
    async def _refresh_access_token(self) -> bool:
        """Refresh the access token using refresh token."""
        if not self._refresh_token:
            return False
        
        data = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'refresh_token': self._refresh_token,
            'grant_type': 'refresh_token'
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.TOKEN_URL,
                    data=data,
                    headers={'Content-Type': 'application/x-www-form-urlencoded'}
                ) as response:
                    if response.status == 200:
                        token_response = await response.json()
                        await self._store_tokens(token_response, keep_refresh_token=True)
                        logger.info("Access token refreshed successfully")
                        return True
                    else:
                        logger.error(f"Failed to refresh token: {response.status}")
                        return False
        except Exception as e:
            logger.error(f"Token refresh failed: {e}")
            return False
    
    async def _store_tokens(self, token_response: Dict[str, Any], keep_refresh_token: bool = False):
        """Store tokens in memory and database."""
        self._access_token = token_response['access_token']
        
        if not keep_refresh_token and 'refresh_token' in token_response:
            self._refresh_token = token_response['refresh_token']
        
        expires_in = token_response.get('expires_in', 3600)
        self._token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
        
        # Store in database if available
        if self.database:
            token_data = {
                'access_token': self._access_token,
                'refresh_token': self._refresh_token,
                'expires_at': self._token_expires_at.isoformat()
            }
            
            # Encrypt token data before storing
            token_json = json.dumps(token_data)
            encrypted_token = self.encryption.encrypt(token_json)
            
            await self.database.store_auth_token(
                'google_calendar',
                encrypted_token,
                self._token_expires_at
            )
    
    async def _load_tokens_from_db(self):
        """Load tokens from database."""
        if not self.database:
            return
        
        try:
            encrypted_token = await self.database.get_auth_token('google_calendar')
            if encrypted_token:
                # Decrypt token data
                decrypted_token = self.encryption.decrypt(encrypted_token)
                token_data = json.loads(decrypted_token)
                
                self._access_token = token_data.get('access_token')
                self._refresh_token = token_data.get('refresh_token')
                
                expires_at_str = token_data.get('expires_at')
                if expires_at_str:
                    self._token_expires_at = datetime.fromisoformat(expires_at_str)
                
                logger.info("Loaded existing tokens from database")
        except Exception as e:
            logger.error(f"Failed to load tokens from database: {e}")
    
    async def revoke_token(self):
        """Revoke the current access token."""
        if not self._access_token:
            return
        
        revoke_url = f"https://oauth2.googleapis.com/revoke?token={self._access_token}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(revoke_url) as response:
                    if response.status == 200:
                        logger.info("Token revoked successfully")
                    else:
                        logger.warning(f"Token revocation returned status: {response.status}")
        except Exception as e:
            logger.error(f"Failed to revoke token: {e}")
        
        # Clear tokens
        self._access_token = None
        self._refresh_token = None
        self._token_expires_at = None