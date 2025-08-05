"""
Encryption utility for CalDAV Mirror

Uses Fernet symmetric encryption from the cryptography library to secure
sensitive data, such as OAuth tokens, before storing them in the database.
"""

import base64
import logging
from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

logger = logging.getLogger(__name__)

class Encryption:
    """Handles encryption and decryption of data."""

    def __init__(self, secret_key: str):
        if not secret_key or len(secret_key) < 16:
            raise ValueError("Encryption key must be at least 16 characters long.")
        self.fernet = self._get_fernet_instance(secret_key)

    def _get_fernet_instance(self, secret_key: str) -> Fernet:
        """
        Derives a key from the secret and returns a Fernet instance.
        This ensures the key is always the correct format for Fernet.
        """
        # Use a static salt for deterministic key generation from the secret key.
        # This is acceptable here because we are using a user-provided secret.
        salt = b'caldav-mirror-salt' 
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(secret_key.encode()))
        return Fernet(key)

    def encrypt(self, data: str) -> str:
        """
        Encrypts a string.

        Args:
            data: The string to encrypt.

        Returns:
            The encrypted string, safe for storage.
        """
        try:
            encrypted_data = self.fernet.encrypt(data.encode('utf-8'))
            return encrypted_data.decode('utf-8')
        except Exception as e:
            logger.error(f"Encryption failed: {e}", exc_info=True)
            raise

    def decrypt(self, encrypted_data: str) -> str:
        """
        Decrypts a string.

        Args:
            encrypted_data: The encrypted string to decrypt.

        Returns:
            The original, decrypted string.
        """
        try:
            decrypted_data = self.fernet.decrypt(encrypted_data.encode('utf-8'))
            return decrypted_data.decode('utf-8')
        except InvalidToken:
            logger.error("Decryption failed: Invalid token. The encryption key may have changed.")
            raise
        except Exception as e:
            logger.error(f"Decryption failed: {e}", exc_info=True)
            raise