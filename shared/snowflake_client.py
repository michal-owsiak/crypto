import os
from pathlib import Path
from dotenv import load_dotenv
import snowflake.connector
from cryptography.hazmat.primitives import serialization

try:
    import streamlit as st
except Exception:
    st = None


load_dotenv(Path(__file__).resolve().parents[1] / ".env")


def get_secret(name, default=None):
    if st is not None:
        try:
            if name in st.secrets:
                return st.secrets[name]
        except Exception:
            pass

    value = os.getenv(name, default)
    if value is None:
        raise KeyError(f"Missing secret/env: {name}")
    return value


def _pem_to_der_bytes(private_key_value: str) -> bytes:
    if not isinstance(private_key_value, str):
        raise TypeError(
            f"SNOWFLAKE_PRIVATE_KEY must be a string, got {type(private_key_value).__name__}"
        )

    private_key_pem = private_key_value.strip()

    if private_key_pem.startswith('"') and private_key_pem.endswith('"'):
        private_key_pem = private_key_pem[1:-1]
    if private_key_pem.startswith("'") and private_key_pem.endswith("'"):
        private_key_pem = private_key_pem[1:-1]

    private_key_pem = private_key_pem.replace("\\n", "\n")
    private_key_pem = private_key_pem.replace("\r\n", "\n").replace("\r", "\n")

    p_key = serialization.load_pem_private_key(
        private_key_pem.encode("utf-8"),
        password=None,
    )

    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def _get_private_key_bytes():
    private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if private_key_path and Path(private_key_path).exists():
        with open(private_key_path, "rb") as f:
            p_key = serialization.load_pem_private_key(f.read(), password=None)
        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    private_key_value = get_secret("SNOWFLAKE_PRIVATE_KEY")

    if isinstance(private_key_value, str):
        possible_path = Path(private_key_value)
        if possible_path.exists() and possible_path.is_file():
            with open(possible_path, "rb") as f:
                p_key = serialization.load_pem_private_key(f.read(), password=None)
            return p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

        return _pem_to_der_bytes(private_key_value)

    raise TypeError(
        "SNOWFLAKE_PRIVATE_KEY must be either a file path string or a PEM string."
    )


def get_connection():
    return snowflake.connector.connect(
        user=get_secret("SNOWFLAKE_USER"),
        account=get_secret("SNOWFLAKE_ACCOUNT"),
        private_key=_get_private_key_bytes(),
        warehouse=get_secret("SNOWFLAKE_WAREHOUSE"),
        database=get_secret("SNOWFLAKE_DATABASE"),
        schema=get_secret("SNOWFLAKE_DBT_SCHEMA"),
        role=get_secret("SNOWFLAKE_ROLE", None),
    )