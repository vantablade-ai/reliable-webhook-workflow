import hashlib
import hmac


def sign_body(raw_body: bytes, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()


def verify_signature(raw_body: bytes, signature: str | None, secret: str) -> bool:
    if not signature or len(signature) != 64:
        return False
    try:
        int(signature, 16)
    except ValueError:
        return False
    return hmac.compare_digest(sign_body(raw_body, secret), signature.lower())
