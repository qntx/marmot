"""Regenerate v2-crypto.json from the spec text, independently of any Rust code.

features/encrypted-media.md ("Key Derivation", "Encryption") at the pinned spec rev:
file_key = HKDF-Expand(media_secret, "encrypted-media-v2" 0x00 plaintext_sha256 0x00
                       media_type 0x00 filename 0x00 "key", 32)       (HKDF-SHA256, no Extract)
aad      = "encrypted-media-v2" 0x00 plaintext_sha256 0x00 media_type 0x00 filename
ct       = ChaCha20-Poly1305(file_key, nonce, plaintext, aad)

Run: python3 -m venv /tmp/v && /tmp/v/bin/pip install cryptography==50.0.2
     /tmp/v/bin/python vectors/encrypted-media/gen-v2-crypto.py > vectors/encrypted-media/v2-crypto.json
"""

import hashlib
import json

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import ChaCha20Poly1305
from cryptography.hazmat.primitives.kdf.hkdf import HKDFExpand

LABEL = b"encrypted-media-v2"


def aad(plaintext_sha256: bytes, media_type: str, filename: str) -> bytes:
    return b"\x00".join([LABEL, plaintext_sha256, media_type.encode(), filename.encode()])


def file_key(secret: bytes, plaintext_sha256: bytes, media_type: str, filename: str) -> bytes:
    info = aad(plaintext_sha256, media_type, filename) + b"\x00key"
    return HKDFExpand(hashes.SHA256(), 32, info).derive(secret)


def key_schedule_case(name, secret, plaintext_sha256, media_type, filename):
    return {
        "name": name,
        "media_secret_hex": secret.hex(),
        "plaintext_sha256": plaintext_sha256.hex(),
        "media_type": media_type,
        "filename": filename,
        "file_key_hex": file_key(secret, plaintext_sha256, media_type, filename).hex(),
        "aad_hex": aad(plaintext_sha256, media_type, filename).hex(),
    }


def encryption_case(name, secret, plaintext, media_type, filename, nonce):
    digest = hashlib.sha256(plaintext).digest()
    key = file_key(secret, digest, media_type, filename)
    ciphertext = ChaCha20Poly1305(key).encrypt(nonce, plaintext, aad(digest, media_type, filename))
    case = key_schedule_case(name, secret, digest, media_type, filename)
    case.update(
        {
            "plaintext_hex": plaintext.hex(),
            "nonce_hex": nonce.hex(),
            "ciphertext_hex": ciphertext.hex(),
            "ciphertext_sha256": hashlib.sha256(ciphertext).hexdigest(),
        }
    )
    return case


print(
    json.dumps(
        {
            "schema": "mk-encrypted-media-v2-crypto/v1",
            "description": "Known-answer values for the encrypted-media-v2 key schedule and file "
            "encryption, computed by gen-v2-crypto.py from the spec text with Python "
            "`cryptography` 50.0.2. `mdk-kdf-aad` reproduces the literal key and AAD asserted by "
            "MDK v0.16.0 crates/marmot-app/src/media/tests.rs "
            "(encrypted_media_v2_kdf_and_aad_are_independent_from_v1).",
            "key_schedule": [
                key_schedule_case(
                    "mdk-kdf-aad", bytes([7] * 32), bytes([0x22] * 32), "image/jpeg", " Photo.JPG "
                ),
            ],
            "encryption": [
                encryption_case(
                    "octet-stream-fixture",
                    bytes(range(1, 33)),
                    b"marmot encrypted-media-v2 known answer\n",
                    "application/octet-stream",
                    "fixture.bin",
                    bytes([3] * 12),
                ),
                encryption_case(
                    "utf8-filename-image",
                    bytes(range(0xA0, 0xC0)),
                    bytes(range(256)) * 3,
                    "image/png",
                    "fot\u00f6 \u732b.png",
                    bytes(range(12)),
                ),
            ],
        },
        indent=2,
        ensure_ascii=False,
    )
)
