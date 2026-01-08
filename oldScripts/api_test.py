import os, uuid, requests, base64, json
from dotenv import load_dotenv

def must(name: str) -> str:
    v = os.getenv(name, "").strip()
    if not v:
        raise SystemExit(f"Missing env var: {name}")
    return v

def decode_jwt_payload(token: str) -> dict:
    parts = token.split(".")
    if len(parts) < 2:
        return {}
    payload_b64 = parts[1] + "=" * (-len(parts[1]) % 4)
    payload = base64.urlsafe_b64decode(payload_b64.encode("utf-8"))
    return json.loads(payload.decode("utf-8", errors="replace"))

def main():
    load_dotenv()

    client_id = must("BOLAGSVERKET_CLIENT_ID")
    client_secret = must("BOLAGSVERKET_CLIENT_SECRET")
    token_url = must("BOLAGSVERKET_TOKEN_URL")
    base_url = must("BOLAGSVERKET_BASE_URL")
    scope = must("BOLAGSVERKET_SCOPE")

    # 1) token
    tok = requests.post(
        token_url,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": scope,
        },
        timeout=20,
    )
    print("TOKEN:", tok.status_code)
    if tok.status_code != 200:
        print(tok.text[:2000])
        return

    token_json = tok.json()
    access_token = token_json.get("access_token")
    print("expires_in:", token_json.get("expires_in"))

    # 1b) visa scopes i token (utan att visa token)
    payload = decode_jwt_payload(access_token)
    print("token scope claim:", payload.get("scope") or payload.get("scp"))
    print("token aud:", payload.get("aud"))
    print("token iss:", payload.get("iss"))

    # 2) /organisationer
    url = f"{base_url}/organisationer"
    orgnr = os.getenv("TEST_ORGNR", "5562086107").strip()

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
            "X-Request-Id": str(uuid.uuid4()),
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        json={"identitetsbeteckning": orgnr},
        timeout=30,
    )

    print("ORG:", resp.status_code)
    print("ORG body:", resp.text[:2000])

if __name__ == "__main__":
    main()
