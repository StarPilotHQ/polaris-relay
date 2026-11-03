"""Environment settings: only what the UI can't set. Everything else lives in the database (see `db.Setting`)."""
import logging
import os
import secrets
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

log = logging.getLogger("relay")


class StartupError(Exception):
  pass


def _split(value: str) -> list[str]:
  return [v.strip() for v in value.split(",") if v.strip()]


@dataclass
class Config:
  data_dir: Path = Path("/data")
  polaris_host: str = ""
  auth_user: str | None = None
  auth_hash: bytes | None = None
  insecure_no_auth: bool = False
  allowed_hosts: list[str] = field(default_factory=list)
  allowed_origins: list[str] = field(default_factory=list)
  trusted_proxies: list[str] = field(default_factory=list)
  origins_given: bool = False
  comma_api: str = "https://api.commadotai.com"
  athena_api: str = "https://athena.comma.ai"
  registry: str = "https://ghcr.io"
  image: str = "starpilothq/polaris-relay"
  tick_s: float = 60.0
  warnings: list[str] = field(default_factory=list)

  @classmethod
  def from_env(cls, env: dict | None = None) -> "Config":
    env = os.environ if env is None else env
    c = cls(data_dir=Path(env.get("DATA_DIR") or "/data"))
    c.polaris_host = (env.get("POLARIS_HOST") or "").strip().rstrip("/")
    if not c.polaris_host:
      raise StartupError("This image was built without its site address. Use an official image, or set POLARIS_HOST.")
    if not c.polaris_host.startswith(("https://", "http://")):
      c.polaris_host = "https://" + c.polaris_host
    password = (env.get("AUTH_PASSWORD") or "").strip()
    c.insecure_no_auth = env.get("INSECURE_ALLOW_NO_AUTH", "").strip() in ("1", "true", "yes")
    if password:
      c.auth_user, c.auth_hash = parse_htpasswd(password)
    elif not c.insecure_no_auth:
      raise StartupError("No sign-in is configured. Set AUTH_PASSWORD to an htpasswd line with a bcrypt hash (for "
                         "example from `htpasswd -nbBC 12 admin 'your password'`), or set INSECURE_ALLOW_NO_AUTH=1 if your "
                         "own reverse proxy already asks for a sign-in.")
    if c.insecure_no_auth and password:
      c.insecure_no_auth = False
      c.warnings.append("Both AUTH_PASSWORD and INSECURE_ALLOW_NO_AUTH are set; the password sign-in is used.")
    elif c.insecure_no_auth:
      c.warnings.append("INSECURE_ALLOW_NO_AUTH=1: there is no sign-in. Anyone who can reach this page controls your "
                        "comma token. Make sure your reverse proxy asks for one.")
    c.allowed_hosts = [h.lower() for h in _split(env.get("ALLOWED_HOSTS", ""))]
    c.allowed_origins = [o.rstrip("/").lower() for o in _split(env.get("ALLOWED_ORIGINS", ""))]
    if not c.allowed_hosts:
      raise StartupError("Set ALLOWED_HOSTS to the host names you open the page on (for example "
                         "relay.example.com,localhost:8080). It stops other sites from reaching it through your browser.")
    c.origins_given = bool(c.allowed_origins)
    if not c.allowed_origins:
      c.allowed_origins = [f"{scheme}://{h}" for h in c.allowed_hosts if h != "*" for scheme in ("http", "https")]
      if "*" in c.allowed_hosts:
        c.allowed_origins.append("*")
    if "*" in c.allowed_hosts:
      c.warnings.append("ALLOWED_HOSTS is *: requests for any host name are answered, which allows DNS rebinding.")
    if "*" in c.allowed_origins:
      c.warnings.append("ALLOWED_ORIGINS is *: forms are accepted from any site's origin; only the CSRF token "
                        "protects them.")
    c.trusted_proxies = _split(env.get("TRUSTED_PROXIES", ""))
    for name in ("COMMA_API", "ATHENA_API", "REGISTRY"):
      if env.get(f"RELAY_{name}"):
        setattr(c, name.lower(), env[f"RELAY_{name}"].rstrip("/"))
    if env.get("RELAY_TICK_S"):
      c.tick_s = float(env["RELAY_TICK_S"])
    return c

  @property
  def secure_cookies(self) -> bool:
    """Secure cookies once any origin you named is https. The default origins list both schemes for each host, which
    says nothing about how it's served, so they leave plain http sign-in working."""
    return self.origins_given and any(o.startswith("https://") for o in self.allowed_origins)

  def host_allowed(self, host: str | None) -> bool:
    return "*" in self.allowed_hosts or (host or "").lower() in self.allowed_hosts

  def origin_allowed(self, origin: str | None) -> bool:
    return "*" in self.allowed_origins or (origin or "").rstrip("/").lower() in self.allowed_origins

  def site_url(self, path: str = "") -> str:
    return self.polaris_host + path

  @property
  def site_host(self) -> str:
    return urlsplit(self.polaris_host).netloc

  def secret_key(self) -> str:
    """The session signing key, generated into the data volume on first start."""
    path = self.data_dir / "secret.key"
    if not path.exists():
      path.parent.mkdir(parents=True, exist_ok=True)
      path.write_text(secrets.token_urlsafe(48))
      path.chmod(0o600)
    return path.read_text().strip()


def parse_htpasswd(line: str) -> tuple[str, bytes]:
  user, sep, hashed = line.partition(":")
  if not sep or not user or not hashed.startswith(("$2y$", "$2b$", "$2a$")):
    raise StartupError("AUTH_PASSWORD must be one htpasswd line with a bcrypt hash, like `admin:$2y$12$...` (made with "
                       "`htpasswd -nbBC 12 admin 'your password'`). In docker-compose, write each $ as $$.")
  # Apache writes $2y$, which is the same algorithm as $2b$; normalize so every bcrypt build accepts it.
  return user, ("$2b$" + hashed[4:]).encode()
