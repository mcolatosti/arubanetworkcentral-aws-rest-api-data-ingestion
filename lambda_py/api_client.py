import os
import time
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
import json
import urllib.request
import urllib.error
import urllib.parse
from urllib.error import HTTPError
import threading

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class ArubaApiClient:

    def get(self, endpoint: str, params: Dict[str, Any] = None):
        """Public GET method for compatibility with ingestion handlers."""
        return self._get_json(endpoint, params)

    def get_switch_interfaces(self, serial: str, site_id: str = None) -> list:
        """Fetch interfaces for a switch given serial, passing site_id as a query parameter if required."""
        endpoint = f"/network-monitoring/v1alpha1/switch/{serial}/interfaces"
        params = {"site-id": site_id} if site_id else {}
        try:
            js = self._get_json(endpoint, params)
            for k in ("interfaces", "items", "data"):
                if isinstance(js, dict) and k in js and isinstance(js[k], list):
                    return js[k]
            if isinstance(js, list):
                return js
            return []
        except Exception as e:
            logger.error(f"[switch_interfaces] Failed for serial={serial} site_id={site_id}: {e}")
            return []
    """
    Aruba Central API client (clients-focused after device removal).
    """

    def __init__(self,
                 client_id: Optional[str],
                 client_secret: str,
                 customer_id: Optional[str],
                 base_url: str,
                 oauth_token_url: Optional[str] = None,
                 page_limit: int = 100,
                 early_expiry_buffer: int = 300,
                 page_delay_seconds: float = 2.0,
                 request_timeout: int = 30,
                 max_retries: int = 3):
        self.client_id = client_id
        self.client_secret = client_secret
        self.customer_id = customer_id
        self.base_url = base_url.rstrip("/")
        self.oauth_token_url = oauth_token_url or f"{self.base_url}/oauth2/token"
        self.page_limit = min(page_limit, 100)
        self.page_delay_seconds = page_delay_seconds
        self.early_expiry_buffer = early_expiry_buffer
        self.request_timeout = request_timeout
        self.max_retries = max_retries

        # Token cache (module-level persistence across warm starts)
        self._access_token: Optional[str] = None
        self._expires_at: Optional[datetime] = None

        self.min_interval = float(os.getenv("ARUBA_MIN_REQUEST_INTERVAL_SEC", "0.5"))  # base throttle
        self.max_backoff = float(os.getenv("ARUBA_MAX_BACKOFF_SEC", "30"))
        self._lock = threading.Lock()
        self._last_request_ts = 0.0
        self.max_pages_per_call = int(os.getenv("ARUBA_MAX_PAGES_PER_CALL", "60"))

        # Allow explicit env override for token URL precedence
        env_token_url = os.getenv("ARUBA_OAUTH_TOKEN_URL")
        if env_token_url:
            self.oauth_token_url = env_token_url
        # If secret omitted oauthTokenUrl and base_url is an API gateway host, prefer global SSO default
        if "/oauth2/token" in self.oauth_token_url and "api.central" in self.base_url and not env_token_url and not oauth_token_url:
            # fallback to common SSO endpoint (documented GreenLake)
            self.oauth_token_url = "https://sso.common.cloud.hpe.com/as/token.oauth2"

    # ---------- Public high-level ----------
    def list_sites(self) -> List[Dict[str, Any]]:
        sort_expr = os.getenv("ARUBA_SITES_SORT", "").strip()
        params: Dict[str, Any] = {}
        if sort_expr:
            params["sort"] = sort_expr
        return self._cursor_or_offset_collect(
            endpoint="/network-monitoring/v1alpha1/sites-health",
            root_keys=["sites", "items", "data"],
            params=params
        )

    def list_clients_single_site(self, site_id: str) -> List[Dict[str, Any]]:
        """Fetch clients for a site using status filter with cursor-first pagination.

        Logic:
        1. Build filter clause (status IN + optional lastSeenAt cutoff)
        2. Attempt cursor pagination using 'next' token if present in first response.
        3. Fallback to offset pagination if no 'next' is returned.
        """
        import urllib.parse
        endpoint = "/network-monitoring/v1alpha1/clients"

        try_global = os.getenv("ARUBA_CLIENTS_TRY_GLOBAL", "false").lower() == "true"
        variants = [
            ("site-id", {"site-id": site_id}),
            ("siteId", {"siteId": site_id}),
        ]
        if try_global:
            variants.append(("nosite", {}))

        def _collect_for_variant(variant_params: Dict[str, Any]) -> List[Dict[str, Any]]:
            base_params: Dict[str, Any] = {**variant_params}
            items = self._cursor_or_offset_collect(
                endpoint=endpoint,
                root_keys=["clients", "items", "data"],
                params=base_params,
                annotate=lambda c: c.setdefault("_site_id", site_id) if site_id else None,
                per_page_site_delay=True
            )
            return items

        for label, base in variants:
            vp = dict(base)
            items = _collect_for_variant(vp)
            if items:
                return items
        return []

    def list_devices(self) -> List[Dict[str, Any]]:
        """Fetch devices using cursor-based pagination with 'next' token.

        Aruba docs indicate a 'next' field for pagination. We repeatedly request
        pages until 'next' is null/missing or safety limits are hit.
        Deduplication by stable key (id/deviceId/macAddress/mac/serialNumber) is
        applied to avoid duplicates if API returns overlapping pages.
        """
        import urllib.request, urllib.parse, json as _json
        endpoint = "/network-monitoring/v1alpha1/devices"
        collected: List[Dict[str, Any]] = []
        seen_keys: set = set()
        page = 0
        next_token: Optional[str] = None
        limit = self.page_limit
        # The API allows up to 100 (docs say default 1000 but max 100). Ensure <=100.
        if limit > 100:
            limit = 100
        base_params = {"limit": limit}
        sort_expr = os.getenv("ARUBA_DEVICE_SORT", "")
        if sort_expr:
            base_params["sort"] = sort_expr
        while True:
            page += 1
            params = dict(base_params)
            if page > 1 and next_token:
                params = {"next": next_token}
            qs = urllib.parse.urlencode(params)
            url = f"{self.base_url}{endpoint}?{qs}" if qs else f"{self.base_url}{endpoint}"
            req = urllib.request.Request(url, headers=self._headers())
            raw = self._do_request(req)
            try:
                js = _json.loads(raw.decode())
            except Exception:
                logger.warning(f"[devices] JSON decode failed page={page} url={url}")
                break
            page_items: List[Dict[str, Any]] = []
            if isinstance(js, dict):
                for k in ("devices", "items", "data"):
                    if k in js and isinstance(js[k], list):
                        page_items = js[k]
                        break
                if not page_items:
                    for v in js.values():
                        if isinstance(v, list):
                            page_items = v
                            break
            elif isinstance(js, list):
                page_items = js
            if not page_items:
                logger.info(f"[devices] empty page page={page} breaking")
                break
            added = 0
            dups = 0
            for d in page_items:
                key = (
                    d.get("id") or d.get("deviceId") or d.get("macAddress") or d.get("mac") or d.get("serialNumber")
                )
                if not key:
                    name = d.get("deviceName") or d.get("name")
                    ip4 = d.get("ipv4")
                    if name or ip4:
                        key = f"{name or ''}|{ip4 or ''}"
                if key and key in seen_keys:
                    dups += 1
                    continue
                if key:
                    seen_keys.add(key)
                collected.append(d)
                added += 1
            logger.info(f"[devices] page={page} raw={len(page_items)} added={added} dups={dups} total_unique={len(collected)}")
            # Next token extraction
            new_next = js.get("next") if isinstance(js, dict) else None
            if not new_next:
                logger.info(f"[devices] pagination complete page={page} (no next)")
                break
            next_token = new_next
            if page >= self.max_pages_per_call:
                logger.warning(f"[devices] hit max_pages_per_call={self.max_pages_per_call} stopping early")
                break
            time.sleep(max(self.min_interval, min(self.page_delay_seconds, 1.0)))
        logger.info(f"[devices] collected_unique={len(collected)} pages={page}")
        return collected

    def list_all_clients(self, site_id_override: Optional[str]) -> List[Dict[str, Any]]:
        """Aggregate clients across all sites (or single site if override provided).

        Maintains prior behavior expected by ingestion_handler while leveraging
        cursor-first pagination inside list_clients_single_site.
        """
        if site_id_override:
            clients = self.list_clients_single_site(site_id_override)
            for c in clients:
                c["_site_id"] = site_id_override
            return clients

        import logging
        logger = logging.getLogger(__name__)
        sites = []
        for attempt in range(2):
            try:
                sites = self.list_sites()
                logger.info(f"[sites] Found {len(sites)} sites for aggregation")
                break
            except Exception as e:
                logger.error(f"[sites] Attempt {attempt+1} failed to enumerate sites: {e}")
                if attempt == 1:
                    return []
        all_clients: List[Dict[str, Any]] = []
        for idx, site in enumerate(sites, 1):
            sid = site.get("id")
            if not sid:
                continue
            if all_clients:
                time.sleep(self.page_delay_seconds)
            site_clients = self.list_clients_single_site(sid)
            site_name = site.get("name")
            for sc in site_clients:
                sc["_site_id"] = sid
                sc["_site_name"] = site_name
            logger.info(f"[sites] Site {idx}/{len(sites)} id={sid} name={site_name} clients_found={len(site_clients)}")
            all_clients.extend(site_clients)
        return all_clients

    # ---------- Unified cursor/offset collector ----------
    def _cursor_or_offset_collect(self,
        endpoint: str,
        root_keys: List[str],
        params: Dict[str, Any],
        annotate=None,
        per_page_site_delay: bool = False
    ) -> List[Dict[str, Any]]:
        """Generic collector that prefers cursor ('next') pagination.

        Steps:
          1. Perform first request with provided params + limit.
          2. If JSON has 'next', iterate using only {'next': token} on subsequent pages.
          3. Else fallback to offset loop (limit/offset) until empty or max_pages_per_call.
        annotate: optional callable run per item for tagging (site id injection etc.)
        per_page_site_delay: if True, adds a small delay between pages (used for clients variant pacing).
        """
    # No logging
        import urllib.request, urllib.parse
        collected: List[Dict[str, Any]] = []
        limit = min(self.page_limit, 100)
        base_params = dict(params)
        base_params.setdefault("limit", limit)

        # First request
        def _build_url(p: Dict[str, Any]):
            qs = urllib.parse.urlencode(p, doseq=True)
            return f"{self.base_url}{endpoint}?{qs}" if qs else f"{self.base_url}{endpoint}"

        def _extract_items(js: Any) -> List[Dict[str, Any]]:
            # Prioritize 'items' for client list extraction
            if isinstance(js, dict):
                if 'items' in js and isinstance(js['items'], list):
                    return js['items']
                # Fallback to other root keys
                for k in root_keys:
                    if k in js and isinstance(js[k], list):
                        return js[k]
                for v in js.values():
                    if isinstance(v, list):
                        return v
                if 'data' in js and isinstance(js['data'], dict):
                    return [js['data']]
            elif isinstance(js, list):
                return js
            return []

        page = 0
        next_token: Optional[str] = None
        # Issue first request
        url = _build_url(base_params)
        req = urllib.request.Request(url, headers=self._headers())
        raw = self._do_request(req)
        try:
            js = json.loads(raw.decode())
        except Exception:
            return []
        page += 1
        items = _extract_items(js)
        for it in items:
            if annotate:
                try:
                    annotate(it)
                except Exception:
                    pass
            collected.append(it)
        next_token = js.get("next") if isinstance(js, dict) else None

        # Use 'next' paging if available
        # Save original site identifier params for paging
        site_id_keys = [k for k in params.keys() if k in ("site-id", "siteId")]
        site_id_params = {k: params[k] for k in site_id_keys}
        while next_token and page < self.max_pages_per_call:
            # Only send 'next' (and site id if present), NOT 'limit' for cursor-based paging
            paging_params = {"next": next_token, **site_id_params}
            cursor_url = _build_url(paging_params)
            req = urllib.request.Request(cursor_url, headers=self._headers())
            raw = self._do_request(req)
            try:
                js = json.loads(raw.decode())
            except Exception:
                break
            page += 1
            items = _extract_items(js)
            if not items:
                break
            for it in items:
                if annotate:
                    try:
                        annotate(it)
                    except Exception:
                        pass
                collected.append(it)
            next_token = js.get("next") if isinstance(js, dict) else None
            if per_page_site_delay:
                time.sleep(max(self.min_interval, min(self.page_delay_seconds, 1.0)))
        return collected

    # ---------- Core HTTP ----------
    def _ensure_token(self):
        now = datetime.now(timezone.utc)
        if self._access_token and self._expires_at and now + timedelta(seconds=self.early_expiry_buffer) < self._expires_at:
            return
        self._authenticate()

    def _authenticate(self):
        if not self.client_secret:
            raise RuntimeError("Missing client_secret (ARUBA_CLIENT_SECRET)")
        # Token-as-bearer fallback (no client_id)
        if not self.client_id:
            self._access_token = self.client_secret
            self._expires_at = datetime.now(timezone.utc) + timedelta(hours=2)
            logger.info("[auth] Using token-as-bearer (no client_id)")
            return

        def _post_token(url: str, data: Dict[str, str], headers: Dict[str, str]) -> Optional[Dict[str, Any]]:
            req = urllib.request.Request(
                url,
                method="POST",
                data=urllib.parse.urlencode(data).encode(),
                headers=headers
            )
            try:
                raw = self._do_request(req)
                js = json.loads(raw.decode())
                return js
            except HTTPError as e:
                body = e.read().decode(errors="ignore")
                logger.warning(f"[auth] HTTP {e.code} token_url={url} body={body[:400]}")
                # Capture debugId if present
                try:
                    bjs = json.loads(body)
                    dbg = bjs.get("debugId")
                    if dbg:
                        logger.warning(f"[auth] debugId={dbg}")
                except Exception:
                    pass
                return None
            except Exception as e:
                logger.warning(f"[auth] token request failed url={url} err={e}")
                return None

        # Attempt 1: Form body with client_secret (legacy style)
        primary_headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Accept": "application/json"
        }
        data_primary = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret
        }
        js = _post_token(self.oauth_token_url, data_primary, primary_headers)

        # Attempt 2: If missing / 401, retry with HTTP Basic auth (omit client_secret in body)
        if not js or "access_token" not in js:
            basic_headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
                "Authorization": "Basic " + self._basic_auth_b64(self.client_id, self.client_secret)
            }
            data_basic = {
                "grant_type": "client_credentials",
                "client_id": self.client_id  # some endpoints still expect client_id here
            }
            logger.info("[auth] Retrying token request with Basic auth header")
            js = _post_token(self.oauth_token_url, data_basic, basic_headers)

        if not js or "access_token" not in js:
            raise RuntimeError(f"Auth failed after retries (url={self.oauth_token_url})")

        self._access_token = js["access_token"]
        ttl = js.get("expires_in", 7200)
        self._expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl)
        logger.info(f"[auth] Authenticated; expires_at={self._expires_at.isoformat()} ttl={ttl}s token_url={self.oauth_token_url}")

    def _basic_auth_b64(self, client_id: str, client_secret: str) -> str:
        import base64
        raw = f"{client_id}:{client_secret}".encode()
        return base64.b64encode(raw).decode()

    def _headers(self):
        h = {"Accept": "application/json", "Authorization": f"Bearer {self._access_token}"}
        if self.customer_id:
            h["X-Customer-Id"] = self.customer_id
        return h

    def _do_request(self, req: urllib.request.Request, attempt: int = 1):
        # Simple token bucket: enforce min interval
        with self._lock:
            now = time.time()
            wait = self.min_interval - (now - self._last_request_ts)
            if wait > 0:
                time.sleep(wait)
            self._last_request_ts = time.time()
        try:
            with urllib.request.urlopen(req, timeout=self.request_timeout) as resp:
                return resp.read()
        except urllib.error.HTTPError as e:
            body = e.read().decode(errors="ignore")
            status = e.code
            retriable = status in (429, 500, 502, 503, 504)
            # Special-case 401 invalid access token: force re-auth once then retry
            if status == 401 and attempt == 1:
                # Try to parse error code to ensure it's an auth token problem
                lowered = body.lower()
                if "invalid access token" in lowered or "unauthorized" in lowered:
                    try:
                        # Clear tokens and re-authenticate
                        self._access_token = None
                        self._expires_at = None
                        self._authenticate()
                        # Rebuild headers with new token
                        new_req = urllib.request.Request(req.full_url, headers=self._headers(), method=getattr(req, 'method', None))
                        result = self._do_request(new_req, attempt + 1)
                        if result is None:
                            raise RuntimeError(f"[auth] Token refresh succeeded but retry returned None for url={req.full_url}")
                        return result
                    except Exception as reauth_err:
                        raise RuntimeError(f"[auth] re-auth after 401 failed err={reauth_err}")
            if status == 429:
                # Honor Retry-After if present
                retry_after = e.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = float(retry_after)
                    except ValueError:
                        delay = 5.0
                else:
                    # exponential backoff with cap
                    delay = min( (2 ** (attempt - 1)) * self.min_interval * 2, self.max_backoff)
                logger.warning(f"[rate] 429 attempt={attempt} delay={delay}s url={req.full_url}")
                time.sleep(delay)
                # Increase base interval slightly to reduce future 429s
                self.min_interval = min(self.min_interval * 1.25, 5.0)
            else:
                logger.warning(f"[http] {status} attempt={attempt} retriable={retriable} url={req.full_url} body={body[:300]}")
            if retriable and attempt < self.max_retries:
                backoff = min( (2 ** (attempt - 1)) * self.min_interval, self.max_backoff)
                if status != 429:  # 429 already slept
                    time.sleep(backoff)
                return self._do_request(req, attempt + 1)
            raise
        except urllib.error.URLError as e:
            if attempt < self.max_retries:
                backoff = min( (2 ** (attempt - 1)) * self.min_interval, self.max_backoff)
                logger.warning(f"[net] URLError retry attempt={attempt} backoff={backoff}s err={e}")
                time.sleep(backoff)
                return self._do_request(req, attempt + 1)
            raise

    def _get_json(self, endpoint: str, params: Dict[str, Any] = None):
        self._ensure_token()
        url = f"{self.base_url}{endpoint}"
        if params:
            qs = urllib.parse.urlencode(params, doseq=True)
            url = f"{url}?{qs}"
        req = urllib.request.Request(url, headers=self._headers())
        raw = self._do_request(req)
        try:
            return json.loads(raw.decode())
        except json.JSONDecodeError:
            logger.error(f"[parse] Failed to parse JSON from {url}")
            return {}

    def _paged_collect(self, endpoint: str, base_params: Dict[str, Any] = None,
                       root_keys: List[str] = None) -> List[Dict[str, Any]]:
        base_params = dict(base_params or {})
        collected: List[Dict[str, Any]] = []
        offset = 0
        while True:
            params = {**base_params, "limit": self.page_limit, "offset": offset}
            js = self._get_json(endpoint, params)
            if not isinstance(js, dict):
                break
            items = None
            for k in root_keys or []:
                if k in js and isinstance(js[k], list):
                    items = js[k]
                    break
            if items is None:
                # fallback: try any list value
                for v in js.values():
                    if isinstance(v, list):
                        items = v
                        break
            if not items:
                break
            collected.extend(items)
            if (offset / self.page_limit) + 1 >= self.max_pages_per_call:
                logger.warning(f"[paged] reached max_pages_per_call={self.max_pages_per_call} stopping early")
                break
            offset += self.page_limit
            # Adaptive sleep: slower if min_interval already inflated
            page_sleep = max(self.min_interval, 0.5)
            time.sleep(page_sleep)
        return collected

__all__ = ["ArubaApiClient"]