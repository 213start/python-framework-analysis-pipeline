"""GitCode Issues API client using urllib.request."""
from __future__ import annotations

import json
import logging
import ssl
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

_GITCODE_DEFAULT_BASE = "https://api.gitcode.com/api/v5"
_REQUEST_TIMEOUT = 30  # seconds

_SSL_CONTEXT = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
_SSL_CONTEXT.check_hostname = False
_SSL_CONTEXT.verify_mode = ssl.CERT_NONE
_SSL_CONTEXT.set_ciphers("DEFAULT:@SECLEVEL=0")


class GitCodeClient:
    """GitCode API v5 client (Gitee-compatible).

    Parameters
    ----------
    token:
        GitCode personal-access token (``PRIVATE-TOKEN`` header).
    base_url:
        Override default ``https://api.gitcode.com/api/v5``.
    """

    def __init__(self, token: str, base_url: str | None = None) -> None:
        self._token = token
        self._base = (base_url or _GITCODE_DEFAULT_BASE).rstrip("/")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create_issue(
        self,
        owner: str,
        repo: str,
        title: str,
        body: str,
        labels: list[str] | None = None,
    ) -> dict[str, Any]:
        """Create an issue and return ``{"number": ..., "html_url": ...}``."""
        logger.info("Creating issue in %s/%s: %s", owner, repo, title[:80])
        url = f"{self._base}/repos/{owner}/{repo}/issues"
        payload: dict[str, Any] = {"title": title, "body": body}
        if labels:
            payload["labels"] = ",".join(labels)
        data = self._request("POST", url, payload)
        result = {
            "number": data.get("number"),
            "html_url": data.get("html_url", ""),
        }
        logger.info("Created issue #%s in %s/%s", result["number"], owner, repo)
        return result

    def get_issue_comments(
        self,
        owner: str,
        repo: str,
        issue_number: int,
    ) -> list[dict[str, Any]]:
        """Fetch all comments for *issue_number*."""
        logger.info("Fetching comments for issue #%s in %s/%s", issue_number, owner, repo)
        url = f"{self._base}/repos/{owner}/{repo}/issues/{issue_number}/comments"
        data = self._request("GET", url)
        items = data if isinstance(data, list) else [data]
        comments = [
            {"body": c.get("body", ""), "id": c.get("id")} for c in items
        ]
        logger.info("Fetched %d comments for issue #%s", len(comments), issue_number)
        return comments

    def update_issue(
        self,
        owner: str,
        repo: str,
        issue_number: int,
        body: str,
    ) -> None:
        """Update the body of an existing issue."""
        logger.info("Updating issue #%s body in %s/%s", issue_number, owner, repo)
        url = f"{self._base}/repos/{owner}/{repo}/issues/{issue_number}"
        self._request("PATCH", url, {"body": body})
        logger.info("Updated issue #%s", issue_number)

    def list_issues_by_label(
        self,
        owner: str,
        repo: str,
        label: str,
    ) -> dict[str, dict[str, Any]]:
        """List issues with *label*, keyed by title.

        Returns ``{title: {"number": int, "html_url": str}}``.
        """
        logger.info("Listing issues with label %r in %s/%s", label, owner, repo)
        result: dict[str, dict[str, Any]] = {}
        page = 1
        while True:
            url = (
                f"{self._base}/repos/{owner}/{repo}/issues"
                f"?labels={label}&state=all&per_page=100&page={page}"
            )
            data = self._request("GET", url)
            items = data if isinstance(data, list) else [data]
            if not items:
                break
            for item in items:
                title = item.get("title", "")
                if title:
                    result[title] = {
                        "number": item.get("number"),
                        "html_url": item.get("html_url", ""),
                    }
            if len(items) < 100:
                break
            page += 1
        logger.info("Found %d issues with label %r", len(result), label)
        return result

    def ensure_label(
        self,
        owner: str,
        repo: str,
        name: str,
        color: str,
        description: str = "",
    ) -> None:
        """Create a label; ignore 422 / already-exists errors."""
        url = f"{self._base}/repos/{owner}/{repo}/labels"
        payload = {"name": name, "color": color, "description": description}
        try:
            self._request("POST", url, payload)
            logger.info("Label %r created in %s/%s", name, owner, repo)
        except urllib.error.HTTPError as exc:
            if exc.code in (422, 409):
                logger.debug("Label %r already exists in %s/%s", name, owner, repo)
            else:
                raise

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_request(
        self,
        method: str,
        url: str,
        payload: dict[str, Any] | None = None,
    ) -> urllib.request.Request:
        body: bytes | None = None
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=body, method=method)
        req.add_header("PRIVATE-TOKEN", self._token)
        req.add_header("Content-Type", "application/json; charset=utf-8")
        req.add_header("User-Agent", "pyframework-pipeline")
        return req

    def _request(
        self,
        method: str,
        url: str,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        req = self._build_request(method, url, payload)
        try:
            with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT, context=_SSL_CONTEXT) as resp:
                self._check_rate_limit(resp)
                raw = resp.read()
                return json.loads(raw.decode("utf-8")) if raw else {}
        except urllib.error.HTTPError as exc:
            logger.error(
                "GitCode API %s %s failed: %s %s",
                method,
                url,
                exc.code,
                exc.reason,
            )
            raise
        except urllib.error.URLError as exc:
            logger.error("GitCode API network error: %s", exc.reason)
            raise

    @staticmethod
    def _check_rate_limit(resp: http.client.HTTPResponse) -> None:  # type: ignore[name-defined]  # noqa: F821
        """Log a warning when remaining requests are low.

        GitCode rate limits: 50/min, 4000/hr.  Headers may include
        ``X-RateLimit-Remaining`` and ``X-RateLimit-Limit``.
        """
        remaining = resp.headers.get("X-RateLimit-Remaining")
        if remaining is not None:
            rem = int(remaining)
            if rem < 10:
                logger.warning(
                    "GitCode rate limit very low: %s requests remaining", rem
                )
            elif rem < 50:
                logger.info("GitCode rate limit: %s requests remaining", rem)
