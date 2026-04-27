"""GitHub Discussions API client using GraphQL.

GitHub Discussions use the GraphQL API (not REST).  This client creates
discussions and fetches threaded comments (comment → replies), which is
the structure needed for multi-round LLM analysis + review.
"""
from __future__ import annotations

import json
import logging
import ssl
import urllib.error
import urllib.request
from typing import Any

logger = logging.getLogger(__name__)

_SSL_CONTEXT = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
_SSL_CONTEXT.check_hostname = False
_SSL_CONTEXT.verify_mode = ssl.CERT_NONE
_SSL_CONTEXT.set_ciphers("DEFAULT:@SECLEVEL=0")
_SSL_CONTEXT.maximum_version = ssl.TLSVersion.MAXIMUM_SUPPORTED

_GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"
_REQUEST_TIMEOUT = 30  # seconds


class DiscussionClient:
    """GraphQL client for GitHub Discussions.

    Parameters
    ----------
    token:
        GitHub personal-access token with ``repo`` scope.
    base_url:
        Override default ``https://api.github.com/graphql``.
    """

    def __init__(self, token: str, base_url: str | None = None) -> None:
        self._token = token
        self._graphql_url = base_url or _GITHUB_GRAPHQL_URL

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def create_discussion(
        self,
        repo_id: str,
        category_id: str,
        title: str,
        body: str,
    ) -> dict[str, Any]:
        """Create a Discussion.  Returns ``{"number": ..., "url": ...}``."""
        logger.info("Creating discussion: %s", title[:80])
        query = """
        mutation($repoId: ID!, $categoryId: ID!, $title: String!, $body: String!) {
          createDiscussion(input: {
            repositoryId: $repoId,
            categoryId: $categoryId,
            title: $title,
            body: $body
          }) {
            discussion {
              number
              url
            }
          }
        }
        """
        variables = {
            "repoId": repo_id,
            "categoryId": category_id,
            "title": title,
            "body": body,
        }
        data = self._graphql(query, variables)
        discussion = (
            data.get("createDiscussion", {}).get("discussion", {})
        )
        result = {
            "number": discussion.get("number"),
            "url": discussion.get("url", ""),
        }
        logger.info("Created discussion #%s", result["number"])
        return result

    def get_discussion_comments(
        self,
        owner: str,
        repo: str,
        discussion_number: int,
    ) -> list[dict[str, Any]]:
        logger.info("Fetching comments for discussion #%s", discussion_number)
        """Fetch threaded comments for a Discussion.

        Returns a list of top-level comments, each with a ``replies`` list::

            [
                {
                    "id": "...",
                    "body": "## 跨平台机器码差异分析：...",
                    "replies": [
                        {"id": "...", "body": "Approved ..."}
                    ]
                },
                ...
            ]
        """
        query = """
        query($owner: String!, $repo: String!, $number: Int!) {
          repository(owner: $owner, name: $repo) {
            discussion(number: $number) {
              comments(first: 100) {
                nodes {
                  id
                  body
                  replies(first: 20) {
                    nodes {
                      id
                      body
                    }
                  }
                }
              }
            }
          }
        }
        """
        variables = {
            "owner": owner,
            "repo": repo,
            "number": discussion_number,
        }
        data = self._graphql(query, variables)
        comment_nodes = (
            data.get("repository", {})
            .get("discussion", {})
            .get("comments", {})
            .get("nodes", [])
        )
        result: list[dict[str, Any]] = []
        for node in comment_nodes:
            replies = [
                {"id": r.get("id", ""), "body": r.get("body", "")}
                for r in node.get("replies", {}).get("nodes", [])
            ]
            result.append({
                "id": node.get("id", ""),
                "body": node.get("body", ""),
                "replies": replies,
            })
        logger.info("Fetched %d comments for discussion #%s", len(result), discussion_number)
        return result

    def get_repo_id(self, owner: str, repo: str) -> str:
        logger.info("Resolving repo ID for %s/%s", owner, repo)
        """Get the GraphQL node ID for a repository."""
        query = """
        query($owner: String!, $repo: String!) {
          repository(owner: $owner, name: $repo) {
            id
          }
        }
        """
        variables = {"owner": owner, "repo": repo}
        data = self._graphql(query, variables)
        repo_id = data.get("repository", {}).get("id", "")
        if not repo_id:
            raise ValueError(f"Could not resolve repo ID for {owner}/{repo}")
        logger.info("Resolved repo ID: %s", repo_id[:12] + "...")
        return repo_id

    def get_discussion_category_id(
        self,
        repo_id: str,
        category_name: str = "General",
    ) -> str:
        logger.info("Looking up discussion category: %s", category_name)
        """Get the GraphQL ID for a Discussion category by name."""
        query = """
        query($repoId: ID!) {
          node(id: $repoId) {
            ... on Repository {
              discussionCategories(first: 20) {
                nodes {
                  id
                  name
                }
              }
            }
          }
        }
        """
        variables = {"repoId": repo_id}
        data = self._graphql(query, variables)
        categories = (
            data.get("node", {})
            .get("discussionCategories", {})
            .get("nodes", [])
        )
        for cat in categories:
            if cat.get("name", "").lower() == category_name.lower():
                logger.info("Found category %r (id: %s)", cat["name"], cat["id"][:12] + "...")
                return cat["id"]
        # Fallback: return first category if name doesn't match.
        if categories:
            logger.warning(
                "Category %r not found, using %r",
                category_name,
                categories[0].get("name", ""),
            )
            return categories[0]["id"]
        raise ValueError(
            f"No discussion categories found (looking for {category_name!r})"
        )

    def update_discussion_body(
        self,
        owner: str,
        repo: str,
        discussion_number: int,
        body: str,
    ) -> None:
        """Update the body of an existing Discussion."""
        logger.info("Updating discussion #%s body", discussion_number)
        node_id = self._get_discussion_node_id(owner, repo, discussion_number)
        query = """
        mutation($discussionId: ID!, $body: String!) {
          updateDiscussion(input: {
            discussionId: $discussionId,
            body: $body
          }) {
            discussion {
              number
            }
          }
        }
        """
        variables = {"discussionId": node_id, "body": body}
        self._graphql(query, variables)
        logger.info("Updated discussion #%s", discussion_number)

    def list_discussions(
        self,
        owner: str,
        repo: str,
    ) -> dict[str, dict[str, Any]]:
        """List all discussions, keyed by title.

        Returns ``{title: {"number": int, "url": str}}``.
        """
        logger.info("Listing discussions in %s/%s", owner, repo)
        query = """
        query($owner: String!, $repo: String!, $first: Int!, $after: String) {
          repository(owner: $owner, name: $repo) {
            discussions(first: $first, after: $after) {
              pageInfo {
                hasNextPage
                endCursor
              }
              nodes {
                number
                title
                url
              }
            }
          }
        }
        """
        result: dict[str, dict[str, Any]] = {}
        cursor: str | None = None
        while True:
            variables = {"owner": owner, "repo": repo, "first": 100, "after": cursor}
            data = self._graphql(query, variables)
            page = (
                data.get("repository", {})
                .get("discussions", {})
            )
            for node in page.get("nodes", []):
                result[node["title"]] = {
                    "number": node.get("number"),
                    "url": node.get("url", ""),
                }
            page_info = page.get("pageInfo", {})
            if page_info.get("hasNextPage"):
                cursor = page_info["endCursor"]
            else:
                break
        logger.info("Found %d discussions in %s/%s", len(result), owner, repo)
        return result

    def _get_discussion_node_id(
        self,
        owner: str,
        repo: str,
        discussion_number: int,
    ) -> str:
        """Resolve a discussion number to its GraphQL node ID."""
        query = """
        query($owner: String!, $repo: String!, $number: Int!) {
          repository(owner: $owner, name: $repo) {
            discussion(number: $number) {
              id
            }
          }
        }
        """
        variables = {
            "owner": owner,
            "repo": repo,
            "number": discussion_number,
        }
        data = self._graphql(query, variables)
        node_id = (
            data.get("repository", {})
            .get("discussion", {})
            .get("id", "")
        )
        if not node_id:
            raise ValueError(
                f"Could not resolve node ID for discussion #{discussion_number}"
            )
        return node_id

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _graphql(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a GraphQL query and return the ``data`` field."""
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables
        body = json.dumps(payload).encode("utf-8")

        req = urllib.request.Request(
            self._graphql_url, data=body, method="POST",
        )
        req.add_header("Authorization", f"Bearer {self._token}")
        req.add_header("Content-Type", "application/json; charset=utf-8")
        req.add_header("User-Agent", "pyframework-pipeline")

        try:
            with urllib.request.urlopen(req, timeout=_REQUEST_TIMEOUT, context=_SSL_CONTEXT) as resp:
                raw = resp.read()
                result = json.loads(raw.decode("utf-8")) if raw else {}
        except urllib.error.HTTPError as exc:
            logger.error(
                "GitHub GraphQL error: %s %s", exc.code, exc.reason,
            )
            raise
        except urllib.error.URLError as exc:
            logger.error("GitHub GraphQL network error: %s", exc.reason)
            raise

        errors = result.get("errors")
        if errors:
            msgs = "; ".join(
                e.get("message", "unknown") for e in errors
            )
            raise RuntimeError(f"GraphQL errors: {msgs}")

        return result.get("data", {})
