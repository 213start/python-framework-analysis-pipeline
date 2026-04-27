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

_SSL_CONTEXT = ssl._create_unverified_context()

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
        return {
            "number": discussion.get("number"),
            "url": discussion.get("url", ""),
        }

    def get_discussion_comments(
        self,
        owner: str,
        repo: str,
        discussion_number: int,
    ) -> list[dict[str, Any]]:
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
        return result

    def get_repo_id(self, owner: str, repo: str) -> str:
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
        return repo_id

    def get_discussion_category_id(
        self,
        repo_id: str,
        category_name: str = "General",
    ) -> str:
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
