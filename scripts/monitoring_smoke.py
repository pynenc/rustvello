"""Smoke-test a live rustvello-monitoring dashboard.

The check is intentionally HTTP-only so agents can run it without a browser:

    python scripts/monitoring_smoke.py http://127.0.0.1:50849
"""

from __future__ import annotations

import argparse
import sys

from html.parser import HTMLParser
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


class AssetParser(HTMLParser):
    """Collect dashboard assets from HTML."""

    def __init__(self) -> None:
        """Initialize empty asset collections."""
        super().__init__()
        self.stylesheets: list[str] = []
        self.scripts: list[str] = []
        self.images: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        """Record linked CSS, JavaScript, and image assets."""
        values = dict(attrs)
        if tag == "link" and values.get("rel") == "stylesheet" and values.get("href"):
            self.stylesheets.append(values["href"] or "")
        elif tag == "script" and values.get("src"):
            self.scripts.append(values["src"] or "")
        elif tag == "img" and values.get("src"):
            self.images.append(values["src"] or "")


def fetch(url: str) -> tuple[int, str, bytes]:
    """Fetch a URL and return status, content type, and response body."""
    request = Request(url, headers={"User-Agent": "rustvello-monitoring-smoke/1.0"})
    try:
        with urlopen(request, timeout=5) as response:
            return (
                response.status,
                response.headers.get("content-type", ""),
                response.read(),
            )
    except HTTPError as exc:
        return exc.code, exc.headers.get("content-type", ""), exc.read()
    except URLError as exc:
        message = f"{url}: {exc.reason}"
        raise RuntimeError(message) from exc


def same_origin(page_url: str, asset_url: str) -> bool:
    """Return whether an asset URL belongs to the dashboard origin."""
    page = urlparse(page_url)
    asset = urlparse(asset_url)
    return (page.scheme, page.netloc) == (asset.scheme, asset.netloc)


def check_asset(page_url: str, asset_path: str, expected_type: str) -> list[str]:
    """Validate an expected dashboard asset."""
    asset_url = urljoin(page_url, asset_path)
    status, content_type, body = fetch(asset_url)
    problems: list[str] = []
    if status != 200:
        problems.append(f"{asset_path} returned HTTP {status}")
        return problems
    if expected_type not in content_type:
        problems.append(
            f"{asset_path} returned content-type {content_type!r}, "
            f"expected {expected_type!r}"
        )
    if asset_path.endswith("rustvello.css") and b"--nav-bg" not in body:
        problems.append(f"{asset_path} does not contain rustvello design variables")
    if asset_path.endswith(".js") and b"timelineFromCurrent" not in body:
        problems.append(f"{asset_path} does not contain shared monitoring JavaScript")
    if asset_path.endswith(".png") and not body.startswith(b"\x89PNG\r\n\x1a\n"):
        problems.append(f"{asset_path} is not a PNG payload")
    return problems


def main() -> int:
    """Run the live monitoring dashboard smoke check."""
    parser = argparse.ArgumentParser()
    parser.add_argument("url", help="Base monitoring URL, e.g. http://127.0.0.1:50849")
    parser.add_argument(
        "--strict-local",
        action="store_true",
        help="Fail if the dashboard still depends on external CDN assets.",
    )
    args = parser.parse_args()

    page_url = args.url.rstrip("/") + "/"
    status, content_type, body = fetch(page_url)
    problems: list[str] = []
    if status != 200:
        problems.append(f"dashboard returned HTTP {status}")
    if "text/html" not in content_type:
        problems.append(f"dashboard returned content-type {content_type!r}")

    parser_html = AssetParser()
    parser_html.feed(body.decode("utf-8", errors="replace"))

    required_assets = {
        "/static/css/rustvello.css": "text/css",
        "/static/css/timeline.css": "text/css",
        "/static/css/arguments.css": "text/css",
        "/static/css/invocations.css": "text/css",
        "/static/css/histogram.css": "text/css",
        "/static/js/monitoring.js": "text/javascript",
        "/static/logo.png": "image/png",
    }
    discovered = set(parser_html.stylesheets + parser_html.scripts + parser_html.images)
    for asset_path, expected_type in required_assets.items():
        if asset_path not in discovered:
            problems.append(f"dashboard does not reference {asset_path}")
        problems.extend(check_asset(page_url, asset_path, expected_type))

    external_assets = sorted(
        asset
        for asset in discovered
        if asset.startswith(("http://", "https://"))
        and not same_origin(page_url, asset)
    )
    if args.strict_local and external_assets:
        problems.append("external assets referenced: " + ", ".join(external_assets))

    if problems:
        print("rustvello monitoring smoke check failed:", file=sys.stderr)
        for problem in problems:
            print(f"- {problem}", file=sys.stderr)
        return 1

    print("rustvello monitoring smoke check passed")
    if external_assets:
        print("external assets still referenced:")
        for asset in external_assets:
            print(f"- {asset}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
