#!/usr/bin/env python3
"""
threads_post.py

Daily publisher for reserved Threads posts stored in a Notion data source.

Environment variables required:
  NOTION_TOKEN
  NOTION_DATA_SOURCE_ID
  THREADS_USER_ID
  THREADS_ACCESS_TOKEN

Optional:
  NOTION_VERSION (default: 2025-09-03)
  NOTION_PROPERTY_TITLE (default: 投稿名)
  NOTION_PROPERTY_SCHEDULED_AT (default: 投稿予定日時)
  NOTION_PROPERTY_PLATFORM (default: プラットフォーム)
  NOTION_PROPERTY_CONTENT (default: 投稿文)
  NOTION_PROPERTY_STATUS (default: ステータス)
  NOTION_PROPERTY_POSTED_AT (default: 実投稿日時)
  NOTION_PROPERTY_THREADS_POST_ID (default: Threads投稿ID)
  NOTION_PROPERTY_ERROR (default: エラー内容)
  THREADS_BASE_URL (default: https://graph.threads.net/v1.0)
  NOTION_API_BASE_URL (default: https://api.notion.com/v1)

Notes:
- Notion API version 2025-09-03 is used.
- Threads text posts are limited to 500 characters.
- No automatic retries are used to avoid duplicate publishing.
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional

import requests


JST = ZoneInfo("Asia/Tokyo")


def jst_now() -> datetime:
    return datetime.now(JST)


def iso_now_jst() -> str:
    return jst_now().isoformat()


def iso_date_now_jst() -> str:
    # Notion date property can accept a date-time string; we keep JST for consistency.
    return iso_now_jst()


def truncate_text(text: str, max_chars: int = 500) -> str:
    text = text.strip()
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1] + "…"


class NotionAPIError(RuntimeError):
    pass


class ThreadsAPIError(RuntimeError):
    pass


@dataclass
class DataSourceSchema:
    id: str
    properties: Dict[str, Dict[str, Any]]


class Config:
    notion_token: str
    notion_data_source_id: str
    threads_user_id: str
    threads_access_token: str
    notion_version: str
    notion_api_base_url: str
    threads_base_url: str

    title_prop: str
    scheduled_at_prop: str
    platform_prop: str
    content_prop: str
    status_prop: str
    posted_at_prop: str
    threads_post_id_prop: str
    error_prop: str

    def __init__(self) -> None:
        self.notion_token = os.environ["NOTION_TOKEN"].strip()
        self.notion_data_source_id = os.environ["NOTION_DATA_SOURCE_ID"].strip()
        self.threads_user_id = os.environ["THREADS_USER_ID"].strip()
        self.threads_access_token = os.environ["THREADS_ACCESS_TOKEN"].strip()

        self.notion_version = os.getenv("NOTION_VERSION", "2025-09-03").strip()
        self.notion_api_base_url = os.getenv("NOTION_API_BASE_URL", "https://api.notion.com/v1").rstrip("/")
        self.threads_base_url = os.getenv("THREADS_BASE_URL", "https://graph.threads.net/v1.0").rstrip("/")

        self.title_prop = os.getenv("NOTION_PROPERTY_TITLE", "投稿名")
        self.scheduled_at_prop = os.getenv("NOTION_PROPERTY_SCHEDULED_AT", "投稿予定日時")
        self.platform_prop = os.getenv("NOTION_PROPERTY_PLATFORM", "プラットフォーム")
        self.content_prop = os.getenv("NOTION_PROPERTY_CONTENT", "投稿文")
        self.status_prop = os.getenv("NOTION_PROPERTY_STATUS", "ステータス")
        self.posted_at_prop = os.getenv("NOTION_PROPERTY_POSTED_AT", "実投稿日時")
        self.threads_post_id_prop = os.getenv("NOTION_PROPERTY_THREADS_POST_ID", "Threads投稿ID")
        self.error_prop = os.getenv("NOTION_PROPERTY_ERROR", "エラー内容")


def notion_headers(config: Config) -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {config.notion_token}",
        "Notion-Version": config.notion_version,
        "Content-Type": "application/json",
    }


def notion_request(
    config: Config,
    method: str,
    path: str,
    *,
    params: Optional[dict] = None,
    json_body: Optional[dict] = None,
) -> dict:
    url = f"{config.notion_api_base_url}{path}"
    response = requests.request(
        method=method,
        url=url,
        headers=notion_headers(config),
        params=params,
        json=json_body,
        timeout=30,
    )
    if not response.ok:
        raise NotionAPIError(f"Notion {method} {path} failed: {response.status_code} {response.text}")
    if response.text.strip():
        return response.json()
    return {}


def threads_request(
    config: Config,
    method: str,
    path: str,
    *,
    params: Optional[dict] = None,
    data: Optional[dict] = None,
) -> dict:
    url = f"{config.threads_base_url}{path}"
    response = requests.request(method=method, url=url, params=params, data=data, timeout=30)
    if not response.ok:
        raise ThreadsAPIError(f"Threads {method} {path} failed: {response.status_code} {response.text}")
    if response.text.strip():
        return response.json()
    return {}


def load_data_source_schema(config: Config) -> DataSourceSchema:
    payload = notion_request(config, "GET", f"/data_sources/{config.notion_data_source_id}")
    properties = payload.get("properties")
    if not isinstance(properties, dict):
        raise NotionAPIError("Data source schema did not include a properties object.")
    return DataSourceSchema(id=payload["id"], properties=properties)


def prop_type(schema: DataSourceSchema, prop_name: str) -> str:
    try:
        prop = schema.properties[prop_name]
    except KeyError as exc:
        raise NotionAPIError(f"Property not found in data source schema: {prop_name}") from exc
    prop_type_value = prop.get("type")
    if not isinstance(prop_type_value, str):
        raise NotionAPIError(f"Property type missing for: {prop_name}")
    return prop_type_value


def build_text_filter(property_name: str, prop_type_name: str, value: str) -> Dict[str, Any]:
    if prop_type_name not in {"select", "status"}:
        raise NotionAPIError(
            f"Property '{property_name}' must be select or status for equality filter, got '{prop_type_name}'."
        )
    return {
        "property": property_name,
        prop_type_name: {
            "equals": value,
        },
    }


def build_date_filter(property_name: str, value: str) -> Dict[str, Any]:
    return {
        "property": property_name,
        "date": {
            "on_or_before": value,
        },
    }


def extract_title_from_page(page: Dict[str, Any], title_prop_name: str) -> str:
    prop = page.get("properties", {}).get(title_prop_name, {})
    if not isinstance(prop, dict):
        return ""
    title_items = prop.get("title", [])
    if not isinstance(title_items, list):
        return ""
    parts: List[str] = []
    for item in title_items:
        if isinstance(item, dict):
            plain_text = item.get("plain_text")
            if isinstance(plain_text, str):
                parts.append(plain_text)
    return "".join(parts).strip()


def extract_rich_text(page: Dict[str, Any], prop_name: str) -> str:
    prop = page.get("properties", {}).get(prop_name, {})
    if not isinstance(prop, dict):
        return ""
    rich_items = prop.get("rich_text", [])
    if not isinstance(rich_items, list):
        return ""
    parts: List[str] = []
    for item in rich_items:
        if isinstance(item, dict):
            plain_text = item.get("plain_text")
            if isinstance(plain_text, str):
                parts.append(plain_text)
    return "".join(parts).strip()


def query_due_pages(config: Config, schema: DataSourceSchema) -> List[Dict[str, Any]]:
    status_type = prop_type(schema, config.status_prop)
    platform_type = prop_type(schema, config.platform_prop)

    now_value = iso_now_jst()
    filter_obj = {
        "and": [
            build_text_filter(config.status_prop, status_type, "予約"),
            build_date_filter(config.scheduled_at_prop, now_value),
            build_text_filter(config.platform_prop, platform_type, "Threads"),
        ]
    }

    results: List[Dict[str, Any]] = []
    start_cursor: Optional[str] = None

    while True:
        body: Dict[str, Any] = {
            "filter": filter_obj,
            "page_size": 100,
            "sorts": [
                {"property": config.scheduled_at_prop, "direction": "ascending"},
                {"property": config.title_prop, "direction": "ascending"},
            ],
        }
        if start_cursor:
            body["start_cursor"] = start_cursor

        payload = notion_request(config, "POST", f"/data_sources/{config.notion_data_source_id}/query", json_body=body)
        batch = payload.get("results", [])
        if isinstance(batch, list):
            results.extend(batch)

        next_cursor = payload.get("next_cursor")
        has_more = bool(payload.get("has_more"))
        if not (has_more and isinstance(next_cursor, str) and next_cursor):
            break
        start_cursor = next_cursor

    return results


def notion_update_page(config: Config, page_id: str, properties: Dict[str, Any]) -> None:
    notion_request(config, "PATCH", f"/pages/{page_id}", json_body={"properties": properties})


def notion_update_page_with_retry(
    config: Config,
    page_id: str,
    properties: Dict[str, Any],
    *,
    attempts: int = 3,
    base_sleep_seconds: float = 1.5,
) -> None:
    last_exc: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            notion_update_page(config, page_id, properties)
            return
        except Exception as exc:
            last_exc = exc
            if attempt < attempts:
                time.sleep(base_sleep_seconds * attempt)
    assert last_exc is not None
    raise last_exc


def update_page_success(config: Config, page_id: str, threads_post_id: str, status_key: str) -> None:
    now_value = iso_date_now_jst()
    notion_update_page_with_retry(
        config,
        page_id,
        {
            config.status_prop: {
                status_key: {"name": "投稿済"},
            },
            config.posted_at_prop: {
                "date": {"start": now_value},
            },
            config.threads_post_id_prop: {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": threads_post_id},
                    }
                ]
            },
            config.error_prop: {
                "rich_text": [],
            },
        },
    )


def update_page_error(config: Config, page_id: str, error_message: str, status_key: str) -> None:
    truncated = truncate_text(error_message, 1500)
    notion_update_page_with_retry(
        config,
        page_id,
        {
            config.status_prop: {
                status_key: {"name": "エラー"},
            },
            config.error_prop: {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": truncated},
                    }
                ]
            },
        },
    )


def update_page_skip(config: Config, page_id: str, message: str, status_key: str) -> None:
    notion_update_page_with_retry(
        config,
        page_id,
        {
            config.status_prop: {
                status_key: {"name": "スキップ"},
            },
            config.error_prop: {
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": truncate_text(message, 1500)},
                    }
                ]
            },
        },
    )


def create_threads_container(config: Config, text: str) -> str:
    payload = threads_request(
        config,
        "POST",
        f"/{config.threads_user_id}/threads",
        data={
            "media_type": "TEXT",
            "text": text,
            "access_token": config.threads_access_token,
        },
    )
    container_id = payload.get("id") or payload.get("creation_id")
    if not isinstance(container_id, str) or not container_id:
        raise ThreadsAPIError(
            f"Threads container creation response did not include an id: {json.dumps(payload, ensure_ascii=False)}"
        )
    return container_id


def publish_threads_container(config: Config, creation_id: str) -> str:
    payload = threads_request(
        config,
        "POST",
        f"/{config.threads_user_id}/threads_publish",
        data={
            "creation_id": creation_id,
            "access_token": config.threads_access_token,
        },
    )
    post_id = payload.get("id") or payload.get("post_id") or payload.get("creation_id")
    if not isinstance(post_id, str) or not post_id:
        post_id = creation_id
    return post_id


def validate_thread_text(text: str) -> str:
    cleaned = text.strip()
    if not cleaned:
        raise ValueError("投稿文が空です。")
    if len(cleaned) > 500:
        raise ValueError(f"Threadsの文字数上限500字を超えています: {len(cleaned)}")
    return cleaned


def process_page(config: Config, page: Dict[str, Any], *, status_key: str, dry_run: bool = False) -> None:
    page_id = page["id"]
    title = extract_title_from_page(page, config.title_prop)
    text = extract_rich_text(page, config.content_prop)

    if not text:
        raise ValueError("投稿文が空です。")

    validated_text = validate_thread_text(text)

    if dry_run:
        print(
            json.dumps(
                {
                    "page_id": page_id,
                    "title": title,
                    "text": validated_text,
                    "action": "dry_run_no_post",
                },
                ensure_ascii=False,
            )
        )
        return

    creation_id = create_threads_container(config, validated_text)
    post_id = publish_threads_container(config, creation_id)

    try:
        update_page_success(config, page_id, post_id, status_key)
    except Exception as exc:
        # Posting succeeded. Do not mark the row as error because that would be misleading.
        # Log the failure so the job output shows that the Notion reconciliation step failed.
        print(
            json.dumps(
                {
                    "page_id": page_id,
                    "title": title,
                    "threads_post_id": post_id,
                    "notion_update_warning": f"{type(exc).__name__}: {exc}",
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )

    print(
        json.dumps(
            {
                "page_id": page_id,
                "title": title,
                "creation_id": creation_id,
                "threads_post_id": post_id,
                "status": "posted",
            },
            ensure_ascii=False,
        )
    )


def main() -> int:
    dry_run = "--dry-run" in sys.argv[1:]

    try:
        config = Config()
        schema = load_data_source_schema(config)
        due_pages = query_due_pages(config, schema)

        print(
            json.dumps(
                {
                    "timestamp_jst": iso_now_jst(),
                    "matched_rows": len(due_pages),
                    "dry_run": dry_run,
                },
                ensure_ascii=False,
            )
        )

        if not due_pages:
            return 0

        status_type = prop_type(schema, config.status_prop)
        status_key = "status" if status_type == "status" else "select"

        for page in due_pages:
            page_id = page["id"]
            title = extract_title_from_page(page, config.title_prop)
            try:
                process_page(config, page, status_key=status_key, dry_run=dry_run)
            except Exception as exc:
                error_message = f"{type(exc).__name__}: {exc}"
                try:
                    if not dry_run:
                        update_page_error(config, page_id, error_message, status_key=status_key)
                except Exception as update_exc:
                    # Keep the script alive even if error logging fails.
                    print(
                        json.dumps(
                            {
                                "page_id": page_id,
                                "title": title,
                                "error": error_message,
                                "notion_error_update_failed": f"{type(update_exc).__name__}: {update_exc}",
                            },
                            ensure_ascii=False,
                        ),
                        file=sys.stderr,
                    )
                else:
                    print(
                        json.dumps(
                            {
                                "page_id": page_id,
                                "title": title,
                                "error": error_message,
                                "status": "notion_error_updated" if not dry_run else "dry_run_error",
                            },
                            ensure_ascii=False,
                        ),
                        file=sys.stderr,
                    )

        return 0

    except Exception as exc:
        print(f"[fatal] {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
