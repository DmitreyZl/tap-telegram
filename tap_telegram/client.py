"""Custom client handling, including TelegramStream base class."""

from __future__ import annotations
import datetime as dt
from singer_sdk.helpers.jsonpath import extract_jsonpath


from pyrogram import Client

import typing as t

from singer_sdk.streams import Stream

if t.TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class TelegramStream(Stream):
    """Stream class for Telegram streams."""
    """Stream class for Senler streams."""
    records_jsonpath = "$[*]"

    def get_records(
        self,
        context: Context | None,
    ) -> t.Iterable[dict]:

        API_ID = self.config.get('api_id')  # Ваш API ID
        API_HASH = self.config.get('api_hash')  # Ваш API Hash
        SESSION = self.config.get('session_key')  # Сессия ПОЛЬЗОВАТЕЛЯ, АДМИНА КАНАЛА
        CHANNEL = self.config.get('channel')

        with Client(name="my_account", api_id=API_ID, api_hash=API_HASH, session_string=SESSION) as app:
            # ── основная «паспортная» информация ────────────────────────────
            chat = app.get_chat(CHANNEL)

            row = {
                "date": dt.date.today().isoformat(),
                "id": chat.id,
                "title": chat.title,
                "description": chat.description or "",
                "members_total": chat.members_count,
                "channel": chat.username,
                "invite_link": chat.invite_link
            }
            yield from extract_jsonpath(self.records_jsonpath, input=[row])
