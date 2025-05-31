"""Stream type classes for tap-telegram."""

from __future__ import annotations

import logging
import time
import typing as t
from importlib import resources
import json, sys
import pandas as pd
from pyrogram.raw import functions, types
from pyrogram.errors import GraphInvalidReload, MsgIdInvalid, FloodWait
from singer_sdk.helpers.jsonpath import extract_jsonpath
import datetime as dt

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_telegram.client import TelegramStream
from pyrogram import Client, raw

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = resources.files(__package__) / "schemas"


# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class GroupStream(TelegramStream):
    """Define custom stream."""

    name = "group"
    primary_keys: t.ClassVar[list[str]] = ["id", "date"]
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("date", th.DateType),
        th.Property("title", th.StringType),
        th.Property("description", th.StringType),
        th.Property("channel", th.StringType),
        th.Property("invite_link", th.StringType),
        th.Property("members_total", th.IntegerType),
    ).to_dict()


class GroupSourcesStream(TelegramStream):
    """Define custom stream."""
    records_jsonpath = "$[*]"
    name = "group_sources_members_stat"
    primary_keys: t.ClassVar[list[str]] = ["date", "channel"]
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("channel", th.StringType),
        th.Property("ads", th.IntegerType),
        th.Property("link", th.IntegerType),
        th.Property("similar_channels", th.IntegerType),
        th.Property("shareable_chat", th.IntegerType),
        th.Property("pm", th.IntegerType),
        th.Property("search", th.IntegerType),
        th.Property("groups", th.IntegerType),
    ).to_dict()

    def as_input(self, app, chat):
        p = app.resolve_peer(chat)
        return types.InputChannel(channel_id=p.channel_id,
                                  access_hash=p.access_hash)

    def fetch_stats(self, app, input_ch):
        # канал → broadcast, супергруппа → megagroup
        try:
            return app.invoke(
                functions.stats.GetBroadcastStats(channel=input_ch, dark=False)
            )
        except Exception:
            try:
                return app.invoke(
                    functions.stats.GetMegagroupStats(channel=input_ch)
                )
            except Exception:
                return []

    def load_graph(self, app, token):
        return app.invoke(functions.stats.LoadAsyncGraph(token=token, x=0))

    def get_records(
            self,
            context: Context | None,
    ) -> t.Iterable[dict]:
        API_ID = self.config.get('api_id')  # Ваш API ID
        API_HASH = self.config.get('api_hash')  # Ваш API Hash
        SESSION = self.config.get('session_key')  # Сессия ПОЛЬЗОВАТЕЛЯ, АДМИНА КАНАЛА
        CHANNEL = self.config.get('channel')

        with Client(name="my_account", api_id=API_ID, api_hash=API_HASH, session_string=SESSION) as app:
            ch = self.as_input(app, CHANNEL)

            # 1️⃣ Получаем stats и token
            stats = self.fetch_stats(app, ch)
            try:
                fg = stats.new_followers_by_source_graph
                token = fg.token if isinstance(fg, types.StatsGraphAsync) else fg.zoom_token
                if not token:
                    sys.exit("Граф не содержит token/zoom_token")

                # 2️⃣ Пробуем загрузить график, при ошибке — берём новый token
                for attempt in (1, 2):  # максимум две попытки
                    try:
                        graph = self.load_graph(app, token)
                        break  # успех — выходим из цикла
                    except GraphInvalidReload:
                        if attempt == 2:
                            raise  # повторный сбой → бросаем ошибку
                        stats = self.fetch_stats(app, ch)  # обновляем stats
                        fg = stats.new_followers_by_source_graph
                        token = fg.token if isinstance(fg, types.StatsGraphAsync) else fg.zoom_token
                        if not token:
                            raise RuntimeError("Обновлённый stats тоже без token")

                # 3️⃣ JSON → DataFrame  ➜ берём ровно один день
                data = json.loads(graph.json.data)
                # print(data["names"])
                cols = {c[0]: c[1:] for c in data["columns"]}  # 'x', 'y0', 'y1'
                df = pd.DataFrame(cols)
                df["x"] = pd.to_datetime(df["x"], unit="ms")
                df["x"] = df["x"].astype(str)
                df['channel'] = CHANNEL[1:]
                df.rename(columns=data["names"], inplace=True)
                df.rename(columns={'x': 'date', 'Ads': 'ads', 'URL': 'link', 'Similar Channels': 'similar_channels',
                                   'Shareable Chat Folders': 'shareable_chat', 'PM': 'pm', 'Search': 'search',
                                   'Groups': 'groups'}, inplace=True)

                yield from extract_jsonpath(self.records_jsonpath, input=df.to_dict(orient='records'))
            except Exception:
                df = pd.DataFrame()
                yield from extract_jsonpath(self.records_jsonpath, input=df.to_dict(orient='records'))


class GroupEnabledNotificationsStream(TelegramStream):
    """Define custom stream."""
    records_jsonpath = "$[*]"
    name = "group_enabled_notifications"
    primary_keys: t.ClassVar[list[str]] = ["date", "channel"]
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("channel", th.StringType),
        th.Property("part", th.IntegerType),
        th.Property("total", th.IntegerType),
        th.Property("pct", th.IntegerType),
    ).to_dict()

    def as_input(self, app, chat):
        p = app.resolve_peer(chat)
        return types.InputChannel(channel_id=p.channel_id,
                                  access_hash=p.access_hash)

    def fetch_stats(self, app, input_ch):
        # канал → broadcast, супергруппа → megagroup
        try:
            return app.invoke(
                functions.stats.GetBroadcastStats(channel=input_ch, dark=False)
            )
        except Exception:
            try:
                return app.invoke(
                    functions.stats.GetMegagroupStats(channel=input_ch)
                )
            except Exception:
                return []

    def load_graph(self, app, token):
        return app.invoke(functions.stats.LoadAsyncGraph(token=token, x=0))

    def get_records(
            self,
            context: Context | None,
    ) -> t.Iterable[dict]:
        API_ID = self.config.get('api_id')  # Ваш API ID
        API_HASH = self.config.get('api_hash')  # Ваш API Hash
        SESSION = self.config.get('session_key')  # Сессия ПОЛЬЗОВАТЕЛЯ, АДМИНА КАНАЛА
        CHANNEL = self.config.get('channel')

        with Client(name="my_account", api_id=API_ID, api_hash=API_HASH, session_string=SESSION) as app:
            ch = self.as_input(app, CHANNEL)

            # 1️⃣ Получаем stats и token
            stats = self.fetch_stats(app, ch)
            try:
                fg = stats.enabled_notifications
                if isinstance(fg, types.StatsPercentValue):
                    # всего две цифры – сразу считаем процент
                    pct = fg.part * 100 / fg.total
                    row = {
                        "date": dt.date.today().isoformat(),
                        "part": fg.part,
                        "total": fg.total,
                        "pct": pct,
                        "channel": CHANNEL
                    }
                yield from extract_jsonpath(self.records_jsonpath, input=[row])
            except Exception:
                df = pd.DataFrame()
                yield from extract_jsonpath(self.records_jsonpath, input=df.to_dict(orient='records'))


class GroupMuteStatStream(TelegramStream):
    """Define custom stream."""
    records_jsonpath = "$[*]"
    name = "group_mute_stat"
    primary_keys: t.ClassVar[list[str]] = ["date", "channel"]
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("channel", th.StringType),
        th.Property("Muted", th.IntegerType),
        th.Property("Unmuted", th.IntegerType),
    ).to_dict()

    def as_input(self, app, chat):
        p = app.resolve_peer(chat)
        return types.InputChannel(channel_id=p.channel_id,
                                  access_hash=p.access_hash)

    def fetch_stats(self, app, input_ch):
        # канал → broadcast, супергруппа → megagroup
        try:
            return app.invoke(
                functions.stats.GetBroadcastStats(channel=input_ch, dark=False)
            )
        except Exception:
            try:
                return app.invoke(
                    functions.stats.GetMegagroupStats(channel=input_ch)
                )
            except Exception:
                return []

    def load_graph(self, app, token):
        return app.invoke(functions.stats.LoadAsyncGraph(token=token, x=0))

    def get_records(
            self,
            context: Context | None,
    ) -> t.Iterable[dict]:
        API_ID = self.config.get('api_id')  # Ваш API ID
        API_HASH = self.config.get('api_hash')  # Ваш API Hash
        SESSION = self.config.get('session_key')  # Сессия ПОЛЬЗОВАТЕЛЯ, АДМИНА КАНАЛА
        CHANNEL = self.config.get('channel')

        with Client(name="my_account", api_id=API_ID, api_hash=API_HASH, session_string=SESSION) as app:
            ch = self.as_input(app, CHANNEL)

            # 1️⃣ Получаем stats и token
            stats = self.fetch_stats(app, ch)
            try:
                fg = stats.mute_graph
                token = fg.token if isinstance(fg, types.StatsGraphAsync) else fg.zoom_token
                if not token:
                    sys.exit("Граф не содержит token/zoom_token")

                data = json.loads(fg.json.data)
                # print(data["names"])
                cols = {c[0]: c[1:] for c in data["columns"]}  # 'x', 'y0', 'y1'
                df = pd.DataFrame(cols)
                df["x"] = pd.to_datetime(df["x"], unit="ms")
                df["date"] = df["x"].astype(str)
                df['channel'] = CHANNEL[1:]
                df.rename(columns=data["names"], inplace=True)
                yield from extract_jsonpath(self.records_jsonpath, input=df.to_dict(orient='records'))
            except Exception:
                df = pd.DataFrame()
                yield from extract_jsonpath(self.records_jsonpath, input=df.to_dict(orient='records'))


class GroupViewsSourcesStream(TelegramStream):
    """Define custom stream."""
    records_jsonpath = "$[*]"
    name = "group_sources_views_stat"
    primary_keys: t.ClassVar[list[str]] = ["date", "channel"]
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("channel", th.StringType),
        th.Property("ads", th.IntegerType),
        th.Property("link", th.IntegerType),
        th.Property("similar_channels", th.IntegerType),
        th.Property("channels", th.IntegerType),
        th.Property("pm", th.IntegerType),
        th.Property("search", th.IntegerType),
        th.Property("groups", th.IntegerType),
        th.Property("followers", th.IntegerType),
        th.Property("other", th.IntegerType),
    ).to_dict()

    def as_input(self, app, chat):
        p = app.resolve_peer(chat)
        return types.InputChannel(channel_id=p.channel_id,
                                  access_hash=p.access_hash)

    def fetch_stats(self, app, input_ch):
        # канал → broadcast, супергруппа → megagroup
        try:
            return app.invoke(
                functions.stats.GetBroadcastStats(channel=input_ch, dark=False)
            )
        except Exception:
            try:
                return app.invoke(
                    functions.stats.GetMegagroupStats(channel=input_ch)
                )
            except Exception:
                return []

    def load_graph(self, app, token):
        return app.invoke(functions.stats.LoadAsyncGraph(token=token, x=0))

    def get_records(
            self,
            context: Context | None,
    ) -> t.Iterable[dict]:
        API_ID = self.config.get('api_id')  # Ваш API ID
        API_HASH = self.config.get('api_hash')  # Ваш API Hash
        SESSION = self.config.get('session_key')  # Сессия ПОЛЬЗОВАТЕЛЯ, АДМИНА КАНАЛА
        CHANNEL = self.config.get('channel')

        with Client(name="my_account", api_id=API_ID, api_hash=API_HASH, session_string=SESSION) as app:
            ch = self.as_input(app, CHANNEL)

            # 1️⃣ Получаем stats и token
            stats = self.fetch_stats(app, ch)
            try:
                fg = stats.views_by_source_graph
                token = fg.token if isinstance(fg, types.StatsGraphAsync) else fg.zoom_token
                if not token:
                    sys.exit("Граф не содержит token/zoom_token")

                # 2️⃣ Пробуем загрузить график, при ошибке — берём новый token
                for attempt in (1, 2):  # максимум две попытки
                    try:
                        graph = self.load_graph(app, token)
                        break  # успех — выходим из цикла
                    except GraphInvalidReload:
                        if attempt == 2:
                            raise  # повторный сбой → бросаем ошибку
                        stats = self.fetch_stats(app, ch)  # обновляем stats
                        fg = stats.views_by_source_graph
                        token = fg.token if isinstance(fg, types.StatsGraphAsync) else fg.zoom_token
                        if not token:
                            raise RuntimeError("Обновлённый stats тоже без token")

                # 3️⃣ JSON → DataFrame  ➜ берём ровно один день
                data = json.loads(graph.json.data)
                # print(data["names"])
                cols = {c[0]: c[1:] for c in data["columns"]}  # 'x', 'y0', 'y1'
                df = pd.DataFrame(cols)
                df["x"] = pd.to_datetime(df["x"], unit="ms")
                df["x"] = df["x"].astype(str)
                df['channel'] = CHANNEL[1:]
                df.rename(columns=data["names"], inplace=True)
                df.rename(columns={'x': 'date', 'Ads': 'ads', 'URL': 'link', 'Similar Channels': 'similar_channels',
                                   'Channels': 'channels', 'PM': 'pm', 'Search': 'search', 'Groups': 'groups',
                                   'Followers': 'followers', 'Other': 'other'}, inplace=True)

                yield from extract_jsonpath(self.records_jsonpath, input=df.to_dict(orient='records'))
            except Exception:
                df = pd.DataFrame()
                yield from extract_jsonpath(self.records_jsonpath, input=df.to_dict(orient='records'))


class GroupLanguagesStream(TelegramStream):
    """Define custom stream."""
    records_jsonpath = "$[*]"
    name = "group_languages_stat"
    primary_keys: t.ClassVar[list[str]] = ["date", "channel"]
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("channel", th.StringType),
        th.Property("Russian", th.IntegerType),
        th.Property("Ukrainian", th.IntegerType),
        th.Property("English", th.IntegerType),
        th.Property("German", th.IntegerType),
        th.Property("Italian", th.IntegerType),
        th.Property("Spanish", th.IntegerType),
        th.Property("Latvian", th.IntegerType),
        th.Property("Uzbek", th.IntegerType),
        th.Property("French", th.IntegerType),
    ).to_dict()

    def as_input(self, app, chat):
        p = app.resolve_peer(chat)
        return types.InputChannel(channel_id=p.channel_id,
                                  access_hash=p.access_hash)

    def fetch_stats(self, app, input_ch):
        # канал → broadcast, супергруппа → megagroup
        try:
            return app.invoke(
                functions.stats.GetBroadcastStats(channel=input_ch, dark=False)
            )
        except Exception:
            try:
                return app.invoke(
                    functions.stats.GetMegagroupStats(channel=input_ch)
                )
            except Exception:
                return []

    def load_graph(self, app, token):
        return app.invoke(functions.stats.LoadAsyncGraph(token=token, x=0))

    def get_records(
            self,
            context: Context | None,
    ) -> t.Iterable[dict]:
        API_ID = self.config.get('api_id')  # Ваш API ID
        API_HASH = self.config.get('api_hash')  # Ваш API Hash
        SESSION = self.config.get('session_key')  # Сессия ПОЛЬЗОВАТЕЛЯ, АДМИНА КАНАЛА
        CHANNEL = self.config.get('channel')

        with Client(name="my_account", api_id=API_ID, api_hash=API_HASH, session_string=SESSION) as app:
            ch = self.as_input(app, CHANNEL)

            # 1️⃣ Получаем stats и token
            stats = self.fetch_stats(app, ch)
            try:
                fg = stats.languages_graph
                token = fg.token if isinstance(fg, types.StatsGraphAsync) else fg.zoom_token
                if not token:
                    sys.exit("Граф не содержит token/zoom_token")

                # 2️⃣ Пробуем загрузить график, при ошибке — берём новый token
                for attempt in (1, 2):  # максимум две попытки
                    try:
                        graph = self.load_graph(app, token)
                        break  # успех — выходим из цикла
                    except GraphInvalidReload:
                        if attempt == 2:
                            raise  # повторный сбой → бросаем ошибку
                        stats = self.fetch_stats(app, ch)  # обновляем stats
                        fg = stats.languages_graph
                        token = fg.token if isinstance(fg, types.StatsGraphAsync) else fg.zoom_token
                        if not token:
                            raise RuntimeError("Обновлённый stats тоже без token")

                # 3️⃣ JSON → DataFrame  ➜ берём ровно один день
                data = json.loads(graph.json.data)
                # print(data["names"])
                cols = {c[0]: c[1:] for c in data["columns"]}  # 'x', 'y0', 'y1'
                df = pd.DataFrame(cols)
                df["x"] = pd.to_datetime(df["x"], unit="ms")
                df["x"] = df["x"].astype(str)
                df['channel'] = CHANNEL[1:]
                df.rename(columns=data["names"], inplace=True)
                df.rename(columns={'x': 'date'}, inplace=True)
                yield from extract_jsonpath(self.records_jsonpath, input=df.to_dict(orient='records'))
            except Exception:
                df = pd.DataFrame()
                yield from extract_jsonpath(self.records_jsonpath, input=df.to_dict(orient='records'))


class GroupFollowersStream(TelegramStream):
    """Define custom stream."""
    records_jsonpath = "$[*]"
    name = "group_followers_stat"
    primary_keys: t.ClassVar[list[str]] = ["date", "channel"]
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("channel", th.StringType),
        th.Property("Joined", th.IntegerType),
        th.Property("Left", th.IntegerType)
    ).to_dict()

    def as_input(self, app, chat):
        p = app.resolve_peer(chat)
        return types.InputChannel(channel_id=p.channel_id,
                                  access_hash=p.access_hash)

    def fetch_stats(self, app, input_ch):
        # канал → broadcast, супергруппа → megagroup
        try:
            return app.invoke(
                functions.stats.GetBroadcastStats(channel=input_ch, dark=False)
            )
        except Exception:
            try:
                return app.invoke(
                    functions.stats.GetMegagroupStats(channel=input_ch)
                )
            except Exception:
                return []

    def load_graph(self, app, token):
        return app.invoke(functions.stats.LoadAsyncGraph(token=token, x=0))

    def get_records(
            self,
            context: Context | None,
    ) -> t.Iterable[dict]:
        API_ID = self.config.get('api_id')  # Ваш API ID
        API_HASH = self.config.get('api_hash')  # Ваш API Hash
        SESSION = self.config.get('session_key')  # Сессия ПОЛЬЗОВАТЕЛЯ, АДМИНА КАНАЛА
        CHANNEL = self.config.get('channel')

        with Client(name="my_account", api_id=API_ID, api_hash=API_HASH, session_string=SESSION) as app:
            ch = self.as_input(app, CHANNEL)

            # 1️⃣ Получаем stats и token
            stats = self.fetch_stats(app, ch)
            try:
                fg = stats.followers_graph

                # 3️⃣ JSON → DataFrame  ➜ берём ровно один день
                data = json.loads(fg.json.data)
                # print(data["names"])
                cols = {c[0]: c[1:] for c in data["columns"]}  # 'x', 'y0', 'y1'
                df = pd.DataFrame(cols)
                df["x"] = pd.to_datetime(df["x"], unit="ms")
                df["x"] = df["x"].astype(str)
                df['channel'] = CHANNEL[1:]
                df.rename(columns=data["names"], inplace=True)
                df.rename(columns={'x': 'date'}, inplace=True)

                yield from extract_jsonpath(self.records_jsonpath, input=df.to_dict(orient='records'))
            except Exception:
                df = pd.DataFrame()
                yield from extract_jsonpath(self.records_jsonpath, input=df.to_dict(orient='records'))


class GroupInteractionsStream(TelegramStream):
    """Define custom stream."""
    records_jsonpath = "$[*]"
    name = "group_interactions_stat"
    primary_keys: t.ClassVar[list[str]] = ["date", "channel"]
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("channel", th.StringType),
        th.Property("Views", th.IntegerType),
        th.Property("Shares", th.IntegerType),
    ).to_dict()

    def as_input(self, app, chat):
        p = app.resolve_peer(chat)
        return types.InputChannel(channel_id=p.channel_id,
                                  access_hash=p.access_hash)

    def fetch_stats(self, app, input_ch):
        # канал → broadcast, супергруппа → megagroup
        try:
            return app.invoke(
                functions.stats.GetBroadcastStats(channel=input_ch, dark=False)
            )
        except Exception:
            try:
                return app.invoke(
                    functions.stats.GetMegagroupStats(channel=input_ch)
                )
            except Exception:
                return []

    def load_graph(self, app, token):
        return app.invoke(functions.stats.LoadAsyncGraph(token=token, x=0))

    def get_records(
            self,
            context: Context | None,
    ) -> t.Iterable[dict]:
        API_ID = self.config.get('api_id')  # Ваш API ID
        API_HASH = self.config.get('api_hash')  # Ваш API Hash
        SESSION = self.config.get('session_key')  # Сессия ПОЛЬЗОВАТЕЛЯ, АДМИНА КАНАЛА
        CHANNEL = self.config.get('channel')

        with Client(name="my_account", api_id=API_ID, api_hash=API_HASH, session_string=SESSION) as app:
            ch = self.as_input(app, CHANNEL)
            # 1️⃣ Получаем stats и token
            stats = self.fetch_stats(app, ch)
            try:
                fg = stats.interactions_graph
                token = fg.token if isinstance(fg, types.StatsGraphAsync) else fg.zoom_token
                if not token:
                    sys.exit("Граф не содержит token/zoom_token")

                # 2️⃣ Пробуем загрузить график, при ошибке — берём новый token
                for attempt in (1, 2):  # максимум две попытки
                    try:
                        graph = self.load_graph(app, token)
                        break  # успех — выходим из цикла
                    except GraphInvalidReload:
                        if attempt == 2:
                            raise  # повторный сбой → бросаем ошибку
                        stats = self.fetch_stats(app, ch)  # обновляем stats
                        fg = stats.interactions_graph
                        token = fg.token if isinstance(fg, types.StatsGraphAsync) else fg.zoom_token
                        if not token:
                            raise RuntimeError("Обновлённый stats тоже без token")

                data = json.loads(graph.json.data)
                # print(data["names"])
                cols = {c[0]: c[1:] for c in data["columns"]}  # 'x', 'y0', 'y1'
                df = pd.DataFrame(cols)
                df["x"] = pd.to_datetime(df["x"], unit="ms")
                df["x"] = df["x"].astype(str)
                df['channel'] = CHANNEL[1:]
                df.rename(columns=data["names"], inplace=True)
                df.rename(columns={'x': 'date'}, inplace=True)
                yield from extract_jsonpath(self.records_jsonpath, input=df.to_dict(orient='records'))
            except Exception:
                df = pd.DataFrame()
                yield from extract_jsonpath(self.records_jsonpath, input=df.to_dict(orient='records'))


class GroupStoryInteractionsStream(TelegramStream):
    """Define custom stream."""
    records_jsonpath = "$[*]"
    name = "group_story_interactions_stat"
    primary_keys: t.ClassVar[list[str]] = ["date", "channel"]
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("date", th.DateType),
        th.Property("channel", th.StringType),
        th.Property("Views", th.IntegerType),
        th.Property("Shares", th.IntegerType),
    ).to_dict()

    def as_input(self, app, chat):
        p = app.resolve_peer(chat)
        return types.InputChannel(channel_id=p.channel_id,
                                  access_hash=p.access_hash)

    def fetch_stats(self, app, input_ch):
        # канал → broadcast, супергруппа → megagroup
        try:
            return app.invoke(
                functions.stats.GetBroadcastStats(channel=input_ch, dark=False)
            )
        except Exception:
            try:
                return app.invoke(
                    functions.stats.GetMegagroupStats(channel=input_ch)
                )
            except Exception:
                return []

    def load_graph(self, app, token):
        return app.invoke(functions.stats.LoadAsyncGraph(token=token, x=0))

    def get_records(
            self,
            context: Context | None,
    ) -> t.Iterable[dict]:
        API_ID = self.config.get('api_id')  # Ваш API ID
        API_HASH = self.config.get('api_hash')  # Ваш API Hash
        SESSION = self.config.get('session_key')  # Сессия ПОЛЬЗОВАТЕЛЯ, АДМИНА КАНАЛА
        CHANNEL = self.config.get('channel')
        df = pd.DataFrame()

        with Client(name="my_account", api_id=API_ID, api_hash=API_HASH, session_string=SESSION) as app:
            ch = self.as_input(app, CHANNEL)

            # 1️⃣ Получаем stats и token
            stats = self.fetch_stats(app, ch)
            try:
                fg = stats.story_interactions_graph
                token = fg.token if isinstance(fg, types.StatsGraphAsync) else fg.zoom_token
                if not token:
                    sys.exit("Граф не содержит token/zoom_token")

                # 2️⃣ Пробуем загрузить график, при ошибке — берём новый token
                for attempt in (1, 2):  # максимум две попытки
                    try:
                        graph = self.load_graph(app, token)
                        break  # успех — выходим из цикла
                    except GraphInvalidReload:
                        if attempt == 2:
                            raise  # повторный сбой → бросаем ошибку
                        stats = self.fetch_stats(app, ch)  # обновляем stats
                        fg = stats.story_interactions_graph
                        token = fg.token if isinstance(fg, types.StatsGraphAsync) else fg.zoom_token
                        if not token:
                            raise RuntimeError("Обновлённый stats тоже без token")
                try:
                    data = json.loads(graph.json.data)
                    # print(data["names"])
                    cols = {c[0]: c[1:] for c in data["columns"]}  # 'x', 'y0', 'y1'
                    df = pd.DataFrame(cols)
                    df["x"] = pd.to_datetime(df["x"], unit="ms")
                    df["x"] = df["x"].astype(str)
                    df['channel'] = CHANNEL[1:]
                    df.rename(columns=data["names"], inplace=True)
                    df.rename(columns={'x': 'date'}, inplace=True)
                except AttributeError as e:
                    pass
                yield from extract_jsonpath(self.records_jsonpath, input=df.to_dict(orient='records'))
            except Exception:
                df = pd.DataFrame()
                yield from extract_jsonpath(self.records_jsonpath, input=df.to_dict(orient='records'))


class PostsStream(TelegramStream):
    """Define custom stream."""
    records_jsonpath = "$[*]"
    name = "posts"
    primary_keys: t.ClassVar[list[str]] = ["post_id", "channel"]
    replication_key = "post_id"

    schema = th.PropertiesList(
        th.Property("channel", th.StringType),
        th.Property("post_id", th.IntegerType),
        th.Property("created", th.DateType),
        th.Property("text", th.StringType),
        th.Property("views", th.IntegerType),
        th.Property("forwards", th.IntegerType),
        th.Property("reactions", th.StringType),
        th.Property("link", th.StringType),
    ).to_dict()

    def get_records(
            self,
            context: Context | None,
    ) -> t.Iterable[dict]:
        API_ID = self.config.get('api_id')  # Ваш API ID
        API_HASH = self.config.get('api_hash')  # Ваш API Hash
        SESSION = self.config.get('session_key')  # Сессия ПОЛЬЗОВАТЕЛЯ, АДМИНА КАНАЛА
        CHANNEL = self.config.get('channel')
        N_POSTS = 500

        with Client(name="my_account", api_id=API_ID, api_hash=API_HASH, session_string=SESSION) as app:
            # 1️⃣ берём N последних сообщений (history)
            msgs = [m for m in app.get_chat_history(CHANNEL, limit=N_POSTS)]

            rows = []

            for m in msgs:
                link = '-'
                if m.caption_entities:
                    for e in m.caption_entities:
                        if str(e.type) == "MessageEntityType.TEXT_LINK":
                            link = e.url
                            break
                row = {
                    "channel": CHANNEL[1:],
                    "post_id": m.id,
                    "created": m.date,
                    "text": (m.text or m.caption or ""),  # первые 100 символов
                    "views": m.views,
                    "forwards": m.forwards,
                    "reactions": m.reactions,
                    "link": link
                }

                rows.append(row)
            yield from extract_jsonpath(self.records_jsonpath, input=rows)


class CommentsStream(TelegramStream):
    """Define custom stream."""
    records_jsonpath = "$[*]"
    name = "comments"
    primary_keys: t.ClassVar[list[str]] = ["post_id", "id", "channel"]
    replication_key = "id"

    schema = th.PropertiesList(
        th.Property("channel", th.StringType),
        th.Property("post_id", th.IntegerType),
        th.Property("id", th.IntegerType),
        th.Property("date", th.DateType),
        th.Property("author", th.StringType),
        th.Property("first_name", th.StringType),
        th.Property("last_name", th.StringType),
        th.Property("username", th.StringType),
        th.Property("text", th.StringType),
    ).to_dict()

    def get_records(
            self,
            context: Context | None,
    ) -> t.Iterable[dict]:
        API_ID = self.config.get('api_id')  # Ваш API ID
        API_HASH = self.config.get('api_hash')  # Ваш API Hash
        SESSION = self.config.get('session_key')  # Сессия ПОЛЬЗОВАТЕЛЯ, АДМИНА КАНАЛА
        CHANNEL = self.config.get('channel')
        N_POSTS = 500

        with Client(name="my_account", api_id=API_ID, api_hash=API_HASH, session_string=SESSION) as app:
            # 1️⃣ берём N последних сообщений (history)
            rows = []
            for post in app.get_chat_history(CHANNEL, limit=N_POSTS):
                if getattr(post, "reply_to_message_id", 0):
                    # это уже чья-то реплика, а не корневой пост
                    continue
                try:
                    comments = list(app.get_discussion_replies(post.chat.id, post.id))  # ← главное изменение
                except FloodWait as fw:
                    time.sleep(fw.value + 1)
                    comments = list(app.get_discussion_replies(post.chat.id, post.id))  # повторяем тот же запрос
                except MsgIdInvalid:
                    # нет треда – пропускаем, чтобы не обрушить sync-цикл
                    continue
                for c in comments:
                    rows.append({
                        "channel": CHANNEL[1:],
                        "post_id": post.id,
                        "author": c.from_user.id if c.from_user else 'anon',
                        "text": c.text,
                        "id": c.id,
                        "date": c.date,
                        "first_name": c.from_user.first_name if c.from_user and c.from_user.first_name else '-',
                        "last_name": c.from_user.last_name if c.from_user and c.from_user.last_name else '-',
                        "username": c.from_user.username if c.from_user and c.from_user.username else '-'
                    })
            yield from extract_jsonpath(self.records_jsonpath, input=rows)


class StoryStream(TelegramStream):
    """Define custom stream."""
    records_jsonpath = "$[*]"
    name = "stories"
    primary_keys: t.ClassVar[list[str]] = ["id", "channel"]
    replication_key = "id"

    schema = th.PropertiesList(
        th.Property("channel", th.StringType),
        th.Property("id", th.IntegerType),
        th.Property("created", th.DateType),
        th.Property("expire_date", th.DateType),
        th.Property("link", th.StringType),
        th.Property("views", th.IntegerType),
        th.Property("forwards", th.IntegerType),
        th.Property("reactions", th.IntegerType),
        th.Property("reactions_json", th.StringType),
    ).to_dict()

    def to_input_channel(self, app, chat):
        p = app.resolve_peer(chat)
        return types.InputChannel(channel_id=p.channel_id,
                                  access_hash=p.access_hash)

    def get_records(
            self,
            context: Context | None,
    ) -> t.Iterable[dict]:
        API_ID = self.config.get('api_id')  # Ваш API ID
        API_HASH = self.config.get('api_hash')  # Ваш API Hash
        SESSION = self.config.get('session_key')  # Сессия ПОЛЬЗОВАТЕЛЯ, АДМИНА КАНАЛА
        CHANNEL = self.config.get('channel')

        with Client(name="my_account", api_id=API_ID, api_hash=API_HASH, session_string=SESSION) as app:
            peer = app.resolve_peer(CHANNEL)  # PeerChannel
            stories = app.invoke(
                functions.stories.GetPeerStories(
                    peer=peer
                )
            )  # → stories.PeerStories
            rows = []
            link = '-'
            for item in stories.stories.stories:
                if item.media_areas and len(item.media_areas) != 0 and item.media_areas[0].url:
                    link = item.media_areas[0].url
                row = {
                    "channel": CHANNEL[1:],
                    "id": item.id,
                    "created": dt.datetime.utcfromtimestamp(item.date),
                    "expire_date": dt.datetime.utcfromtimestamp(item.expire_date),
                    "link": link,  # первые 100 символов
                    "views": item.views.views_count,
                    "forwards": item.views.forwards_count,
                    "reactions": item.views.reactions_count,
                    "reactions_json": item.views.reactions
                }
                rows.append(row)
        yield from extract_jsonpath(self.records_jsonpath, input=rows)


class InviteLinkStream(TelegramStream):
    """Define custom stream."""
    records_jsonpath = "$[*]"
    name = "invite_links"
    primary_keys: t.ClassVar[list[str]] = ["link"]
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("channel", th.StringType),
        th.Property("link", th.StringType),
        th.Property("creator_id", th.IntegerType),
        th.Property("created", th.DateType),
        th.Property("date", th.DateType),
        th.Property("revoked", th.BooleanType),
        th.Property("permanent", th.BooleanType),
        th.Property("request_needed", th.BooleanType),
        th.Property("joined_cnt", th.IntegerType),
        th.Property("name", th.StringType),
    ).to_dict()

    def fetch_all_invites(self, app: Client, peer: types.InputPeerChannel):
        invites, offset_date, offset_link = [], 0, ""
        while True:
            try:
                me = app.resolve_peer("me")  # InputUserSelf
                r = app.invoke(
                    functions.messages.GetExportedChatInvites(
                        peer=peer,
                        admin_id=me,  # обязательный параметр
                        revoked=False,
                        offset_date=offset_date,
                        offset_link=offset_link,
                        limit=100
                    )
                )
            except FloodWait as e:
                print(f"⚠️ Flood-wait на {e.value} сек…")
                time.sleep(e.value)
                continue

            invites.extend(r.invites)
            if len(r.invites) < 100:
                break  # получили всё
            # пагинация: берём «хвост» предыдущего результата
            offset_date = r.invites[-1].date
            offset_link = r.invites[-1].link
        return invites

    def to_input_channel(self, app, chat):
        p = app.resolve_peer(chat)
        return types.InputChannel(channel_id=p.channel_id,
                                  access_hash=p.access_hash)

    def get_records(
            self,
            context: Context | None,
    ) -> t.Iterable[dict]:
        API_ID = self.config.get('api_id')  # Ваш API ID
        API_HASH = self.config.get('api_hash')  # Ваш API Hash
        SESSION = self.config.get('session_key')  # Сессия ПОЛЬЗОВАТЕЛЯ, АДМИНА КАНАЛА
        CHANNEL = self.config.get('channel')

        with Client(name="my_account", api_id=API_ID, api_hash=API_HASH, session_string=SESSION) as app:
            peer = app.resolve_peer(CHANNEL)  # InputPeerChannel
            invites = self.fetch_all_invites(app, peer)
            rows = []
            for idx, inv in enumerate(invites, 1):
                row = {
                    "channel": CHANNEL[1:],
                    "link": inv.link,
                    "creator_id": inv.admin_id,
                    "created": dt.datetime.fromtimestamp(inv.date, tz=dt.timezone.utc),
                    "date": dt.date.today().isoformat(),
                    "name": inv.title,
                    "joined_cnt": inv.usage,
                    "revoked": inv.revoked,
                    "permanent": inv.permanent,
                    "request_needed": inv.request_needed
                }
                rows.append(row)
        yield from extract_jsonpath(self.records_jsonpath, input=rows)


class InviteLinkUsersStream(TelegramStream):
    """Define custom stream."""
    records_jsonpath = "$[*]"
    name = "invite_link_users"
    primary_keys: t.ClassVar[list[str]] = ["link", "user_id"]
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("channel", th.StringType),
        th.Property("link", th.StringType),
        th.Property("user_id", th.IntegerType),
        th.Property("date", th.DateType),
        th.Property("requested", th.BooleanType),
        th.Property("via_chatlist", th.BooleanType),
        th.Property("name", th.StringType),
        th.Property("first_name", th.StringType),
        th.Property("last_name", th.StringType),
        th.Property("username", th.StringType),
    ).to_dict()

    def fetch_invite_importers(self, app: Client, peer, link_hash: str, limit=100):
        # первый запрос
        r: types.messages.ChatInviteImporters = app.invoke(
            raw.functions.messages.GetChatInviteImporters(
                peer=peer,
                link=link_hash,  # только hash!
                q="",  # пустая строка = без фильтра
                offset_date=0,
                offset_user=raw.types.InputUserEmpty(),
                limit=limit
            )
        )
        users = r.users
        importers = r.importers
        while r.count > len(importers) and len(importers) != 0:
            if len(r.importers) < limit:
                break

            # если импортёров нет вообще, дальше тоже искать нечего
            if not r.importers:
                break
            last_imp = r.importers[-1]
            offset_date = last_imp.date
            u = next(u for u in r.users if u.id == last_imp.user_id)
            offset_user = raw.types.InputUser(user_id=u.id, access_hash=u.access_hash)
            r: types.messages.ChatInviteImporters = app.invoke(
                raw.functions.messages.GetChatInviteImporters(
                    peer=peer,
                    link=link_hash,  # только hash!
                    q="",  # пустая строка = без фильтра
                    offset_date=offset_date,
                    offset_user=offset_user,
                    limit=limit
                )
            )
            users.extend(r.users)
            importers.extend(r.importers)

        return importers, users

    def fetch_all_invites(self, app: Client, peer: types.InputPeerChannel):
        invites, offset_date, offset_link = [], 0, ""
        while True:
            try:
                me = app.resolve_peer("me")  # InputUserSelf
                r = app.invoke(
                    functions.messages.GetExportedChatInvites(
                        peer=peer,
                        admin_id=me,  # обязательный параметр
                        revoked=False,
                        offset_date=offset_date,
                        offset_link=offset_link,
                        limit=100
                    )
                )
            except FloodWait as e:
                print(f"⚠️ Flood-wait на {e.value} сек…")
                time.sleep(e.value)
                continue

            invites.extend(r.invites)
            if len(r.invites) < 100:
                break  # получили всё
            # пагинация: берём «хвост» предыдущего результата
            offset_date = r.invites[-1].date
            offset_link = r.invites[-1].link
        return invites

    def to_input_channel(self, app, chat):
        p = app.resolve_peer(chat)
        return types.InputChannel(channel_id=p.channel_id,
                                  access_hash=p.access_hash)

    def get_records(
            self,
            context: Context | None,
    ) -> t.Iterable[dict]:
        API_ID = self.config.get('api_id')  # Ваш API ID
        API_HASH = self.config.get('api_hash')  # Ваш API Hash
        SESSION = self.config.get('session_key')  # Сессия ПОЛЬЗОВАТЕЛЯ, АДМИНА КАНАЛА
        CHANNEL = self.config.get('channel')

        with Client(name="my_account", api_id=API_ID, api_hash=API_HASH, session_string=SESSION) as app:
            peer = app.resolve_peer(CHANNEL)  # InputPeerChannel
            invites = self.fetch_all_invites(app, peer)
            rows = []
            for idx, inv in enumerate(invites, 1):
                importers, users = self.fetch_invite_importers(app, peer, inv.link)
                for imp in importers:
                    user = {}
                    for u in users:
                        if u.id == imp.user_id:
                            user = u
                            break
                    row = {
                        "channel": CHANNEL[1:],
                        "link": inv.link,
                        "user_id": imp.user_id,
                        "date": dt.datetime.fromtimestamp(imp.date, tz=dt.timezone.utc),
                        "name": inv.title,
                        "requested": imp.requested,
                        "via_chatlist": imp.via_chatlist,
                        "first_name": user.first_name if user.first_name and user.first_name else '-',
                        "last_name": user.last_name if user.last_name and user.last_name else '-',
                        "username": user.username if user.username and user.username else '-'
                    }
                    rows.append(row)
        yield from extract_jsonpath(self.records_jsonpath, input=rows)


class EventsLogStream(TelegramStream):
    """Define custom stream."""
    records_jsonpath = "$[*]"
    name = "events_groups_log"
    primary_keys: t.ClassVar[list[str]] = ["event_id"]
    replication_key = "date"

    schema = th.PropertiesList(
        th.Property("channel", th.StringType),
        th.Property("event_id", th.StringType),
        th.Property("user_id", th.IntegerType),
        th.Property("date", th.DateType),
        th.Property("event_type", th.StringType),
        th.Property("invite_link", th.StringType),
        th.Property("invite_link_title", th.StringType),
        th.Property("invite_admin_id", th.StringType),
    ).to_dict()

    def get_records(
            self,
            context: Context | None,
    ) -> t.Iterable[dict]:
        API_ID = self.config.get('api_id')  # Ваш API ID
        API_HASH = self.config.get('api_hash')  # Ваш API Hash
        SESSION = self.config.get('session_key')  # Сессия ПОЛЬЗОВАТЕЛЯ, АДМИНА КАНАЛА
        CHANNEL = self.config.get('channel')

        with Client(name="my_account", api_id=API_ID, api_hash=API_HASH, session_string=SESSION) as app:
            peer = app.resolve_peer(CHANNEL)  # InputPeerChannel
            # --- 1. резолвим peer в InputPeer ---

            ev_filter = types.ChannelAdminLogEventsFilter(
                join=True,
                leave=True,
                invite=True,
                ban=True,
                unban=True,
                kick=True,
                unkick=True
            )
            max_id = 0
            record = []
            while True:
                result = app.invoke(
                    functions.channels.GetAdminLog(
                        channel=peer,
                        q="",  # поиск по строке, '' = всё
                        events_filter=ev_filter,
                        max_id=max_id,  # диапазон msg_id, 0 = без ограничений
                        min_id=0,
                        limit=100  # сколько записей вернуть
                    )
                )
                if not result.events:
                    break
                for ev in result.events:
                    link = None
                    title = None
                    admin_id = None
                    if isinstance(ev.action, types.ChannelAdminLogEventActionParticipantJoinByInvite):
                        type_event = 'join_by_invite'
                        if ev.action.invite and ev.action.invite.link:
                            link = ev.action.invite.link
                        if ev.action.invite and ev.action.invite.title:
                            title = ev.action.invite.title
                        if ev.action.invite and ev.action.invite.admin_id:
                            admin_id = ev.action.invite.admin_id
                    if isinstance(ev.action, types.ChannelAdminLogEventActionParticipantLeave):
                        type_event = 'leave'
                    if isinstance(ev.action, types.ChannelAdminLogEventActionParticipantJoin):
                        type_event = 'join'
                    if isinstance(ev.action, types.ChannelAdminLogEventActionParticipantInvite):
                        type_event = 'invite'
                    record.append({
                        "channel": CHANNEL[1:],
                        "event_id": ev.id,
                        "event_type": type_event,
                        "user_id": ev.user_id,
                        "date": dt.datetime.fromtimestamp(ev.date, tz=dt.timezone.utc),
                        "invite_link": link,
                        "invite_link_title": title,
                        "invite_admin_id": admin_id
                    })
                max_id = result.events[-1].id
        yield from extract_jsonpath(self.records_jsonpath, input=record)
