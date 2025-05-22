"""Stream type classes for tap-telegram."""

from __future__ import annotations

import logging
import time
import typing as t
from importlib import resources
import json, sys
import pandas as pd
from pyrogram import Client
from pyrogram.raw import functions, types
from pyrogram.errors import GraphInvalidReload, MsgIdInvalid, FloodWait
from singer_sdk.helpers.jsonpath import extract_jsonpath
import datetime as dt

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_telegram.client import TelegramStream

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
                    comments = list(app.get_discussion_replies(post.chat.id, post.id))# ← главное изменение
                except FloodWait as fw:
                    time.sleep(fw.value + 1)
                    comments = list(app.get_discussion_replies(post.chat.id, post.id)) # повторяем тот же запрос
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
            for item in stories.stories.stories:
                row = {
                    "channel": CHANNEL[1:],
                    "id": item.id,
                    "created": dt.datetime.utcfromtimestamp(item.date),
                    "expire_date": dt.datetime.utcfromtimestamp(item.expire_date),
                    "link": item.media_areas[0].url,  # первые 100 символов
                    "views": item.views.views_count,
                    "forwards": item.views.forwards_count,
                    "reactions": item.views.reactions_count,
                    "reactions_json": item.views.reactions
                }
                rows.append(row)
        yield from extract_jsonpath(self.records_jsonpath, input=rows)
