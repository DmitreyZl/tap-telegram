"""Telegram tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_telegram import streams


class Taptelegram(Tap):
    """Telegram tap class."""

    name = "tap-telegram"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_id",
            th.IntegerType(nullable=False),
            required=True,
            secret=True,  # Flag config as protected.
        ),
        th.Property(
            "api_hash",
            th.StringType(nullable=False),
            required=True,
            secret=True,
        ),
        th.Property(
            "session_key",
            th.StringType(nullable=False),
            required=True,
            secret=True,
        ),
        th.Property(
            "channel",
            th.StringType(nullable=False),
            required=True,
            secret=True,
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.TelegramStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.GroupStream(self),
            streams.GroupSourcesStream(self),
            streams.GroupViewsSourcesStream(self),
            streams.GroupLanguagesStream(self),
            streams.GroupInteractionsStream(self),
            streams.GroupStoryInteractionsStream(self),
            streams.PostsStream(self),
            streams.StoryStream(self),
            streams.CommentsStream(self),
            streams.GroupMuteStatStream(self),
            streams.GroupEnabledNotificationsStream(self),
            streams.InviteLinkStream(self),
            streams.InviteLinkUsersStream(self),
            streams.GroupFollowersStream(self),
            streams.EventsLogStream(self)
        ]


if __name__ == "__main__":
    TapTelegram.cli()
