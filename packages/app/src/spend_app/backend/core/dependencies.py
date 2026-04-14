from __future__ import annotations

from typing import TypeAlias
from ._defaults import ConfigDependency, ClientDependency, UserWorkspaceClientDependency
from ._headers import HeadersDependency


class Dependencies:
    """FastAPI dependency injection shorthand for route handler parameters."""

    Client: TypeAlias = ClientDependency
    UserClient: TypeAlias = UserWorkspaceClientDependency
    Config: TypeAlias = ConfigDependency
    Headers: TypeAlias = HeadersDependency
