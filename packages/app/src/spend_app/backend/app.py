from .data_cache import DataCacheDependency  # noqa: F401 -- triggers registration
from .core import create_app
from .router import router

app = create_app(routers=[router])
