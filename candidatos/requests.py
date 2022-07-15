from contextlib import contextmanager

import httpx
from latest_user_agents import get_random_user_agent


def raise_on_4xx_5xx(response):
    response.raise_for_status()


@contextmanager
def get_client():
    ua: str = get_random_user_agent()
    yield httpx.Client(
        headers={"User-Agent": ua},
        verify=False,
        timeout=None,
        limits=httpx.Limits(max_keepalive_connections=8, max_connections=8),
        event_hooks={'response': [raise_on_4xx_5xx]}
    )
