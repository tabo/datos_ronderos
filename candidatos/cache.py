import json
from functools import wraps
from json import JSONDecodeError
from typing import Any, ParamSpec

import httpx

from candidatos import MAIN_DIR
from candidatos.logs import log


P = ParamSpec("P")

_known = set()


class CacheNotFound(Exception):
    pass


def cachejson(dataset: str, category: str, key_base: str) -> Any:
    def decorator(fn: object) -> object:
        @wraps(fn)
        def return_cache(*args: P.args, **kwargs: P.kwargs) -> Any:
            key = get_key(dataset, category, key_base, *args, **kwargs)
            try:
                res = get_from_cache(key)
            except CacheNotFound:
                log.debug("cache_miss", key=key)
                res: Any = fn(*args, **kwargs)
                pp = cache_path(key)
                pp.write_text(
                    json.dumps(res, ensure_ascii=False, indent=4),
                    newline="\n",
                    encoding="utf-8",
                )
                log.debug("cache_store", key=key, size=f"{pp.stat().st_size:,}")
            _known.add(key)
            return res

        return return_cache

    return decorator


def get_value(key, fn):
    good_result = False
    try:
        res = get_from_cache(key)
        good_result = True
    except CacheNotFound:
        log.debug("cache_miss", key=key)
        try:
            res = fn()
        except httpx.HTTPStatusError:
            log.exception("HTTP error", key=key)
        else:
            good_result = True
            pp = cache_path(key)
            pp.write_text(
                json.dumps(res, ensure_ascii=False, indent=4),
                newline="\n",
                encoding="utf-8",
            )
        log.debug("cache_store", key=key, size=f"{pp.stat().st_size:,}")
    if good_result:
        _known.add(key)
        return res


def is_memory_cached(key):
    return key in _known


def is_cached(key):
    if cache_path(key).is_file():
        log.debug("file_cache_hit", key=key)
        return True
    return False


def get_from_cache(key):
    p = cache_path(key)
    if p.is_file():
        log.debug("file_cache_hit", key=key, size=f"{p.stat().st_size:,}")
        return json.loads(p.read_text(encoding="utf-8"))
    raise CacheNotFound


def cache_path(key):
    p = MAIN_DIR / "_cache" / f"{key}.json"
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def get_key(
    dataset: str, category: str, key_base: str, *args: P.args, **kwargs: P.kwargs
):
    key: str = f"{dataset}/{category}/{key_base}"
    for i, arg in enumerate(args):
        if i == 0 and not isinstance(arg, (str, int, float, bool)):
            continue
        if key and not key.endswith("/"):
            key += "-"
        key += arg
    for argkey, argval in sorted(kwargs.items()):
        if key and not key.endswith("/"):
            key += "-"
        key += f"{argkey}={argval}"
    return key
