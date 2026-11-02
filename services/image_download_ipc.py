"""图片下载专用 IPC；仅回环 TCP，认证后传输有界帧，不写图片或账号到磁盘。"""
from __future__ import annotations

import asyncio
import base64
import json
import struct

HEADER = struct.Struct('!cQI')
CHUNK = 256 * 1024
MAX_CONTROL = 1024 * 1024
MAX_BODY = 64 * 1024 * 1024


def _json_default(value):
    if isinstance(value, bytes):
        return {'__download_bytes__': base64.b64encode(value).decode('ascii')}
    raise TypeError('unsupported download transport option')


def _json_hook(value):
    if set(value) == {'__download_bytes__'}:
        return base64.b64decode(value['__download_bytes__'], validate=True)
    return value


def encode(value):
    return json.dumps(value, default=_json_default, separators=(',', ':')).encode('utf8')


def decode(value):
    return json.loads(value, object_hook=_json_hook)


async def receive(reader):
    kind, identity, length = HEADER.unpack(await reader.readexactly(HEADER.size))
    if length > (CHUNK if kind == b'B' else MAX_CONTROL):
        raise ValueError('download IPC frame exceeds limit')
    return kind, identity, await reader.readexactly(length)


async def send(writer, lock, kind, identity, payload=b''):
    if kind != b'B':
        payload = encode(payload)
    if len(payload) > (CHUNK if kind == b'B' else MAX_CONTROL):
        raise ValueError('download IPC frame exceeds limit')
    async with lock:
        writer.write(HEADER.pack(kind, identity, len(payload)))
        writer.write(payload)
        await writer.drain()


def cookie_values(jar):
    return [{**vars(cookie), 'rest': dict(cookie._rest)} for cookie in jar]


def restore_cookies(jar, values):
    from http.cookiejar import Cookie
    for fields in values:
        fields = dict(fields)
        fields.pop('_rest', None)
        jar.set_cookie(Cookie(**fields))
