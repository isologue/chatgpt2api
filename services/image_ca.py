"""复用 certifi 的信任集合，避免每个独立 TLS 连接解析整份 CA 文件。"""
from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
from functools import lru_cache
from pathlib import Path

from curl_cffi import CurlOpt
from curl_cffi.aio import ffi
from curl_cffi.curl import DEFAULT_CACERT

CA_DIRECTORY = Path(sys.prefix) / 'share' / 'chatgpt2api-ca'
_CA_OPTIONS = (CurlOpt.CAINFO, CurlOpt.CAPATH, CurlOpt.CAINFO_BLOB, CurlOpt.SSL_CTX_FUNCTION)


def build_ca_directory(bundle: Path, target: Path) -> int:
    """镜像构建时准备同一份 CA 的哈希目录；失败则停止构建。"""
    payload = bundle.read_bytes()
    certificates = re.findall(rb'-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----', payload, re.S)
    if not certificates:
        raise ValueError('CA bundle contains no certificates')
    # 专用构建目录必须不存在，避免旧证书混入新的信任集合。
    target.mkdir(parents=True, exist_ok=False)
    for index, certificate in enumerate(certificates):
        (target / f'root-{index}.pem').write_bytes(certificate + b'\n')
    subprocess.run(['openssl', 'rehash', str(target)], check=True)
    # openssl 对部分坏证书只给警告；必须确认哈希索引没有漏掉任何信任根。
    indexed = {entry.read_bytes().strip() for entry in target.iterdir()
        if re.fullmatch(r'[0-9a-f]{8}\.[0-9]+', entry.name)}
    if indexed != {certificate.strip() for certificate in certificates}:
        raise ValueError('CA hash directory does not cover the complete bundle')
    (target / 'manifest.json').write_text(json.dumps({
        'version': 1, 'sha256': hashlib.sha256(payload).hexdigest(),
        'certificates': len(certificates),
    }), encoding='utf8')
    return len(certificates)


@lru_cache(maxsize=1)
def _default_ca_directory() -> str | None:
    if sys.platform != 'linux':
        return None
    try:
        manifest = json.loads((CA_DIRECTORY / 'manifest.json').read_text(encoding='utf8'))
        if (manifest.get('version') != 1 or not manifest.get('certificates')
                or manifest.get('sha256') != hashlib.sha256(Path(DEFAULT_CACERT).read_bytes()).hexdigest()):
            return None
    except (OSError, ValueError, TypeError, AttributeError):
        return None
    return str(CA_DIRECTORY)


def image_ca_options(options: dict, verify: bool | str | None) -> dict:
    # 自定义 CA、关闭校验以及原生 SSL 上下文选项，保持调用方原来的语义。
    if verify is not True or any(option in options for option in _CA_OPTIONS):
        return options
    directory = _default_ca_directory()
    if directory is None:
        return options
    # ffi.NULL 明确取消 CAINFO，不能用 None；curl_cffi 会把 None 拒绝为非法指针。
    # 证书链和主机名校验均保持开启，目录内容与 DEFAULT_CACERT 完全一致。
    return {**options, CurlOpt.CAINFO: ffi.NULL, CurlOpt.CAPATH: directory}
