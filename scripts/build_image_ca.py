"""镜像构建时生成图片传输专用 CA 哈希目录。"""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from curl_cffi.curl import DEFAULT_CACERT
from services.image_ca import CA_DIRECTORY, build_ca_directory

if __name__ == '__main__':
    count = build_ca_directory(Path(DEFAULT_CACERT), CA_DIRECTORY)
    print(f'Prepared {count} image TLS trust certificates')
