#!/usr/bin/env python3
"""Install pinned local PoC tools without modifying the global Python installation."""
import hashlib
import platform
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import venv
import zipfile

ROOT = Path(__file__).resolve().parents[2]
TOOLS = ROOT / '.poc-tools'
VERSION = '1.8.1'
MANIFEST_SHA256 = '623b62f6ead2ac46f161a8c859bd1679f4a87ea0e7d1a4c48f9a46f648873b52'


def download(url, destination):
    subprocess.run(['curl', '--fail', '--location', '--silent', '--show-error', '--retry', '2',
                    '--output', str(destination), url], check=True)


def main():
    TOOLS.mkdir(exist_ok=True)
    python = TOOLS / 'venv/bin/python'
    if not python.exists():
        venv.create(TOOLS / 'venv', with_pip=True)
    subprocess.run([str(python), '-m', 'pip', 'install', '--only-binary=:all:', '-r',
                    str(ROOT / 'scripts/poc/requirements.txt')], check=True)
    system = {'Darwin': 'macos', 'Linux': 'linux'}.get(platform.system())
    arch = {'arm64': 'arm64', 'aarch64': 'arm64', 'x86_64': 'amd64'}.get(platform.machine())
    if not system or not arch:
        raise SystemExit('Supported k6 platforms: macOS/Linux arm64/amd64')
    ext = 'zip' if system == 'macos' else 'tar.gz'
    name = f'k6-v{VERSION}-{system}-{arch}.{ext}'
    base = f'https://github.com/grafana/k6/releases/download/v{VERSION}/'
    with tempfile.TemporaryDirectory(dir=TOOLS) as temporary:
        directory = Path(temporary)
        manifest = directory / 'checksums.txt'
        download(base + f'k6-v{VERSION}-checksums.txt', manifest)
        if hashlib.sha256(manifest.read_bytes()).hexdigest() != MANIFEST_SHA256:
            raise SystemExit('k6 checksum manifest mismatch')
        hashes = {line.split()[1].lstrip('*'): line.split()[0] for line in manifest.read_text().splitlines()}
        archive = directory / name
        download(base + name, archive)
        if hashlib.sha256(archive.read_bytes()).hexdigest() != hashes[name]:
            raise SystemExit('k6 archive checksum mismatch')
        # Copy only the binary, never extract arbitrary archive paths.
        if ext == 'zip':
            with zipfile.ZipFile(archive) as source:
                member, = [x for x in source.namelist() if x.endswith('/k6')]
                data = source.read(member)
        else:
            with tarfile.open(archive) as source:
                member, = [x for x in source.getmembers() if x.isfile() and x.name.endswith('/k6')]
                data = source.extractfile(member).read()
        (TOOLS / 'k6').write_bytes(data)
        (TOOLS / 'k6').chmod(0o755)
    subprocess.run([str(TOOLS / 'k6'), 'version'], check=True)
    print('Ready: .poc-tools/venv/bin/python scripts/poc/run.py --suite smoke', flush=True)


if __name__ == '__main__':
    main()
