from __future__ import annotations

import os
import sys
import platform
import shutil
import subprocess
import urllib.request
from pathlib import Path
from typing import Dict

FONT_SOURCES: Dict[str, str] = {
    "NotoSans-Regular.ttf": "https://github.com/googlefonts/noto-fonts/raw/main/hinted/ttf/NotoSans/NotoSans-Regular.ttf",
    "NotoSansSymbols2-Regular.ttf": "https://github.com/googlefonts/noto-fonts/raw/main/hinted/ttf/NotoSansSymbols2/NotoSansSymbols2-Regular.ttf",
    "NotoSansCJK-Regular.ttc": "https://github.com/googlefonts/noto-cjk/raw/main/Sans/OTC/NotoSansCJK-Regular.ttc",
    "NotoColorEmoji.ttf": "https://github.com/googlefonts/noto-emoji/raw/main/fonts/NotoColorEmoji.ttf",
    "NotoEmoji-Regular.ttf": "https://github.com/googlefonts/noto-emoji/raw/main/fonts/NotoEmoji-Regular.ttf",
}


def ensure_utf8() -> None:
    os.environ.setdefault("PYTHONUTF8", "1")
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None:
            continue
        try:
            stream.reconfigure(encoding="utf-8")
        except Exception:
            pass


def _download_font(url: str, dest: Path) -> None:
    if dest.exists() and dest.stat().st_size > 0:
        return
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    with urllib.request.urlopen(url, timeout=30) as response:
        data = response.read()
    tmp.write_bytes(data)
    tmp.replace(dest)


def _install_user_fonts(fonts_dir: Path) -> None:
    system = platform.system().lower()
    if system == "linux":
        target = Path.home() / ".local" / "share" / "fonts" / "sosiski"
    elif system == "darwin":
        target = Path.home() / "Library" / "Fonts" / "sosiski"
    elif system == "windows":
        base = Path(os.environ.get("LOCALAPPDATA", str(Path.home())))
        target = base / "Microsoft" / "Windows" / "Fonts"
    else:
        return
    target.mkdir(parents=True, exist_ok=True)
    for name in FONT_SOURCES:
        src = fonts_dir / name
        if not src.exists():
            continue
        dst = target / name
        if not dst.exists() or dst.stat().st_size != src.stat().st_size:
            try:
                shutil.copy2(src, dst)
            except Exception:
                pass
    if system == "linux":
        try:
            subprocess.run(["fc-cache", "-f", str(target)], check=False)
        except Exception:
            pass


def ensure_fonts(base_dir: Path) -> None:
    fonts_dir = base_dir / "fonts"
    fonts_dir.mkdir(parents=True, exist_ok=True)
    for name, url in FONT_SOURCES.items():
        try:
            _download_font(url, fonts_dir / name)
        except Exception:
            continue
    _install_user_fonts(fonts_dir)

    font_paths = [str(fonts_dir / name) for name in FONT_SOURCES]
    existing = os.environ.get("SOSISKI_FONT_PATHS", "")
    extra = [part.strip() for part in existing.split(";") if part.strip()]
    combined = []
    for path in font_paths + extra:
        if path not in combined:
            combined.append(path)
    if combined:
        os.environ["SOSISKI_FONT_PATHS"] = ";".join(combined)
