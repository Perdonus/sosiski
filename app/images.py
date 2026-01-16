from __future__ import annotations

import colorsys
import os
import random
import shutil
import subprocess
import tempfile
import textwrap
import unicodedata
from io import BytesIO
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

from PIL import (
    Image,
    ImageDraw,
    ImageFilter,
    ImageFont,
    ImageOps,
    ImageSequence,
    ImageStat,
)

from cards import Card, card_display_name, card_file_path
from config import (
    BASE_DIR,
    KAZIK_DIGITS,
    KAZIK_DIGIT_SIZE,
    KAZIK_IMAGE_HEIGHT,
    KAZIK_IMAGE_WIDTH,
    KAZIK_SLOT_GAP,
    KAZIK_SLOT_RADIUS,
    KAZIK_SUBTITLE_SIZE,
    KAZIK_TITLE_SIZE,
    LEADERBOARD_AVATAR_SIZE,
    LEADERBOARD_BG,
    LEADERBOARD_ENTRY_SIZE,
    LEADERBOARD_HEADER_TO_ROWS_GAP,
    LEADERBOARD_OUTER_MARGIN,
    LEADERBOARD_PLATE_PADDING,
    LEADERBOARD_ROW_GAP,
    LEADERBOARD_TITLE_SIZE,
    MENU_IMAGE_HEIGHT,
    MENU_IMAGE_WIDTH,
    MENU_SUBTITLE_SIZE,
    MENU_TITLE_SIZE,
    PHOTO_CACHE_DIR,
    PROFILE_INFO_SIZE,
    PROFILE_TITLE_SIZE,
    PROFILE_FONT_PATH,
    PROFILE_FONT_CJK_PATH,
    PROFILE_FONT_SYMBOL_PATH,
    BASE_FONT_PATH,
    BASE_FONT_CJK_PATH,
    BASE_FONT_SYMBOL_PATH,
    SOSISKI_FONT_PATH,
    SOSISKI_FONT_PATHS,
    LOGO_FILE,
    IMAGE_CACHE_VERSION,
    SYMBOL_FONT_NAMES,
    FONT_CANDIDATES,
    CJK_FONT_NAMES,
    RARITY_NAMES,
)
from app.utils import build_kazik_text_line


SHOWCASE_RARITY_COLORS: Dict[str, Tuple[int, int, int]] = {
    "dno": (120, 120, 120),
    "common": (110, 170, 120),
    "uncommon": (80, 160, 170),
    "rare": (90, 140, 220),
    "epic": (210, 120, 80),
    "legendary": (220, 170, 70),
    "platinum": (190, 200, 210),
    "meme": (100, 220, 120),
    "exclusive": (240, 210, 120),
}


def truncate_text(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def font_line_height(font: ImageFont.FreeTypeFont) -> int:
    try:
        ascent, descent = font.getmetrics()
        return ascent + descent
    except Exception:
        bbox = font.getbbox("Hg")
        return bbox[3] - bbox[1]


def fit_text_to_width(
    text: str, max_width: int, font: ImageFont.FreeTypeFont, draw: ImageDraw.ImageDraw
) -> str:
    if not text:
        return ""
    if draw.textlength(text, font=font) <= max_width:
        return text
    ellipsis = "..."
    trimmed = text
    while trimmed and draw.textlength(trimmed + ellipsis, font=font) > max_width:
        trimmed = trimmed[:-1]
    return trimmed + ellipsis if trimmed else ellipsis


def load_truetype_font(path: Path, size: int) -> ImageFont.FreeTypeFont:
    if hasattr(ImageFont, "LAYOUT_RAQM"):
        return ImageFont.truetype(
            str(path), size=size, layout_engine=ImageFont.LAYOUT_RAQM
        )
    return ImageFont.truetype(str(path), size=size)


def pick_font_from_candidates(
    size: int, candidates: List[Path]
) -> ImageFont.FreeTypeFont:
    for font_path in candidates:
        if font_path.exists():
            try:
                return load_truetype_font(font_path, size=size)
            except Exception:
                continue
    return ImageFont.load_default()


def collect_font_candidates() -> List[Path]:
    env_paths = []
    if SOSISKI_FONT_PATH:
        env_paths.append(Path(SOSISKI_FONT_PATH))
    if SOSISKI_FONT_PATHS:
        env_paths.extend(
            Path(part.strip()) for part in SOSISKI_FONT_PATHS.split(";") if part.strip()
        )
    return env_paths + FONT_CANDIDATES


def split_text_by_script(text: str) -> List[Tuple[str, str]]:
    if not text:
        return []
    result: List[Tuple[str, str]] = []
    buffer = []
    current = None
    for char in text:
        code = ord(char)
        if (
            code > 0x1F000
            or char in "\u200d\ufe0f"
            or unicodedata.category(char) in {"So", "Sk"}
        ):
            script = "symbol"
        elif code > 0x3000:
            script = "cjk"
        else:
            script = "base"
        if current and script != current:
            result.append((current, "".join(buffer)))
            buffer = []
        buffer.append(char)
        current = script
    if buffer:
        result.append((current or "base", "".join(buffer)))
    return result


def text_length_mixed(
    text: str,
    draw: ImageDraw.ImageDraw,
    font_base: ImageFont.FreeTypeFont,
    font_cjk: ImageFont.FreeTypeFont,
    font_symbol: ImageFont.FreeTypeFont,
) -> int:
    width = 0
    for script, chunk in split_text_by_script(text):
        font = font_base
        if script == "cjk":
            font = font_cjk
        elif script == "symbol":
            font = font_symbol
        width += int(draw.textlength(chunk, font=font))
    return width


def render_text_layer_mixed(
    text: str,
    font_base: ImageFont.FreeTypeFont,
    font_cjk: ImageFont.FreeTypeFont,
    font_symbol: ImageFont.FreeTypeFont,
    fill: Tuple[int, int, int, int],
) -> Image.Image:
    if not text:
        return Image.new("RGBA", (1, 1), (0, 0, 0, 0))
    dummy = ImageDraw.Draw(Image.new("RGB", (1, 1)))
    width = int(text_length_mixed(text, dummy, font_base, font_cjk, font_symbol) + 0.5)
    width = max(1, width)
    height = max(
        font_line_height(font_base),
        font_line_height(font_cjk),
        font_line_height(font_symbol),
    )
    layer = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    layer_draw = ImageDraw.Draw(layer)
    draw_text_mixed(
        layer_draw,
        (0, 0),
        text,
        font_base,
        font_cjk,
        font_symbol,
        fill,
    )
    return layer


def fit_text_to_width_mixed(
    text: str,
    max_width: int,
    draw: ImageDraw.ImageDraw,
    font_base: ImageFont.FreeTypeFont,
    font_cjk: ImageFont.FreeTypeFont,
    font_symbol: ImageFont.FreeTypeFont,
) -> str:
    if text_length_mixed(text, draw, font_base, font_cjk, font_symbol) <= max_width:
        return text
    ellipsis = "..."
    trimmed = text
    while trimmed:
        if (
            text_length_mixed(trimmed + ellipsis, draw, font_base, font_cjk, font_symbol)
            <= max_width
        ):
            return trimmed + ellipsis
        trimmed = trimmed[:-1]
    return ellipsis


def wrap_text_mixed(
    text: str,
    max_width: int,
    draw: ImageDraw.ImageDraw,
    font_base: ImageFont.FreeTypeFont,
    font_cjk: ImageFont.FreeTypeFont,
    font_symbol: ImageFont.FreeTypeFont,
) -> List[str]:
    words = textwrap.wrap(text, width=40) if text else []
    lines: List[str] = []
    for chunk in words:
        current = ""
        for word in chunk.split():
            candidate = word if not current else f"{current} {word}"
            if (
                text_length_mixed(candidate, draw, font_base, font_cjk, font_symbol)
                <= max_width
            ):
                current = candidate
            else:
                if current:
                    lines.append(current)
                current = word
        if current:
            lines.append(current)
    return lines


def build_showcase_card_art(
    title: str,
    effect_text: str,
    rarity: str,
    size: Tuple[int, int],
) -> Image.Image:
    width, height = size
    base = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    gradient = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(gradient)
    for y in range(height):
        shade = 20 + int(45 * (y / height))
        draw.line([(0, y), (width, y)], fill=(shade, shade, shade, 220))
    base.alpha_composite(gradient)

    draw = ImageDraw.Draw(base)
    border_color = SHOWCASE_RARITY_COLORS.get(rarity, (200, 200, 200))
    draw.rounded_rectangle(
        (6, 6, width - 6, height - 6),
        radius=26,
        fill=(18, 18, 18, 230),
        outline=(*border_color, 220),
        width=4,
    )
    if rarity == "meme":
        glow = Image.new("RGBA", (width, height), (0, 0, 0, 0))
        glow_draw = ImageDraw.Draw(glow)
        glow_draw.rounded_rectangle(
            (4, 4, width - 4, height - 4),
            radius=26,
            outline=(*border_color, 140),
            width=6,
        )
        glow = glow.filter(ImageFilter.GaussianBlur(radius=6))
        base.alpha_composite(glow)

    title_font = pick_font_bundle(24)
    body_font = pick_font_bundle(18)
    title_base, title_cjk, title_sym = title_font
    body_base, body_cjk, body_sym = body_font

    draw = ImageDraw.Draw(base)
    max_text_width = width - 32
    title_text = fit_text_to_width_mixed(
        title, max_text_width, draw, title_base, title_cjk, title_sym
    )
    title_w = text_length_mixed(title_text, draw, title_base, title_cjk, title_sym)
    title_x = (width - title_w) // 2
    title_y = 18
    draw_text_mixed(
        draw,
        (title_x, title_y),
        title_text,
        title_base,
        title_cjk,
        title_sym,
        (240, 240, 240, 255),
    )

    lines = wrap_text_mixed(
        effect_text, max_text_width, draw, body_base, body_cjk, body_sym
    )
    lines = lines[:4]
    line_height = max(
        font_line_height(body_base),
        font_line_height(body_cjk),
        font_line_height(body_sym),
    )
    block_h = len(lines) * line_height + max(0, len(lines) - 1) * 6
    start_y = (height - block_h) // 2
    for idx, line in enumerate(lines):
        text_w = text_length_mixed(line, draw, body_base, body_cjk, body_sym)
        text_x = (width - text_w) // 2
        text_y = start_y + idx * (line_height + 6)
        draw_text_mixed(
            draw,
            (text_x, text_y),
            line,
            body_base,
            body_cjk,
            body_sym,
            (210, 210, 210, 255),
        )

    rarity_label = RARITY_NAMES.get(rarity, rarity).strip()
    rarity_text = fit_text_to_width_mixed(
        rarity_label, max_text_width, draw, body_base, body_cjk, body_sym
    )
    rarity_w = text_length_mixed(
        rarity_text, draw, body_base, body_cjk, body_sym
    )
    draw_text_mixed(
        draw,
        ((width - rarity_w) // 2, height - 40),
        rarity_text,
        body_base,
        body_cjk,
        body_sym,
        (180, 180, 180, 255),
    )
    return base


def build_showcase_card_image(
    title: str,
    effect_text: str,
    rarity: str,
    size: Tuple[int, int] = (280, 400),
) -> BytesIO:
    image = build_showcase_card_art(title, effect_text, rarity, size)
    output = BytesIO()
    image.convert("RGB").save(output, format="JPEG", quality=92)
    output.seek(0)
    return output


def build_showcase_board_image(
    slots: List[Optional[Tuple[str, str, str]]],
) -> BytesIO:
    width, height = MENU_IMAGE_WIDTH, MENU_IMAGE_HEIGHT
    base = None
    if LEADERBOARD_BG.exists():
        try:
            bg = Image.open(LEADERBOARD_BG).convert("RGB")
            base = ImageOps.fit(bg, (width, height), method=Image.LANCZOS)
            base = base.filter(ImageFilter.GaussianBlur(radius=12))
        except Exception:
            base = None
    if base is None:
        base = Image.new("RGB", (width, height), "#1b1b1b")
        draw = ImageDraw.Draw(base)
        for y in range(height):
            shade = 24 + int(55 * (y / height))
            draw.line([(0, y), (width, y)], fill=(shade, shade, shade))

    base = base.convert("RGBA")
    plate_w = int(width * 0.92)
    plate_h = int(height * 0.76)
    plate_x = (width - plate_w) // 2
    plate_y = (height - plate_h) // 2
    plate = Image.new("RGBA", (plate_w, plate_h), (0, 0, 0, 0))
    plate_draw = ImageDraw.Draw(plate)
    plate_draw.rounded_rectangle(
        (0, 0, plate_w, plate_h),
        radius=32,
        fill=(0, 0, 0, 180),
    )
    base.alpha_composite(plate, (plate_x, plate_y))

    card_w = int(plate_w * 0.28)
    card_h = int(plate_h * 0.82)
    gap = int(plate_w * 0.04)
    start_x = plate_x + (plate_w - (card_w * 3 + gap * 2)) // 2
    start_y = plate_y + (plate_h - card_h) // 2

    for idx in range(3):
        slot = slots[idx] if idx < len(slots) else None
        if slot:
            title, effect_text, rarity = slot
        else:
            title, effect_text, rarity = ("Пусто", "Слот свободен", "common")
        card_img = build_showcase_card_art(title, effect_text, rarity, (card_w, card_h))
        x = start_x + idx * (card_w + gap)
        base.alpha_composite(card_img, (x, start_y))

    apply_corner_logo(base)

    output = BytesIO()
    base.convert("RGB").save(output, format="JPEG", quality=92)
    output.seek(0)
    return output


def draw_text_mixed(
    draw: ImageDraw.ImageDraw,
    position: Tuple[int, int],
    text: str,
    font_base: ImageFont.FreeTypeFont,
    font_cjk: ImageFont.FreeTypeFont,
    font_symbol: ImageFont.FreeTypeFont,
    fill: Tuple[int, int, int, int],
) -> None:
    x, y = position
    for script, chunk in split_text_by_script(text):
        font = font_base
        if script == "cjk":
            font = font_cjk
        elif script == "symbol":
            font = font_symbol
        draw.text((x, y), chunk, font=font, fill=fill)
        x += int(draw.textlength(chunk, font=font))


def pick_font_bundle(size: int) -> Tuple[ImageFont.FreeTypeFont, ImageFont.FreeTypeFont, ImageFont.FreeTypeFont]:
    candidates = collect_font_candidates()
    base_font = _load_optional_font(BASE_FONT_PATH, size) or pick_font_from_candidates(size, candidates)
    cjk_font = base_font
    symbol_font = base_font
    for font_path in candidates:
        if font_path.name in CJK_FONT_NAMES and font_path.exists():
            try:
                cjk_font = load_truetype_font(font_path, size=size)
                break
            except Exception:
                continue
    for font_path in candidates:
        if font_path.name in SYMBOL_FONT_NAMES and font_path.exists():
            try:
                symbol_font = load_truetype_font(font_path, size=size)
                break
            except Exception:
                continue
    custom_cjk = _load_optional_font(BASE_FONT_CJK_PATH, size)
    if custom_cjk:
        cjk_font = custom_cjk
    custom_symbol = _load_optional_font(BASE_FONT_SYMBOL_PATH, size)
    if custom_symbol:
        symbol_font = custom_symbol
    return base_font, cjk_font, symbol_font


def _load_optional_font(
    font_path: Optional[Path],
    size: int,
) -> Optional[ImageFont.FreeTypeFont]:
    if not font_path or not font_path.exists():
        return None
    try:
        return load_truetype_font(font_path, size=size)
    except Exception:
        return None


def pick_profile_font_bundle(
    size: int,
) -> Tuple[ImageFont.FreeTypeFont, ImageFont.FreeTypeFont, ImageFont.FreeTypeFont]:
    base_font, cjk_font, symbol_font = pick_font_bundle(size)
    custom_base = _load_optional_font(PROFILE_FONT_PATH, size)
    if custom_base:
        base_font = custom_base
    custom_cjk = _load_optional_font(PROFILE_FONT_CJK_PATH, size)
    if custom_cjk:
        cjk_font = custom_cjk
    custom_symbol = _load_optional_font(PROFILE_FONT_SYMBOL_PATH, size)
    if custom_symbol:
        symbol_font = custom_symbol
    return base_font, cjk_font, symbol_font


def pick_font(size: int) -> ImageFont.FreeTypeFont:
    return pick_font_bundle(size)[0]


def pick_font_for_text(text: str, size: int) -> ImageFont.FreeTypeFont:
    if not text:
        return pick_font(size)
    for font_path in collect_font_candidates():
        if font_path.exists():
            try:
                font = load_truetype_font(font_path, size=size)
            except Exception:
                continue
            if "\u4e00" <= max(text) <= "\u9fff":
                return font
            if font.getmask(text).getbbox():
                return font
    return pick_font(size)


_logo_template: Optional[Image.Image] = None


def load_logo_template() -> Optional[Image.Image]:
    global _logo_template
    if _logo_template is not None:
        return _logo_template
    candidates = []
    if LOGO_FILE:
        candidates.append(LOGO_FILE)
    candidates.extend(["logo.webp", "logo.png", "logo.jpg", "logo.jpeg"])
    for name in candidates:
        path = BASE_DIR / name
        if not path.exists() or not path.is_file():
            continue
        try:
            _logo_template = Image.open(path).convert("RGBA")
            return _logo_template
        except Exception:
            continue
    _logo_template = None
    return None


def pick_logo_colors(image: Image.Image, box: Tuple[int, int, int, int]) -> Tuple[
    Tuple[int, int, int, int], Tuple[int, int, int, int]
]:
    try:
        region = image.crop(box).convert("RGB")
        r, g, b = ImageStat.Stat(region).mean
        luminance = (0.2126 * r + 0.7152 * g + 0.0722 * b) / 255.0
        if luminance >= 0.6:
            fg = (0, 0, 0, 255)
        elif luminance <= 0.4:
            fg = (255, 255, 255, 255)
        else:
            contrast_white = 1.05 / (luminance + 0.05)
            contrast_black = (luminance + 0.05) / 0.05
            fg = (
                (255, 255, 255, 255)
                if contrast_white >= contrast_black
                else (0, 0, 0, 255)
            )
    except Exception:
        fg = (255, 255, 255, 255)
    shadow = (0, 0, 0, 255) if fg[0] > 0 else (255, 255, 255, 255)
    return fg, shadow


def build_logo_stamp(
    logo: Image.Image,
    size: int,
    fg: Tuple[int, int, int, int],
    shadow: Tuple[int, int, int, int],
) -> Image.Image:
    logo_img = ImageOps.contain(logo, (size, size), method=Image.LANCZOS)
    alpha = logo_img.getchannel("A")
    fg_logo = Image.new("RGBA", logo_img.size, fg)
    fg_logo.putalpha(alpha)
    shadow_logo = Image.new("RGBA", logo_img.size, shadow)
    shadow_alpha = alpha.point(lambda a: int(a * 0.7))
    shadow_logo.putalpha(shadow_alpha)
    shadow_logo = shadow_logo.filter(ImageFilter.GaussianBlur(radius=3))

    stamp = Image.new("RGBA", logo_img.size, (0, 0, 0, 0))
    stamp.alpha_composite(shadow_logo, (2, 2))
    stamp.alpha_composite(fg_logo, (0, 0))
    return stamp


def apply_corner_logo(image: Image.Image) -> None:
    logo = load_logo_template()
    if logo is None:
        return
    if image.mode != "RGBA":
        image_rgba = image.convert("RGBA")
        image.paste(image_rgba)

    width, height = image.size
    size = max(26, int(min(width, height) * 0.09))
    logo_img = ImageOps.contain(logo, (size, size), method=Image.LANCZOS)
    margin = max(14, size // 3)
    x = max(0, width - margin - logo_img.width)
    y = margin
    box = (x, y, x + logo_img.width, y + logo_img.height)
    fg, shadow = pick_logo_colors(image, box)
    stamp = build_logo_stamp(logo, size, fg, shadow)
    image.alpha_composite(stamp, (x, y))


def build_logo_stamp_for_image(image: Image.Image) -> Tuple[Image.Image, Tuple[int, int]]:
    logo = load_logo_template()
    if logo is None:
        raise RuntimeError("Logo not found")
    width, height = image.size
    size = max(26, int(min(width, height) * 0.09))
    margin = max(14, size // 3)
    x = max(0, width - margin - size)
    y = margin
    box = (x, y, x + size, y + size)
    fg, shadow = pick_logo_colors(image, box)
    stamp = build_logo_stamp(logo, size, fg, shadow)
    return stamp, (x, y)


def ensure_exclusive_cache_dir() -> Path:
    cache_dir = PHOTO_CACHE_DIR / "exclusive"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def exclusive_cache_path(source: Path) -> Path:
    cache_version = IMAGE_CACHE_VERSION
    filename = f"{source.stem}_wm_{cache_version}{source.suffix.lower()}"
    return ensure_exclusive_cache_dir() / filename


def watermark_exclusive_image(source: Path, target: Path) -> bool:
    try:
        image = Image.open(source).convert("RGBA")
        stamp, position = build_logo_stamp_for_image(image)
        image.alpha_composite(stamp, position)
        target.parent.mkdir(parents=True, exist_ok=True)
        suffix = target.suffix.lower()
        if suffix == ".webp":
            image.save(target, format="WEBP")
        elif suffix == ".png":
            image.save(target, format="PNG")
        else:
            image.convert("RGB").save(target)
        return True
    except Exception:
        return False


def watermark_exclusive_gif(source: Path, target: Path) -> bool:
    try:
        base = Image.open(source)
        frames = []
        durations = []
        for frame in ImageSequence.Iterator(base):
            frame_rgba = frame.convert("RGBA")
            stamp, position = build_logo_stamp_for_image(frame_rgba)
            frame_rgba.alpha_composite(stamp, position)
            frames.append(frame_rgba)
            durations.append(frame.info.get("duration", base.info.get("duration", 100)))
        if not frames:
            return False
        target.parent.mkdir(parents=True, exist_ok=True)
        frames[0].save(
            target,
            save_all=True,
            append_images=frames[1:],
            loop=base.info.get("loop", 0),
            duration=durations,
            disposal=base.info.get("disposal", 2),
            optimize=False,
        )
        return True
    except Exception:
        return False


def watermark_exclusive_video(source: Path, target: Path) -> bool:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        return False
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_dir_path = Path(tmp_dir)
            sample_path = tmp_dir_path / "sample.png"
            logo_path = tmp_dir_path / "logo.png"
            subprocess.run(
                [
                    ffmpeg,
                    "-y",
                    "-i",
                    str(source),
                    "-frames:v",
                    "1",
                    str(sample_path),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
            )
            sample = Image.open(sample_path).convert("RGBA")
            stamp, position = build_logo_stamp_for_image(sample)
            stamp.save(logo_path, format="PNG")

            x, y = position
            filters = f"overlay={x}:{y}"
            target.parent.mkdir(parents=True, exist_ok=True)
            codec_args = []
            if target.suffix.lower() == ".webm":
                codec_args = [
                    "-c:v",
                    "libvpx-vp9",
                    "-b:v",
                    "0",
                    "-crf",
                    "32",
                    "-c:a",
                    "libopus",
                ]
            else:
                codec_args = [
                    "-c:v",
                    "libx264",
                    "-crf",
                    "23",
                    "-preset",
                    "veryfast",
                    "-pix_fmt",
                    "yuv420p",
                    "-c:a",
                    "aac",
                ]
            subprocess.run(
                [
                    ffmpeg,
                    "-y",
                    "-i",
                    str(source),
                    "-i",
                    str(logo_path),
                    "-filter_complex",
                    filters,
                    "-map",
                    "0:v:0",
                    "-map",
                    "0:a?",
                    *codec_args,
                    str(target),
                ],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=True,
            )
        return True
    except Exception:
        return False


def get_exclusive_media_path(source: Path) -> Path:
    if not source.exists():
        return source
    target = exclusive_cache_path(source)
    if target.exists() and target.stat().st_size > 0:
        return target
    suffix = source.suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png", ".webp"}:
        if watermark_exclusive_image(source, target):
            return target
    elif suffix == ".gif":
        if watermark_exclusive_gif(source, target):
            return target
    elif suffix in {".mp4", ".webm"}:
        if watermark_exclusive_video(source, target):
            return target
    return source


def ensure_card_cache_dir() -> Path:
    cache_dir = PHOTO_CACHE_DIR / "cards"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def card_cache_path(source: Path) -> Path:
    cache_version = IMAGE_CACHE_VERSION
    prefix = f"{source.parent.name}_{source.stem}"
    filename = f"{prefix}_wm_{cache_version}{source.suffix.lower()}"
    return ensure_card_cache_dir() / filename


def get_watermarked_media_path(source: Path) -> Path:
    if not source.exists():
        return source
    target = card_cache_path(source)
    if target.exists() and target.stat().st_size > 0:
        return target
    suffix = source.suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png", ".webp"}:
        if watermark_exclusive_image(source, target):
            return target
    elif suffix == ".gif":
        if watermark_exclusive_gif(source, target):
            return target
    elif suffix in {".mp4", ".webm"}:
        if watermark_exclusive_video(source, target):
            return target
    return source


def get_card_media_path(card: Card) -> Path:
    path = card_file_path(card)
    return get_watermarked_media_path(path)


def build_menu_image(title: str, subtitle: Optional[str] = None) -> BytesIO:
    width, height = MENU_IMAGE_WIDTH, MENU_IMAGE_HEIGHT
    base = None
    if LEADERBOARD_BG.exists():
        try:
            bg = Image.open(LEADERBOARD_BG).convert("RGB")
            base = ImageOps.fit(bg, (width, height), method=Image.LANCZOS)
            base = base.filter(ImageFilter.GaussianBlur(radius=14))
        except Exception:
            base = None
    if base is None:
        base = Image.new("RGB", (width, height), "#1b1b1b")
        draw = ImageDraw.Draw(base)
        for y in range(height):
            shade = 20 + int(70 * (y / height))
            draw.line([(0, y), (width, y)], fill=(shade, shade, shade))

    base = base.convert("RGBA")
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    plate_w = int(width * 0.86)
    plate_h = int(height * 0.65)
    plate_x = (width - plate_w) // 2
    plate_y = (height - plate_h) // 2
    draw.rounded_rectangle(
        (plate_x, plate_y, plate_x + plate_w, plate_y + plate_h),
        radius=32,
        fill=(0, 0, 0, 170),
    )
    base.alpha_composite(overlay)

    draw = ImageDraw.Draw(base)
    title_base, title_cjk, title_sym = pick_font_bundle(MENU_TITLE_SIZE)
    subtitle_base, subtitle_cjk, subtitle_sym = pick_font_bundle(MENU_SUBTITLE_SIZE)
    max_text_width = int(plate_w * 0.9)
    title_text = fit_text_to_width_mixed(
        title, max_text_width, draw, title_base, title_cjk, title_sym
    )
    subtitle_text = (
        fit_text_to_width_mixed(
            subtitle, max_text_width, draw, subtitle_base, subtitle_cjk, subtitle_sym
        )
        if subtitle
        else None
    )

    title_w = text_length_mixed(title_text, draw, title_base, title_cjk, title_sym)
    title_h = max(
        font_line_height(title_base),
        font_line_height(title_cjk),
        font_line_height(title_sym),
    )
    if subtitle_text:
        subtitle_w = text_length_mixed(
            subtitle_text, draw, subtitle_base, subtitle_cjk, subtitle_sym
        )
        subtitle_h = max(
            font_line_height(subtitle_base),
            font_line_height(subtitle_cjk),
            font_line_height(subtitle_sym),
        )
        gap = 16
        block_h = title_h + gap + subtitle_h
        start_y = plate_y + (plate_h - block_h) // 2
        title_x = plate_x + (plate_w - title_w) // 2
        title_y = start_y
        subtitle_x = plate_x + (plate_w - subtitle_w) // 2
        subtitle_y = start_y + title_h + gap
        draw_text_mixed(
            draw,
            (title_x, title_y),
            title_text,
            title_base,
            title_cjk,
            title_sym,
            (255, 255, 255, 255),
        )
        draw_text_mixed(
            draw,
            (subtitle_x, subtitle_y),
            subtitle_text,
            subtitle_base,
            subtitle_cjk,
            subtitle_sym,
            (210, 210, 210, 255),
        )
    else:
        title_x = plate_x + (plate_w - title_w) // 2
        title_y = plate_y + (plate_h - title_h) // 2
        draw_text_mixed(
            draw,
            (title_x, title_y),
            title_text,
            title_base,
            title_cjk,
            title_sym,
            (255, 255, 255, 255),
        )

    apply_corner_logo(base)

    output = BytesIO()
    base.convert("RGB").save(output, format="JPEG", quality=92)
    output.seek(0)
    return output


def build_referral_road_image(progress: int) -> BytesIO:
    width, height = MENU_IMAGE_WIDTH, MENU_IMAGE_HEIGHT
    base = None
    if LEADERBOARD_BG.exists():
        try:
            bg = Image.open(LEADERBOARD_BG).convert("RGB")
            base = ImageOps.fit(bg, (width, height), method=Image.LANCZOS)
            base = base.filter(ImageFilter.GaussianBlur(radius=18))
        except Exception:
            base = None
    if base is None:
        base = Image.new("RGB", (width, height), "#1a1a1a")
        draw = ImageDraw.Draw(base)
        for y in range(height):
            shade = 18 + int(80 * (y / height))
            draw.line([(0, y), (width, y)], fill=(shade, shade, shade))

    progress = max(0, min(int(progress), 15))
    base = base.convert("RGBA")
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    plate_w = int(width * 0.9)
    plate_h = int(height * 0.7)
    plate_x = (width - plate_w) // 2
    plate_y = (height - plate_h) // 2
    draw.rounded_rectangle(
        (plate_x, plate_y, plate_x + plate_w, plate_y + plate_h),
        radius=32,
        fill=(0, 0, 0, 175),
    )
    base.alpha_composite(overlay)

    draw = ImageDraw.Draw(base)
    title_font, title_cjk, title_sym = pick_font_bundle(44)
    title = "Дорога славы"
    title_w = text_length_mixed(title, draw, title_font, title_cjk, title_sym)
    title_y = plate_y + 22
    title_x = plate_x + (plate_w - title_w) // 2
    draw_text_mixed(
        draw,
        (title_x, title_y),
        title,
        title_font,
        title_cjk,
        title_sym,
        (255, 255, 255, 255),
    )

    line_y = plate_y + int(plate_h * 0.52)
    line_x1 = plate_x + int(plate_w * 0.06)
    line_x2 = plate_x + int(plate_w * 0.94)
    draw.line((line_x1, line_y, line_x2, line_y), fill=(90, 90, 90, 220), width=6)
    if progress > 0:
        active_x = line_x1 + int((line_x2 - line_x1) * ((progress - 1) / 14))
        draw.line((line_x1, line_y, active_x, line_y), fill=(255, 209, 102, 230), width=6)

    reward_labels = [
        "Случайная сосиска",
        "3 фри крутки",
        "3 фри крутки",
        "3 фри крутки",
        "30 фри круток",
        "3 фри крутки",
        "3 фри крутки",
        "3 фри крутки",
        "3 фри крутки",
        "10 звёзд",
        "3 фри крутки",
        "3 фри крутки",
        "3 фри крутки",
        "3 фри крутки",
        "VIP 30 дней",
    ]
    major_steps = {1, 5, 10, 15}
    label_font, label_cjk, label_sym = pick_font_bundle(15)
    step_font, step_cjk, step_sym = pick_font_bundle(16)
    label_height = max(
        font_line_height(label_font),
        font_line_height(label_cjk),
        font_line_height(label_sym),
    )
    label_gap = 2
    segment_width = (line_x2 - line_x1) / 14 if line_x2 > line_x1 else 1
    label_max_width = max(60, int(segment_width * 0.92))
    for step in range(1, 16):
        ratio = (step - 1) / 14
        x = int(line_x1 + (line_x2 - line_x1) * ratio)
        active = step <= progress
        is_major = step in major_steps
        radius = 12 if is_major else 9
        color = (255, 209, 102, 230) if active else (120, 120, 120, 200)
        if is_major:
            glow_radius = radius + 6
            glow_color = (255, 209, 102, 120) if active else (120, 120, 120, 120)
            draw.ellipse(
                (
                    x - glow_radius,
                    line_y - glow_radius,
                    x + glow_radius,
                    line_y + glow_radius,
                ),
                fill=glow_color,
            )
        draw.ellipse(
            (x - radius, line_y - radius, x + radius, line_y + radius),
            fill=color,
        )
        if is_major:
            draw.ellipse(
                (x - radius, line_y - radius, x + radius, line_y + radius),
                outline=(255, 255, 255, 160),
                width=2,
            )

        step_text = str(step)
        step_w = text_length_mixed(step_text, draw, step_font, step_cjk, step_sym)
        draw_text_mixed(
            draw,
            (x - step_w // 2, line_y - 28),
            step_text,
            step_font,
            step_cjk,
            step_sym,
            (220, 220, 220, 230),
        )

        reward = reward_labels[step - 1]
        lines = wrap_text_mixed(
            reward, label_max_width, draw, label_font, label_cjk, label_sym
        )
        start_y = line_y + 16
        label_color = (255, 230, 180, 240) if is_major else (235, 235, 235, 240)
        for idx, line in enumerate(lines):
            line_w = text_length_mixed(line, draw, label_font, label_cjk, label_sym)
            draw_text_mixed(
                draw,
                (x - line_w // 2, start_y + idx * (label_height + label_gap)),
                line,
                label_font,
                label_cjk,
                label_sym,
                label_color,
            )

    apply_corner_logo(base)

    output = BytesIO()
    base.convert("RGB").save(output, format="JPEG", quality=92)
    output.seek(0)
    return output


def build_profile_image(
    display_name: str,
    rank: int,
    total_users: int,
    total_value: int,
    balance: int,
    stars: int,
    vip: bool,
    is_admin: bool,
    avatar_bytes: Optional[bytes],
) -> BytesIO:
    width, height = 900, 500
    if avatar_bytes:
        avatar = Image.open(BytesIO(avatar_bytes)).convert("RGB")
        base = ImageOps.fit(avatar, (width, height), method=Image.LANCZOS)
        base = base.filter(ImageFilter.BoxBlur(4))
    else:
        base = Image.new("RGB", (width, height), "#1b1b1b")
        draw = ImageDraw.Draw(base)
        for y in range(height):
            shade = 24 + int(60 * (y / height))
            draw.line([(0, y), (width, y)], fill=(shade, shade, shade))

    base = base.convert("RGBA")
    plate_w = int(width * 0.86)
    plate_h = int(height * 0.5)
    plate_x = (width - plate_w) // 2
    plate_y = (height - plate_h) // 2

    plate_region = base.crop((plate_x, plate_y, plate_x + plate_w, plate_y + plate_h))
    plate_region = plate_region.filter(ImageFilter.GaussianBlur(radius=30))
    plate_layer = plate_region.convert("RGBA")
    tint = Image.new("RGBA", (plate_w, plate_h), (255, 255, 255, 45))
    plate_layer = Image.alpha_composite(plate_layer, tint)
    highlight = Image.new("RGBA", (plate_w, plate_h), (255, 255, 255, 0))
    highlight_draw = ImageDraw.Draw(highlight)
    for y in range(plate_h):
        alpha = int(55 * (1 - y / max(1, plate_h)))
        highlight_draw.line([(0, y), (plate_w, y)], fill=(255, 255, 255, alpha))
    plate_layer = Image.alpha_composite(plate_layer, highlight)
    shadow = Image.new("RGBA", (plate_w, plate_h), (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow)
    for y in range(plate_h):
        alpha = int(45 * (y / max(1, plate_h)))
        shadow_draw.line([(0, y), (plate_w, y)], fill=(0, 0, 0, alpha))
    plate_layer = Image.alpha_composite(plate_layer, shadow)

    mask = Image.new("L", (plate_w, plate_h), 0)
    mask_draw = ImageDraw.Draw(mask)
    mask_draw.rounded_rectangle(
        (0, 0, plate_w, plate_h),
        radius=42,
        fill=255,
    )
    base.paste(plate_layer, (plate_x, plate_y), mask)

    edge_layer = Image.new("RGBA", base.size, (0, 0, 0, 0))
    edge_draw = ImageDraw.Draw(edge_layer)
    edge_draw.rounded_rectangle(
        (plate_x, plate_y, plate_x + plate_w, plate_y + plate_h),
        radius=42,
        outline=(255, 255, 255, 110),
        width=2,
    )
    edge_layer = edge_layer.filter(ImageFilter.GaussianBlur(radius=4))
    base = Image.alpha_composite(base, edge_layer)
    draw = ImageDraw.Draw(base)

    avatar_size = int(plate_h * 0.65)
    avatar_x = plate_x + 36
    avatar_y = plate_y + (plate_h - avatar_size) // 2
    if avatar_bytes:
        avatar_img = Image.open(BytesIO(avatar_bytes)).convert("RGB")
        avatar_img = ImageOps.fit(
            avatar_img, (avatar_size, avatar_size), method=Image.LANCZOS
        )
    else:
        avatar_img = Image.new("RGB", (avatar_size, avatar_size), "#2d2d2d")
    mask = Image.new("L", (avatar_size, avatar_size), 0)
    ImageDraw.Draw(mask).ellipse((0, 0, avatar_size, avatar_size), fill=255)
    base.paste(avatar_img, (avatar_x, avatar_y), mask)

    border = 6
    if is_admin:
        ring_layer = Image.new("RGBA", base.size, (0, 0, 0, 0))
        ring_draw = ImageDraw.Draw(ring_layer)
        ring_box = (
            avatar_x - border,
            avatar_y - border,
            avatar_x + avatar_size + border,
            avatar_y + avatar_size + border,
        )

        def draw_rainbow_ring(
            box: Tuple[int, int, int, int], width: int, alpha: int
        ) -> None:
            segments = 24
            step = 360 / segments
            for idx in range(segments):
                start = idx * step
                end = start + step + 1
                hue = idx / segments
                r, g, b = colorsys.hsv_to_rgb(hue, 0.85, 1.0)
                ring_draw.arc(
                    box,
                    start=start,
                    end=end,
                    fill=(int(r * 255), int(g * 255), int(b * 255), alpha),
                    width=width,
                )

        draw_rainbow_ring(ring_box, border + 4, 120)
        draw_rainbow_ring(ring_box, border, 255)
        base = Image.alpha_composite(base, ring_layer)
        draw = ImageDraw.Draw(base)
    else:
        border_color = (255, 215, 0, 255) if vip else (255, 255, 255, 230)
        draw.ellipse(
            (
                avatar_x - border // 2,
                avatar_y - border // 2,
                avatar_x + avatar_size + border // 2,
                avatar_y + avatar_size + border // 2,
            ),
            outline=border_color,
            width=border,
        )

    text_x = avatar_x + avatar_size + 36
    display_text = str(display_name or "")
    title_base, title_cjk, title_sym = pick_profile_font_bundle(PROFILE_TITLE_SIZE)
    info_base, info_cjk, info_sym = pick_profile_font_bundle(PROFILE_INFO_SIZE)
    max_name_width = plate_x + plate_w - 40 - text_x
    name_text = display_text
    name_height = max(
        font_line_height(title_base),
        font_line_height(title_cjk),
        font_line_height(title_sym),
    )
    info_height = max(
        font_line_height(info_base),
        font_line_height(info_cjk),
        font_line_height(info_sym),
    )
    line_gap = max(10, int(info_height * 0.4))
    balance_text = f"Баланс: {balance}р"
    rank_text = f"Место в топе: {rank}"
    info_lines = [balance_text, rank_text]
    total_text_h = (
        name_height
        + line_gap
        + len(info_lines) * info_height
        + line_gap * max(0, len(info_lines) - 1)
    )
    text_y = plate_y + max(0, (plate_h - total_text_h) // 2)

    shadow_layer = Image.new("RGBA", base.size, (0, 0, 0, 0))
    shadow_draw = ImageDraw.Draw(shadow_layer)
    shadow_color = (90, 90, 90, 170)
    shadow_offset = (2, 2)

    def queue_shadow(
        position: Tuple[int, int],
        text: str,
        font_base: ImageFont.FreeTypeFont,
        font_cjk: ImageFont.FreeTypeFont,
        font_symbol: ImageFont.FreeTypeFont,
    ) -> None:
        if not text:
            return
        shadow_pos = (position[0] + shadow_offset[0], position[1] + shadow_offset[1])
        draw_text_mixed(
            shadow_draw,
            shadow_pos,
            text,
            font_base,
            font_cjk,
            font_symbol,
            shadow_color,
        )

    name_color = (255, 255, 255, 255)
    admin_tag = "ADMIN" if is_admin else ""
    tag_base, tag_cjk, tag_sym = pick_profile_font_bundle(max(18, PROFILE_INFO_SIZE - 6))
    tag_height = max(
        font_line_height(tag_base),
        font_line_height(tag_cjk),
        font_line_height(tag_sym),
    )
    tag_gap = 12
    tag_width = (
        text_length_mixed(admin_tag, draw, tag_base, tag_cjk, tag_sym)
        if admin_tag
        else 0
    )
    name_width = text_length_mixed(name_text, draw, title_base, title_cjk, title_sym)
    name_target_width = int(max_name_width) if name_width > max_name_width else None
    if admin_tag and max_name_width > tag_gap + 10:
        name_target_width = int(max(80, max_name_width - tag_width - tag_gap))
    if name_text:
        name_shadow_layer = render_text_layer_mixed(
            name_text, title_base, title_cjk, title_sym, shadow_color
        )
        if name_target_width and name_shadow_layer.width > name_target_width:
            name_shadow_layer = name_shadow_layer.resize(
                (name_target_width, name_shadow_layer.height), resample=Image.LANCZOS
            )
        shadow_layer.alpha_composite(
            name_shadow_layer,
            (text_x + shadow_offset[0], text_y + shadow_offset[1]),
        )
    for idx, info_line in enumerate(info_lines):
        info_y = text_y + name_height + line_gap + idx * (info_height + line_gap)
        queue_shadow(
            (text_x, info_y),
            info_line,
            info_base,
            info_cjk,
            info_sym,
        )
    shadow_layer = shadow_layer.filter(ImageFilter.GaussianBlur(radius=2))
    base = Image.alpha_composite(base, shadow_layer)
    draw = ImageDraw.Draw(base)
    name_render_width = 0
    if name_text:
        name_layer = render_text_layer_mixed(
            name_text, title_base, title_cjk, title_sym, name_color
        )
        if name_target_width and name_layer.width > name_target_width:
            name_layer = name_layer.resize(
                (name_target_width, name_layer.height), resample=Image.LANCZOS
            )
        name_render_width = name_layer.width
        if is_admin:
            glow_layer = Image.new("RGBA", base.size, (0, 0, 0, 0))
            glow_colors = [
                (255, 90, 180, 150),
                (90, 220, 255, 150),
                (255, 210, 90, 150),
            ]
            offsets = [(-2, 0), (2, 0), (0, -2)]
            for (dx, dy), color in zip(offsets, glow_colors):
                glow_text = render_text_layer_mixed(
                    name_text, title_base, title_cjk, title_sym, color
                )
                if name_target_width and glow_text.width > name_target_width:
                    glow_text = glow_text.resize(
                        (name_target_width, glow_text.height), resample=Image.LANCZOS
                    )
                glow_layer.alpha_composite(glow_text, (text_x + dx, text_y + dy))
            glow_layer = glow_layer.filter(ImageFilter.GaussianBlur(radius=2))
            base = Image.alpha_composite(base, glow_layer)
            draw = ImageDraw.Draw(base)
        base.alpha_composite(name_layer, (text_x, text_y))
    if admin_tag:
        tag_layer = render_text_layer_mixed(
            admin_tag, tag_base, tag_cjk, tag_sym, (255, 215, 120, 255)
        )
        tag_y = text_y + max(0, (name_height - tag_height) // 2)
        tag_x = text_x + name_render_width + tag_gap
        base.alpha_composite(tag_layer, (tag_x, tag_y))
    for idx, info_line in enumerate(info_lines):
        info_y = text_y + name_height + line_gap + idx * (info_height + line_gap)
        draw_text_mixed(
            draw,
            (text_x, info_y),
            info_line,
            info_base,
            info_cjk,
            info_sym,
            (220, 220, 220, 255),
        )

    apply_corner_logo(base)

    output = BytesIO()
    base.convert("RGB").save(output, format="JPEG", quality=92)
    output.seek(0)
    return output


def build_leaderboard_image(
    left_entries: List[Tuple[str, int, Optional[bytes], bool, bool]],
    right_entries: List[Tuple[str, int, Optional[bytes], bool, bool]],
    total_users: int,
) -> BytesIO:
    width = 900
    title_base, title_cjk, title_sym = pick_font_bundle(LEADERBOARD_TITLE_SIZE)
    entry_base, entry_cjk, entry_sym = pick_font_bundle(LEADERBOARD_ENTRY_SIZE)
    tag_base, tag_cjk, tag_sym = pick_font_bundle(max(12, LEADERBOARD_ENTRY_SIZE - 6))
    row_gap = LEADERBOARD_ROW_GAP
    outer_margin = LEADERBOARD_OUTER_MARGIN
    plate_padding = LEADERBOARD_PLATE_PADDING
    header_rows_gap = LEADERBOARD_HEADER_TO_ROWS_GAP
    title_left = "Топ игроков"
    title_right = "Топ донатеров"
    title_draw = ImageDraw.Draw(Image.new("RGB", (1, 1)))
    title_h = max(
        font_line_height(title_base),
        font_line_height(title_cjk),
        font_line_height(title_sym),
    )
    row_height = max(LEADERBOARD_AVATAR_SIZE, entry_base.size + 10)
    players_entries = list(left_entries[:5])
    donors_entries = list(right_entries[:3])
    players_rows = max(len(players_entries), 1)
    donors_rows = max(len(donors_entries), 1)
    body_h_players = row_height * players_rows + row_gap * max(0, players_rows - 1)
    body_h_donors = row_height * donors_rows + row_gap * max(0, donors_rows - 1)
    header_h = title_h
    section_pad = plate_padding // 2
    section_gap = 22
    plate_w = width - 2 * outer_margin
    players_h = header_h + header_rows_gap + body_h_players + section_pad * 2
    donors_h = header_h + header_rows_gap + body_h_donors + section_pad * 2
    total_h = outer_margin * 2 + players_h + section_gap + donors_h

    base = None
    if LEADERBOARD_BG.exists():
        try:
            bg = Image.open(LEADERBOARD_BG).convert("RGB")
            base = ImageOps.fit(bg, (width, total_h), method=Image.LANCZOS)
            base = base.filter(ImageFilter.GaussianBlur(radius=14))
        except Exception:
            base = None
    if base is None:
        base = Image.new("RGB", (width, total_h), "#1b1b1b")
        draw = ImageDraw.Draw(base)
        for y in range(total_h):
            shade = 20 + int(70 * (y / total_h))
            draw.line([(0, y), (width, y)], fill=(shade, shade, shade))

    base = base.convert("RGBA")
    overlay = Image.new("RGBA", (width, total_h), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    plate_x = outer_margin
    plate_y = outer_margin
    draw.rounded_rectangle(
        (plate_x, plate_y, plate_x + plate_w, plate_y + players_h),
        radius=32,
        fill=(0, 0, 0, 170),
    )
    donors_y = plate_y + players_h + section_gap
    draw.rounded_rectangle(
        (plate_x, donors_y, plate_x + plate_w, donors_y + donors_h),
        radius=32,
        fill=(0, 0, 0, 170),
    )
    base.alpha_composite(overlay)

    draw = ImageDraw.Draw(base)

    def draw_section(
        section_x: int,
        section_y: int,
        section_w: int,
        title: str,
        entries: List[Tuple[str, int, Optional[bytes], bool, bool]],
        value_suffix: str,
    ) -> None:
        title_w = text_length_mixed(title, title_draw, title_base, title_cjk, title_sym)
        title_x = section_x + (section_w - title_w) // 2
        title_y = section_y + section_pad
        draw_text_mixed(
            draw,
            (title_x, title_y),
            title,
            title_base,
            title_cjk,
            title_sym,
            (255, 255, 255, 255),
        )
        current_y = title_y + header_h + header_rows_gap
        for index, (name, total, avatar_bytes, vip, is_admin) in enumerate(
            entries, start=1
        ):
            row_y = current_y
            avatar = None
            if avatar_bytes:
                try:
                    avatar = Image.open(BytesIO(avatar_bytes)).convert("RGB")
                except Exception:
                    avatar = None
            if avatar is None:
                avatar = Image.new("RGB", (LEADERBOARD_AVATAR_SIZE,) * 2, "#2d2d2d")
            avatar = ImageOps.fit(
                avatar,
                (LEADERBOARD_AVATAR_SIZE, LEADERBOARD_AVATAR_SIZE),
                method=Image.LANCZOS,
            )
            mask = Image.new("L", (LEADERBOARD_AVATAR_SIZE,) * 2, 0)
            ImageDraw.Draw(mask).ellipse(
                (0, 0, LEADERBOARD_AVATAR_SIZE, LEADERBOARD_AVATAR_SIZE), fill=255
            )
            avatar_x = section_x + 18
            base.paste(avatar, (avatar_x, row_y), mask)
            if is_admin:
                border = 4
                ring_box = (
                    avatar_x - border,
                    row_y - border,
                    avatar_x + LEADERBOARD_AVATAR_SIZE + border,
                    row_y + LEADERBOARD_AVATAR_SIZE + border,
                )
                segments = 20
                step = 360 / segments
                for idx in range(segments):
                    start = idx * step
                    end = start + step + 1
                    hue = idx / segments
                    r, g, b = colorsys.hsv_to_rgb(hue, 0.85, 1.0)
                    draw.arc(
                        ring_box,
                        start=start,
                        end=end,
                        fill=(int(r * 255), int(g * 255), int(b * 255), 255),
                        width=border,
                    )
            elif vip:
                border = 4
                draw.ellipse(
                    (
                        avatar_x - border // 2,
                        row_y - border // 2,
                        avatar_x + LEADERBOARD_AVATAR_SIZE + border // 2,
                        row_y + LEADERBOARD_AVATAR_SIZE + border // 2,
                    ),
                    outline=(255, 215, 0, 255),
                    width=border,
                )
            text_x = avatar_x + LEADERBOARD_AVATAR_SIZE + 18
            text_y = row_y + (LEADERBOARD_AVATAR_SIZE - entry_base.size) // 2
            value_text = f"{total}{value_suffix}"
            value_w = text_length_mixed(
                value_text, draw, entry_base, entry_cjk, entry_sym
            )
            tag_text = "ADMIN" if is_admin else ""
            tag_w = (
                text_length_mixed(tag_text, draw, tag_base, tag_cjk, tag_sym)
                if tag_text
                else 0
            )
            tag_gap = 10 if tag_text else 0
            name_max_width = (
                section_w - (text_x - section_x) - value_w - 18 - tag_w - tag_gap
            )
            label_text = fit_text_to_width_mixed(
                f"{index}. {name}",
                max(70, name_max_width),
                draw,
                entry_base,
                entry_cjk,
                entry_sym,
            )
            name_color = (
                (255, 215, 0, 255)
                if vip and not is_admin
                else (255, 255, 255, 255)
            )
            draw_text_mixed(
                draw,
                (text_x, text_y),
                label_text,
                entry_base,
                entry_cjk,
                entry_sym,
                name_color,
            )
            if tag_text:
                label_w = text_length_mixed(
                    label_text, draw, entry_base, entry_cjk, entry_sym
                )
                tag_y = text_y + max(0, (entry_base.size - tag_base.size) // 2)
                draw_text_mixed(
                    draw,
                    (text_x + label_w + tag_gap, tag_y),
                    tag_text,
                    tag_base,
                    tag_cjk,
                    tag_sym,
                    (255, 215, 120, 255),
                )
            value_x = section_x + section_w - 18 - value_w
            draw_text_mixed(
                draw,
                (value_x, text_y),
                value_text,
                entry_base,
                entry_cjk,
                entry_sym,
                (210, 210, 210, 255),
            )
            current_y += row_height + row_gap

    draw_section(plate_x, plate_y, plate_w, title_left, players_entries, "р")
    draw_section(plate_x, donors_y, plate_w, title_right, donors_entries, "⭐")

    apply_corner_logo(base)

    output = BytesIO()
    base.convert("RGB").save(output, format="JPEG", quality=92)
    output.seek(0)
    return output


def build_kazik_title_image(title: str, subtitle: Optional[str] = None) -> BytesIO:
    width, height = KAZIK_IMAGE_WIDTH, KAZIK_IMAGE_HEIGHT
    base = None
    if LEADERBOARD_BG.exists():
        try:
            bg = Image.open(LEADERBOARD_BG).convert("RGB")
            base = ImageOps.fit(bg, (width, height), method=Image.LANCZOS)
            base = base.filter(ImageFilter.GaussianBlur(radius=14))
        except Exception:
            base = None
    if base is None:
        base = Image.new("RGB", (width, height), "#1b1b1b")
        draw = ImageDraw.Draw(base)
        for y in range(height):
            shade = 20 + int(70 * (y / height))
            draw.line([(0, y), (width, y)], fill=(shade, shade, shade))

    base = base.convert("RGBA")
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    plate_w = int(width * 0.86)
    plate_h = int(height * 0.65)
    plate_x = (width - plate_w) // 2
    plate_y = (height - plate_h) // 2
    draw.rounded_rectangle(
        (plate_x, plate_y, plate_x + plate_w, plate_y + plate_h),
        radius=32,
        fill=(0, 0, 0, 170),
    )
    base.alpha_composite(overlay)

    draw = ImageDraw.Draw(base)
    title_base, title_cjk, title_sym = pick_font_bundle(KAZIK_TITLE_SIZE)
    subtitle_base, subtitle_cjk, subtitle_sym = pick_font_bundle(KAZIK_SUBTITLE_SIZE)
    max_text_width = int(plate_w * 0.9)
    title_text = fit_text_to_width_mixed(
        title, max_text_width, draw, title_base, title_cjk, title_sym
    )
    subtitle_text = (
        fit_text_to_width_mixed(
            subtitle, max_text_width, draw, subtitle_base, subtitle_cjk, subtitle_sym
        )
        if subtitle
        else None
    )

    title_w = text_length_mixed(title_text, draw, title_base, title_cjk, title_sym)
    title_h = max(
        font_line_height(title_base),
        font_line_height(title_cjk),
        font_line_height(title_sym),
    )
    if subtitle_text:
        subtitle_w = text_length_mixed(
            subtitle_text, draw, subtitle_base, subtitle_cjk, subtitle_sym
        )
        subtitle_h = max(
            font_line_height(subtitle_base),
            font_line_height(subtitle_cjk),
            font_line_height(subtitle_sym),
        )
        gap = 16
        block_h = title_h + gap + subtitle_h
        start_y = plate_y + (plate_h - block_h) // 2
        title_x = plate_x + (plate_w - title_w) // 2
        title_y = start_y
        subtitle_x = plate_x + (plate_w - subtitle_w) // 2
        subtitle_y = start_y + title_h + gap
        draw_text_mixed(
            draw,
            (title_x, title_y),
            title_text,
            title_base,
            title_cjk,
            title_sym,
            (255, 255, 255, 255),
        )
        draw_text_mixed(
            draw,
            (subtitle_x, subtitle_y),
            subtitle_text,
            subtitle_base,
            subtitle_cjk,
            subtitle_sym,
            (210, 210, 210, 255),
        )
    else:
        title_x = plate_x + (plate_w - title_w) // 2
        title_y = plate_y + (plate_h - title_h) // 2
        draw_text_mixed(
            draw,
            (title_x, title_y),
            title_text,
            title_base,
            title_cjk,
            title_sym,
            (255, 255, 255, 255),
        )

    apply_corner_logo(base)

    output = BytesIO()
    base.convert("RGB").save(output, format="JPEG", quality=92)
    output.seek(0)
    return output


def build_kazik_spin_image(
    digits: List[int],
    revealed: int,
    title: Optional[str] = None,
) -> BytesIO:
    width, height = KAZIK_IMAGE_WIDTH, KAZIK_IMAGE_HEIGHT
    base = None
    if LEADERBOARD_BG.exists():
        try:
            bg = Image.open(LEADERBOARD_BG).convert("RGB")
            base = ImageOps.fit(bg, (width, height), method=Image.LANCZOS)
            base = base.filter(ImageFilter.GaussianBlur(radius=14))
        except Exception:
            base = None
    if base is None:
        base = Image.new("RGB", (width, height), "#1b1b1b")
        draw = ImageDraw.Draw(base)
        for y in range(height):
            shade = 20 + int(70 * (y / height))
            draw.line([(0, y), (width, y)], fill=(shade, shade, shade))

    base = base.convert("RGBA")
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw = ImageDraw.Draw(overlay)
    plate_w = int(width * 0.86)
    plate_h = int(height * 0.65)
    plate_x = (width - plate_w) // 2
    plate_y = (height - plate_h) // 2
    draw.rounded_rectangle(
        (plate_x, plate_y, plate_x + plate_w, plate_y + plate_h),
        radius=32,
        fill=(0, 0, 0, 170),
    )
    base.alpha_composite(overlay)

    draw = ImageDraw.Draw(base)
    if title and revealed <= 0:
        title_base, title_cjk, title_sym = pick_font_bundle(KAZIK_TITLE_SIZE)
        max_text_width = int(plate_w * 0.9)
        title_text = fit_text_to_width_mixed(
            title, max_text_width, draw, title_base, title_cjk, title_sym
        )
        title_w = text_length_mixed(
            title_text, draw, title_base, title_cjk, title_sym
        )
        title_h = max(
            font_line_height(title_base),
            font_line_height(title_cjk),
            font_line_height(title_sym),
        )
        title_x = plate_x + (plate_w - title_w) // 2
        title_y = plate_y + (plate_h - title_h) // 2
        draw_text_mixed(
            draw,
            (title_x, title_y),
            title_text,
            title_base,
            title_cjk,
            title_sym,
            (255, 255, 255, 255),
        )
        apply_corner_logo(base)
        output = BytesIO()
        base.convert("RGB").save(output, format="JPEG", quality=92)
        output.seek(0)
        return output

    title_offset = 0
    if title:
        title_base, title_cjk, title_sym = pick_font_bundle(KAZIK_SUBTITLE_SIZE)
        max_text_width = int(plate_w * 0.9)
        title_text = fit_text_to_width_mixed(
            title, max_text_width, draw, title_base, title_cjk, title_sym
        )
        title_w = text_length_mixed(
            title_text, draw, title_base, title_cjk, title_sym
        )
        title_h = max(
            font_line_height(title_base),
            font_line_height(title_cjk),
            font_line_height(title_sym),
        )
        title_x = plate_x + (plate_w - title_w) // 2
        title_y = plate_y + 18
        draw_text_mixed(
            draw,
            (title_x, title_y),
            title_text,
            title_base,
            title_cjk,
            title_sym,
            (220, 220, 220, 255),
        )
        title_offset = int(title_h * 0.4) + 12
    slot_w = int((plate_w - 2 * KAZIK_SLOT_GAP) / 3)
    slot_h = int(plate_h * 0.6)
    slot_y = plate_y + (plate_h - slot_h) // 2 + title_offset
    digit_font = pick_font(KAZIK_DIGIT_SIZE)

    for index in range(3):
        slot_x = plate_x + index * (slot_w + KAZIK_SLOT_GAP)
        draw.rounded_rectangle(
            (slot_x, slot_y, slot_x + slot_w, slot_y + slot_h),
            radius=KAZIK_SLOT_RADIUS,
            fill=(15, 15, 15, 210),
        )
        digit_layer = Image.new("RGBA", (slot_w, slot_h), (0, 0, 0, 0))
        digit_draw = ImageDraw.Draw(digit_layer)
        digit_value = digits[index] if index < revealed else random.choice(KAZIK_DIGITS)
        digit_text = str(digit_value)
        text_box = digit_draw.textbbox((0, 0), digit_text, font=digit_font)
        text_w = text_box[2] - text_box[0]
        text_h = text_box[3] - text_box[1]
        text_x = (slot_w - text_w) // 2 - text_box[0]
        text_y = (slot_h - text_h) // 2 - text_box[1]
        digit_draw.text(
            (text_x, text_y),
            digit_text,
            font=digit_font,
            fill=(255, 255, 255, 230),
        )
        if index >= revealed:
            digit_layer = digit_layer.filter(ImageFilter.GaussianBlur(radius=6))
        base.alpha_composite(digit_layer, (slot_x, slot_y))

    apply_corner_logo(base)

    output = BytesIO()
    base.convert("RGB").save(output, format="JPEG", quality=92)
    output.seek(0)
    return output


def ensure_photo_cache_dir() -> None:
    PHOTO_CACHE_DIR.mkdir(parents=True, exist_ok=True)


def ensure_cached_image(path: Path, builder: Callable[[], BytesIO]) -> Path:
    if not path.exists() or path.stat().st_size == 0:
        ensure_photo_cache_dir()
        image = builder()
        path.write_bytes(image.getvalue())
    return path


def get_cached_menu_image(key: str, title: str, subtitle: Optional[str]) -> Path:
    cache_version = IMAGE_CACHE_VERSION
    filename = f"menu_{key}_{cache_version}.jpg"
    path = PHOTO_CACHE_DIR / filename
    return ensure_cached_image(path, lambda: build_menu_image(title, subtitle))


def get_cached_referral_road_image(progress: int) -> Path:
    cache_version = IMAGE_CACHE_VERSION
    safe_progress = max(0, min(int(progress), 15))
    filename = f"ref_road_{safe_progress}_{cache_version}.jpg"
    path = PHOTO_CACHE_DIR / filename
    return ensure_cached_image(path, lambda: build_referral_road_image(safe_progress))


def get_cached_kazik_title_image() -> Path:
    cache_version = IMAGE_CACHE_VERSION
    path = PHOTO_CACHE_DIR / f"kazik_title_{cache_version}.jpg"
    return ensure_cached_image(path, lambda: build_kazik_title_image("Казик"))


def get_cached_kazik_result_image(win: bool, digits: List[int]) -> Path:
    digits_slug = "-".join(str(digit) for digit in digits)
    suffix = "win" if win else "lose"
    title = "Выигрыш!" if win else "Проигрыш"
    subtitle = f"Выпало: {build_kazik_text_line(digits, 3)}"
    cache_version = IMAGE_CACHE_VERSION
    filename = f"kazik_{suffix}_{digits_slug}_{cache_version}.jpg"
    path = PHOTO_CACHE_DIR / filename
    return ensure_cached_image(path, lambda: build_kazik_title_image(title, subtitle))
