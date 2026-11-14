#!/usr/bin/env python3
"""Regenerate packaged icons from logo.svg (pip install resvg-py Pillow).

Derived assets are checked in so normal builds need no image conversion tools.
"""
import io
from pathlib import Path

from PIL import Image
import resvg_py

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "crates/sinomo-app/assets"
ANDROID = ROOT / "crates/sinomo-app-android/res"


def render(size):
    return resvg_py.svg_to_bytes(
        svg_path=str(ASSETS / "logo.svg"), width=size, height=size,
        skip_system_fonts=True,
    )


def main():
    png = render(256)
    (ASSETS / "logo.png").write_bytes(png)
    Image.open(io.BytesIO(png)).save(
        ASSETS / "logo.ico", sizes=[(s, s) for s in (16, 24, 32, 48, 64, 128, 256)]
    )
    for density, size in (("mdpi", 48), ("hdpi", 72), ("xhdpi", 96),
                          ("xxhdpi", 144), ("xxxhdpi", 192)):
        directory = ANDROID / f"mipmap-{density}"
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "ic_launcher.png").write_bytes(render(size))
        # Keep the complete mark inside the safe area of adaptive launcher masks.
        canvas = Image.new("RGBA", (size * 108 // 48,) * 2)
        foreground = Image.open(io.BytesIO(render(size * 60 // 48)))
        offset = (canvas.width - foreground.width) // 2
        canvas.paste(foreground, (offset, offset))
        canvas.save(directory / "ic_launcher_foreground.png")


if __name__ == "__main__":
    main()
