# jp_font_setup.py
from matplotlib import font_manager, rcParams

def setup_japanese_font():
    """
    Set a Japanese-capable font for matplotlib.
    """
    candidates = [
        "Hiragino Sans",
        "Hiragino Kaku Gothic ProN",
        "Yu Gothic",
        "YuGothic",
        "IPAexGothic",
        "IPA Gothic",
        "Noto Sans CJK JP",
        "TakaoGothic",
    ]

    available = {f.name for f in font_manager.fontManager.ttflist}

    for name in candidates:
        if name in available:
            print(f"✅ Using Japanese font: {name}")
            # The important part: override *sans-serif* stack as well
            rcParams["font.family"] = "sans-serif"
            rcParams["font.sans-serif"] = [name]
            rcParams["axes.unicode_minus"] = False
            return

    print("⚠️ No Japanese font from candidates found.")
    print("   Install one (e.g. 'Noto Sans CJK JP' or 'IPAexGothic') and rerun.")
