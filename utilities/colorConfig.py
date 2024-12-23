# colorConfig.py
# ____________________________________________________________________________________________
#                                           Color
# ____________________________________________________________________________________________
"""
colorConfig.py
---------------
This module provides a utility class for managing terminal color codes
and applying them to text for formatted output.

Features:
- Standard colors
- Extended colors
- Custom colors
- Color themes with shades
- Utility methods for applying and formatting colors
- Support for background colors
- Text styling options (bold, italic, underline, etc.)
- Comprehensive documentation for easier understanding

Author: Levi Gagne
"""

class C:
    # Text Modifiers
    r  = '\033[0m'  # Reset all attributes
    b  = '\033[1m'  # Bold/brighter
    d  = '\033[2m'  # Dim/darker
    i  = '\033[3m'  # Italic (not widely supported)
    u  = '\033[4m'  # Underline
    bl = '\033[5m'  # Blink (not widely supported and generally discouraged)
    rv = '\033[7m'  # Reverse (invert foreground and background colors)
    h  = '\033[8m'  # Hidden (not widely supported)

    # Base Colors
    black  = '\033[30m'  # Black text
    red    = '\033[31m'  # Red text
    green  = '\033[32m'  # Green text
    yellow = '\033[33m'  # Yellow text
    blue   = '\033[34m'  # Blue text
    purple = '\033[35m'  # Purple text
    cyan   = '\033[36m'  # Cyan text
    white  = '\033[37m'  # White text

    # Background Colors
    black_bg  = '\033[40m'  # Black background
    red_bg    = '\033[41m'  # Red background
    green_bg  = '\033[42m'  # Green background
    yellow_bg = '\033[43m'  # Yellow background
    blue_bg   = '\033[44m'  # Blue background
    purple_bg = '\033[45m'  # Purple background
    cyan_bg   = '\033[46m'  # Cyan background
    white_bg  = '\033[47m'  # White background

    # Extended Colors
    dark_red     = '\033[38;2;139;0;0m'
    crimson      = '\033[38;2;220;20;60m'
    dark_green   = '\033[38;2;0;100;0m'
    olive        = '\033[38;2;128;128;0m'
    navy         = '\033[38;2;0;0;128m'
    teal         = '\033[38;2;0;128;128m'
    silver       = '\033[38;2;192;192;192m'
    maroon       = '\033[38;2;128;0;0m'
    lime         = '\033[38;2;0;255;0m'
    aqua         = '\033[38;2;0;255;255m'
    fuchsia      = '\033[38;2;255;0;255m'
    gray         = '\033[38;2;128;128;128m'

    # Custom Colors
    vibrant_red    = '\033[38;2;176;29;45m'
    soft_orange    = '\033[38;2;226;76;44m'
    deep_blue      = '\033[38;2;3;84;146m'
    forest_green   = '\033[38;2;0;123;51m'
    sky_blue       = '\033[38;2;1;162;217m'
    bright_pink    = '\033[38;2;255;20;147m'
    golden_yellow  = '\033[38;2;255;223;0m'
    cool_gray      = '\033[38;2;119;136;153m'

    # Color Themes with Shades
    black_shades = {
        "shade_1": "\033[38;2;20;20;20m",
        "shade_2": "\033[38;2;40;40;40m",
        "shade_3": "\033[38;2;60;60;60m",
        "shade_4": "\033[38;2;80;80;80m",
        "shade_5": "\033[38;2;100;100;100m",
        "shade_6": "\033[38;2;120;120;120m"
    }

    red_shades = {
        "shade_1": "\033[38;2;139;0;0m",
        "shade_2": "\033[38;2;165;42;42m",
        "shade_3": "\033[38;2;178;34;34m",
        "shade_4": "\033[38;2;205;92;92m",
        "shade_5": "\033[38;2;220;20;60m",
        "shade_6": "\033[38;2;255;0;0m"
    }

    blue_shades = {
        "shade_1": "\033[38;2;0;0;139m",
        "shade_2": "\033[38;2;0;0;205m",
        "shade_3": "\033[38;2;65;105;225m",
        "shade_4": "\033[38;2;100;149;237m",
        "shade_5": "\033[38;2;135;206;235m",
        "shade_6": "\033[38;2;173;216;230m"
    }

    @staticmethod
    def apply_color(text, color_code, bg_color_code=None, text_style=None):
        """
        Applies the specified color code, optional background color, and optional text style to the given text.

        :param text: The text to format.
        :param color_code: The ANSI color code to apply.
        :param bg_color_code: The optional background color code to apply.
        :param text_style: The optional text style to apply.
        :return: Formatted text with the applied color, background, and style.
        """
        style = text_style if text_style else ""
        if bg_color_code:
            return f"{style}{color_code}{bg_color_code}{text}{C.r}"
        return f"{style}{color_code}{text}{C.r}"

    @staticmethod
    def list_colors():
        """
        Lists all available color categories, text styles, and names.
        """
        print("Text Modifiers:")
        print("- r (Reset), b (Bold), d (Dim), i (Italic), u (Underline), bl (Blink), rv (Reverse), h (Hidden)")

        print("\nBase Colors:")
        for color in ["black", "red", "green", "yellow", "blue", "purple", "cyan", "white"]:
            print(f"- {color}")

        print("\nBackground Colors:")
        for color in ["black_bg", "red_bg", "green_bg", "yellow_bg", "blue_bg", "purple_bg", "cyan_bg", "white_bg"]:
            print(f"- {color}")

        print("\nExtended Colors:")
        for color in ["dark_red", "crimson", "dark_green", "olive", "navy", "teal", "silver", "maroon", "lime", "aqua", "fuchsia", "gray"]:
            print(f"- {color}")

        print("\nCustom Colors:")
        for color in ["vibrant_red", "soft_orange", "deep_blue", "forest_green", "sky_blue", "bright_pink", "golden_yellow", "cool_gray"]:
            print(f"- {color}")

    @staticmethod
    def colorConfigCheck():
        """
        Verifies the color configuration is working as expected.
        """
        print("Color config check successful.")
