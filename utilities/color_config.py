"""
ColorConfig.py
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

class ColorConfig:
    """
    A utility class for managing and using terminal color codes.
    """

    # Reset color formatting
    RESET = "\033[0m"

    # Text Styles
    TEXT_STYLES = {
        "bold": "\033[1m",
        "italic": "\033[3m",
        "underline": "\033[4m",
        "blink": "\033[5m",
        "reverse": "\033[7m",
        "hidden": "\033[8m"
    }

    # Standard Colors
    STANDARD_COLORS = {
        "black": "\033[0;30m",
        "red": "\033[0;31m",
        "green": "\033[0;32m",
        "yellow": "\033[0;33m",
        "blue": "\033[0;34m",
        "purple": "\033[0;35m",
        "cyan": "\033[0;36m",
        "white": "\033[0;37m"
    }

    # Background Colors
    BACKGROUND_COLORS = {
        "black_bg": "\033[40m",
        "red_bg": "\033[41m",
        "green_bg": "\033[42m",
        "yellow_bg": "\033[43m",
        "blue_bg": "\033[44m",
        "purple_bg": "\033[45m",
        "cyan_bg": "\033[46m",
        "white_bg": "\033[47m"
    }

    # Extended Colors
    EXTENDED_COLORS = {
        "dark_red": "\033[38;2;139;0;0m",
        "crimson": "\033[38;2;220;20;60m",
        "dark_green": "\033[38;2;0;100;0m",
        "olive": "\033[38;2;128;128;0m",
        "navy": "\033[38;2;0;0;128m",
        "teal": "\033[38;2;0;128;128m",
        "silver": "\033[38;2;192;192;192m",
        "maroon": "\033[38;2;128;0;0m",
        "lime": "\033[38;2;0;255;0m",
        "aqua": "\033[38;2;0;255;255m",
        "fuchsia": "\033[38;2;255;0;255m",
        "gray": "\033[38;2;128;128;128m",
    }

    # Custom Colors
    CUSTOM_COLORS = {
        "vibrant_red": "\033[38;2;176;29;45m",
        "soft_orange": "\033[38;2;226;76;44m",
        "deep_blue": "\033[38;2;3;84;146m",
        "forest_green": "\033[38;2;0;123;51m",
        "sky_blue": "\033[38;2;1;162;217m",
        "bright_pink": "\033[38;2;255;20;147m",
        "golden_yellow": "\033[38;2;255;223;0m",
        "cool_gray": "\033[38;2;119;136;153m"
    }

    # Color Themes with Shades
    COLOR_THEMES = {
        "black": {
            "shade_1": "\033[38;2;20;20;20m",
            "shade_2": "\033[38;2;40;40;40m",
            "shade_3": "\033[38;2;60;60;60m",
            "shade_4": "\033[38;2;80;80;80m",
            "shade_5": "\033[38;2;100;100;100m",
            "shade_6": "\033[38;2;120;120;120m"
        },
        "red": {
            "shade_1": "\033[38;2;139;0;0m",
            "shade_2": "\033[38;2;165;42;42m",
            "shade_3": "\033[38;2;178;34;34m",
            "shade_4": "\033[38;2;205;92;92m",
            "shade_5": "\033[38;2;220;20;60m",
            "shade_6": "\033[38;2;255;0;0m"
        },
        "blue": {
            "shade_1": "\033[38;2;0;0;139m",
            "shade_2": "\033[38;2;0;0;205m",
            "shade_3": "\033[38;2;65;105;225m",
            "shade_4": "\033[38;2;100;149;237m",
            "shade_5": "\033[38;2;135;206;235m",
            "shade_6": "\033[38;2;173;216;230m"
        }
        # Add more themes as needed
    }

    @classmethod
    def apply_color(cls, text, color_code, bg_color_code=None, text_style=None):
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
            return f"{style}{color_code}{bg_color_code}{text}{cls.RESET}"
        return f"{style}{color_code}{text}{cls.RESET}"

    @classmethod
    def list_colors(cls):
        """
        Lists all available color categories, text styles, and names.
        """
        print("Text Styles:")
        for name in cls.TEXT_STYLES.keys():
            print(f"- {name}")

        print("\nStandard Colors:")
        for name in cls.STANDARD_COLORS.keys():
            print(f"- {name}")

        print("\nBackground Colors:")
        for name in cls.BACKGROUND_COLORS.keys():
            print(f"- {name}")

        print("\nExtended Colors:")
        for name in cls.EXTENDED_COLORS.keys():
            print(f"- {name}")

        print("\nCustom Colors:")
        for name in cls.CUSTOM_COLORS.keys():
            print(f"- {name}")

    @classmethod
    def get_color(cls, category, name):
        """
        Retrieves a color code from the specified category and name.

        :param category: The category (e.g., 'STANDARD_COLORS').
        :param name: The name of the color in that category.
        :return: The ANSI color code if found, else None.
        """
        return getattr(cls, category, {}).get(name, None)


# Example Usage
if __name__ == "__main__":
    print(ColorConfig.apply_color("This is a bold red text", ColorConfig.STANDARD_COLORS["red"], text_style=ColorConfig.TEXT_STYLES["bold"]))
    print(ColorConfig.apply_color("This is a vibrant red text", ColorConfig.CUSTOM_COLORS["vibrant_red"]))
    print(ColorConfig.apply_color("This is red text on yellow background", ColorConfig.STANDARD_COLORS["red"], ColorConfig.BACKGROUND_COLORS["yellow_bg"]))
    print(ColorConfig.apply_color("This is italic text", ColorConfig.STANDARD_COLORS["green"], text_style=ColorConfig.TEXT_STYLES["italic"]))
    ColorConfig.list_colors()