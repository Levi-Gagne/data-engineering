"""
messageConfig.py

This module provides a set of utility methods for standardized and reusable messages.
It integrates with colorConfig to apply consistent styling to messages, ensuring a clear and professional output.

Key Features:
- Displays Spark session information (start/end).
- Outputs version information for Python, Pandas, and Apache Arrow.
- Displays date and time in Eastern timezone.
- Utility methods for different message types (success, fail, info, warnings).
- Customizable messages, including rainbow-styled text.

Dependencies:
- colorConfig.py (for color and style configurations)
- sys, datetime, pytz (for system and date-time operations)
"""

import sys
import datetime
import pytz
from .colorConfig import C

class M:
    """
    A class to standardize message outputs with consistent styling.
    All methods are static, allowing direct usage without class instantiation.
    """

    @staticmethod
    def sparkStart(spark_version: str, script_name: str):
        """
        Display a message indicating the start of a Spark session.

        Args:
            spark_version (str): The version of Spark being used.
            script_name (str): The name of the script being executed.
        """
        print(f"{C.b}{C.spark}Spark Session:{C.r} {C.b}{C.imperialIvory}{spark_version}{C.r}")

    @staticmethod
    def sparkEnd():
        """
        Display a message indicating the end of a Spark session.
        """
        print(f"{C.b}{C.spark}Spark Session{C.r} has been successfully {C.rv}{C.b}{C.negative}CLOSED.{C.r}")

    @staticmethod
    def pythonVersion():
        """
        Display the current Python version.
        """
        print(f"{C.b}{C.python}Python Version:{C.r} {C.b}{C.imperialIvory}{sys.version.split()[0]}{C.r}")

    @staticmethod
    def pandasVersion():
        """
        Display the current Pandas version.
        """
        import pandas as pd  # Import moved here to avoid unnecessary dependency if method isn't used.
        print(f"{C.b}{C.pandas}Pandas Version:{C.r} {C.b}{C.imperialIvory}{pd.__version__}{C.r}")

    @staticmethod
    def arrowVersion():
        """
        Display the current Apache Arrow version, if installed.
        """
        version = (
            sys.modules.get('pyarrow', type('', (), {'__version__': None})()).__version__
            if hasattr(sys.modules.get('pyarrow', None), '__version__') else None
        )
        if version:
            print(f"{C.b}{C.arrow}Apache Arrow Version:{C.r} {C.b}{C.seaShell}{version}{C.r}")
        else:
            print(f"{C.b}{C.arrow}Apache Arrow is not installed.{C.r}")

    @staticmethod
    def dateAndTime():
        """
        Display the current date and time in the Eastern timezone.
        """
        eastern = pytz.timezone('America/New_York')
        now_eastern = datetime.datetime.now(eastern)
        date_str = now_eastern.strftime('%Y-%m-%d')
        time_str = now_eastern.strftime('%H:%M:%S EST')

        print(f"{C.b}{C.imperialIvory}Current Date:{C.r} {C.b}{C.tropicalTurquoise}{date_str}{C.r}\n"
              f"{C.b}{C.imperialIvory}Current Time:{C.r} {C.b}{C.tropicalTurquoise}{time_str}{C.r}")

    @staticmethod
    def startMain():
        """
        Generate a message indicating the start of the main function.

        Returns:
            str: Styled start message.
        """
        return f"{C.b}{C.rv}{C.rallyRed}Starting Main Function . . .{C.r}"

    @staticmethod
    def endMain():
        """
        Generate a message indicating the end of the main function.

        Returns:
            str: Styled end message.
        """
        return f"{C.b}{C.rv}{C.rallyRed}. . . End of Main Function{C.r}"

    @staticmethod
    def fail(message):
        """
        Display a failure message.

        Args:
            message (str): The message to display.
        """
        print(f"{C.b}{C.negative}{message}{C.r}")

    @staticmethod
    def success(message):
        """
        Display a success message.

        Args:
            message (str): The message to display.
        """
        print(f"{C.b}{C.positive}{message}{C.r}")

    @staticmethod
    def info(message):
        """
        Display an informational message.

        Args:
            message (str): The message to display.
        """
        print(f"{C.cyan}{message}{C.r}")

    @staticmethod
    def warning(message):
        """
        Display a warning message.

        Args:
            message (str): The message to display.
        """
        print(f"{C.yellow}{message}{C.r}")

    @staticmethod
    def positive(message: str) -> None:
        """
        Display a positive message.

        Args:
            message (str): The message to display.
        """
        print(f"{C.b}{C.positive}{message}{C.r}")

    @staticmethod
    def negative(message: str) -> None:
        """
        Display a negative message.

        Args:
            message (str): The message to display.
        """
        print(f"{C.b}{C.negative}{message}!!!{C.r}")

    @staticmethod
    def rainbow_message(message):
        """
        Display a message with each character styled in a rainbow of colors.

        Args:
            message (str): The message to display.
        """
        colors = [C.red, C.yellow, C.green, C.cyan, C.blue, C.purple]
        colored_message = ''.join(colors[i % len(colors)] + char for i, char in enumerate(message))
        print(colored_message + C.r)

    @staticmethod
    def colorConfigCheck():
        """
        Display a test message to verify colorConfig integration.
        """
        M.rainbow_message("colorConfig.py imported successfully")
