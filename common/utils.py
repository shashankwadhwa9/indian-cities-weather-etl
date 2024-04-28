from datetime import datetime
import argparse


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        msg = f"Not a valid date: '{s}'. Expected format: 'YYYY-MM-DD'."
        raise argparse.ArgumentTypeError(msg)
