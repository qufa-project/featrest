import os


def remove(path):
    if path is None:
        return
    try:
        os.remove(path)
    except FileNotFoundError:
        pass
