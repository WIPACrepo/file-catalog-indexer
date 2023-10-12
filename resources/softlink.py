#!/usr/bin/env python3
# softlink.py

import os
import sys


def find_soft_links(root: str) -> None:
    """Walk the provided tree and print out softlinks."""
    for foldername, subfolders, filenames in os.walk(root):
        # check for soft links in folders
        for subfolder in subfolders:
            path = os.path.join(foldername, subfolder)
            if os.path.islink(path):
                target = os.readlink(path)
                print(f"{path} -> {target}")

        # check for soft links in files
        for filename in filenames:
            path = os.path.join(foldername, filename)
            if os.path.islink(path):
                target = os.readlink(path)
                print(f"{path} -> {target}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 softlink.py <root-path>")
        sys.exit(1)

    root_path = sys.argv[1]

    if not os.path.exists(root_path):
        print(f"The path '{root_path}' does not exist.")
        sys.exit(1)

    find_soft_links(root_path)
