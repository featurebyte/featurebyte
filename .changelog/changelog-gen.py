import os
import sys
from enum import Enum

import yaml


class ChangeTypes(Enum):
    BREAKING = 'breaking'
    DEPRECATION = 'deprecation'
    ENHANCEMENT = 'enhancement'
    BUG_FIX = 'bug_fix'

    def __str__(self):
        return self.value

    def __header__(self):
        if self == ChangeTypes.BREAKING:
            return "### 🛑 Breaking Changes"
        elif self == ChangeTypes.DEPRECATION:
            return "### ⚠️ Deprecations"
        elif self == ChangeTypes.ENHANCEMENT:
            return "### 💡 Enhancements"
        elif self == ChangeTypes.BUG_FIX:
            return "### 🐛 Bug Fixes"

    @staticmethod
    def from_str(s: str):
        if s == 'breaking':
            return ChangeTypes.BREAKING
        elif s == 'deprecation':
            return ChangeTypes.DEPRECATION
        elif s == 'enhancement':
            return ChangeTypes.ENHANCEMENT
        elif s == 'bug_fix':
            return ChangeTypes.BUG_FIX
        else:
            raise ValueError("Invalid change type: " + s)


class Changelog:
    def __init__(self, fpath: str):
        with open(fpath) as f:
            clog = yaml.safe_load(f)

        # Validate changelog file
        self.__validate_keys(clog)
        self.__validate_types(clog)

        # Set changelog attributes
        self.change_type = clog["change_type"]
        self.component = clog["component"]
        self.issues = clog["issues"]
        self.note = clog["note"]
        self.subtext = clog["subtext"]

    def __str__(self):
        return f"Changelog({self.change_type}, {self.component}, {self.issues}, {self.note}, {self.subtext})"

    def __repr__(self):
        return str(self)

    @staticmethod
    def __validate_keys(clog: dict) -> None:
        if "change_type" not in clog:
            raise KeyError("Missing change type key in changelog file")
        if "component" not in clog:
            raise KeyError("Missing component key in changelog file")
        if "issues" not in clog:
            raise KeyError("Missing issues key in changelog file")
        if "note" not in clog:
            raise KeyError("Missing note key in changelog file")
        if "subtext" not in clog:
            raise KeyError("Missing subtext key in changelog file")

    @staticmethod
    def __validate_types(clog: dict) -> None:
        if not isinstance(clog["change_type"], str):
            raise TypeError("Change type must be a string")

        # Convert change type to enum
        clog["change_type"] = ChangeTypes.from_str(clog["change_type"])

        if not isinstance(clog["component"], str):
            raise TypeError("Component must be a string")
        if not isinstance(clog["issues"], list):
            print("[WARN] issues is not a list, converting to empty list")
            clog["issues"] = []
        if not isinstance(clog["note"], str):
            raise TypeError("Note must be a string")
        if not isinstance(clog["subtext"], str):
            print("[WARN] subtext is not a string, converting to empty str")
            clog["subtext"] = ""


def get_script_path() -> str:
    return os.path.dirname(os.path.realpath(sys.argv[0]))


def get_changelog_files() -> list[str]:
    # no changelog file specified
    if len(sys.argv) == 1:
        print("No changelog file specified, using default .changelog/*.yaml")
        files = filter(lambda path: os.path.isfile(path), os.listdir(get_script_path()))
        files = filter(lambda path: path.endswith(".yaml"), files)
        files = list(filter(lambda path: path != "TEMPLATE.yaml", files))

        print("Found changelog files: " + str(files))
        for f in files:
            print(" + " + f)

    # changelog file specified
    else:
        files = sys.argv[1:]
        print("Using specified changelog files: ")
        for f in files:
            print(" + " + f)

        missing_files = list(filter(lambda path: not os.path.isfile(path),
                                    map(lambda f: os.path.join(get_script_path(), f), files)))
        if len(missing_files) != 0:
            print("Specified changelog files do not exist:")
            for f in missing_files:
                print(" + " + f)
            sys.exit(1)

    return list(map(lambda f: os.path.join(get_script_path(), f), files))


class ChangelogRenderer:
    def __init__(self, changelogs: list[Changelog]):
        self.changelogs = changelogs


if __name__ == '__main__':
    if "--help" in sys.argv:
        print("Usage: python changelog-gen.py [<changelog-file>...]")
        print("If no changelog file is specified, the default is .changelog/*.yaml")
        sys.exit(1)

    for file in get_changelog_files():
        clog = Changelog(file)
        pass
