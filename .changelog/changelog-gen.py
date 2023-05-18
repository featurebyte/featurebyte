import os
import sys
from enum import Enum

import yaml


class ChangeTypes(Enum):
    BREAKING = "breaking"
    DEPRECATION = "deprecation"
    ENHANCEMENT = "enhancement"
    BUG_FIX = "bug_fix"

    def __str__(self):
        return self.value

    # Return a sorted list of all ChangeTypes
    @staticmethod
    def all():
        return [
            ChangeTypes.BREAKING,
            ChangeTypes.DEPRECATION,
            ChangeTypes.ENHANCEMENT,
            ChangeTypes.BUG_FIX,
        ]

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
        if s == "breaking":
            return ChangeTypes.BREAKING
        elif s == "deprecation":
            return ChangeTypes.DEPRECATION
        elif s == "enhancement":
            return ChangeTypes.ENHANCEMENT
        elif s == "bug_fix":
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
        self.change_type: ChangeTypes = clog["change_type"]
        self.component: str = clog["component"]
        self.issues: list[str] = clog["issues"]
        self.note: str = clog["note"]
        self.subtext: str = clog["subtext"]

    def __str__(self) -> str:
        return f"Changelog({self.change_type}, {self.component}, {self.issues}, {self.note}, {self.subtext})"

    def __repr__(self) -> str:
        return str(self)

    def render(self) -> str:
        if len(self.issues) != 0:
            issues = "[" + ", ".join(self.issues) + "]"
        else:
            issues = ""

        if self.subtext != "":
            subtext = self.subtext.splitlines()
            subtext = list(map(lambda s: f"  {s}", subtext))
            return f"+ `{self.component}` {self.note} {issues}\n" + "\n".join(subtext) + "\n"
        else:
            return f"+ `{self.component}` {self.note} {issues}\n"

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
        if len(clog["component"].split(" ")) > 1:
            raise TypeError("Component must be a single word")

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


def get_changelog_files(args: list[str]) -> list[str]:
    # no changelog file specified
    if len(args) == 0:
        files = map(
            lambda file: os.path.join(get_script_path(), file), os.listdir(get_script_path())
        )
        files = filter(lambda file: os.path.isfile(file), files)
        files = filter(lambda file: file.endswith(".yaml"), files)
        files = list(filter(lambda file: os.path.basename(file) != "TEMPLATE.yaml", files))

    # changelog file specified
    else:
        files = args
        files = list(map(lambda file: os.path.join(get_script_path(), file), files))
        missing_files = list(filter(lambda path: not os.path.isfile(path), files))
        if len(missing_files) != 0:
            print("Specified changelog files do not exist:")
            for f in missing_files:
                print(" + " + f)
            sys.exit(1)

    return list(files)


class ChangelogRenderer:
    def __init__(self, clogs: list[Changelog]):
        self.clogs = clogs

    def render(self) -> str:
        # Group by sections
        sections = {}
        for clog in self.clogs:
            if clog.change_type not in sections:
                sections[clog.change_type] = []
            sections[clog.change_type].append(clog)

        display = ""
        # Render sections in order
        for section in ChangeTypes.all():
            if section not in sections:
                continue
            display += section.__header__() + "\n\n"
            for clog in sections[section]:
                display += clog.render()
            display += "\n"

        return display


if __name__ == "__main__":
    args = sys.argv[1:]
    if "--help" in args:
        print("Usage: python changelog-gen.py [<changelog-file>...]")
        print("If no changelog file is specified, the default is .changelog/*.yaml")
        sys.exit(1)

    # Get all flags
    file_mode = "--file-mode" in args

    if file_mode:
        args.remove("--file-mode")

    files = get_changelog_files(args)

    if file_mode:
        print("Changelog files:")
        for file in files:
            print(" + " + file)
    else:
        clogs = list(map(lambda f: Changelog(f), files))
        renderer = ChangelogRenderer(clogs)
        print(renderer.render())
