"""
This script is used to enforce import rules for featurebyte.
"""

from typing import Dict, List, Set, Tuple

import ast
import os
import sys

# Define type aliases for clarity
Rule = Dict[str, Set[str]]
Rules = Dict[str, Rule]
Error = Tuple[str, int, str]
Errors = List[Error]


class ImportRuleChecker(ast.NodeVisitor):
    """A class to check Python files against specified import rules."""

    def __init__(self, filename: str, rules: Rules):
        """
        Initialize the ImportRuleChecker.

        Parameters:
        filename (str): The name of the file being checked.
        rules (Rules): The set of rules to apply for import checking.
        """
        self.filename = filename
        self.rules = rules
        self.errors: Errors = []

    def visit_Import(self, node: ast.Import) -> None:
        """Visit a standard import statement and check it against the rules."""
        for alias in node.names:
            self.check_import(alias.name, node.lineno)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:
        """Visit a 'from ... import ...' statement and check it against the rules."""
        if node.module is not None:
            self.check_import(node.module, node.lineno)

    def check_import(self, module_name: str, lineno: int) -> None:
        """Check if an import violates any of the defined rules."""
        for dir_name, rule in self.rules.items():
            if dir_name in self.filename:
                # Check if the file is excluded from the rule
                if "exclude_files" in rule and self.filename in rule["exclude_files"]:
                    # if the file is excluded, we can skip the rest of the rules
                    break

                # Check if the module is not in the whitelist
                if "whitelist" in rule:
                    if any(module_name.startswith(allowed) for allowed in rule["whitelist"]):
                        # if the module is in the whitelist, we can skip the rest of the rules
                        break

                    self.errors.append(
                        (self.filename, lineno, f"Import '{module_name}' not in whitelist")
                    )

                # Check for restricted imports
                if "no_import_from" in rule and module_name in rule["no_import_from"]:
                    self.errors.append(
                        (self.filename, lineno, f"No import allowed from {module_name}")
                    )


def check_file(filename: str, rules: Rules) -> Errors:
    """Check a single Python file against the import rules."""
    with open(filename) as file:
        node = ast.parse(file.read(), filename=filename)
    checker = ImportRuleChecker(filename, rules)
    checker.visit(node)
    return checker.errors


def main(project_root: str, rules: Rules) -> None:
    """Walk through a project directory and check all Python files against the rules."""
    violations_found = False
    for root, _, files in os.walk(project_root):
        for file in files:
            if file.endswith(".py"):
                filepath = os.path.join(root, file)
                errors = check_file(filepath, rules)
                for error in errors:
                    print(f"Violation in {error[0]} line {error[1]}: {error[2]} imported")
                    violations_found = True

    if violations_found:
        sys.exit(1)


if __name__ == "__main__":
    project_root = "featurebyte"

    # Define whitelisted libraries including built-in, standard libraries, and third-party libraries
    BUILTIN_MODULES = set(sys.builtin_module_names)
    STANDARD_LIBS = {
        "__future__",
        "abc",
        "ast",
        "base64",
        "collections",
        "contextlib",
        "copy",
        "ctypes",
        "dataclasses",
        "datetime",
        "enum",
        "functools",
        "hashlib",
        "http",
        "humanize",
        "inspect",
        "io",
        "json",
        "operator",
        "os",
        "packaging.version",
        "pathlib",
        "re",
        "shutil",
        "string",
        "threading",
        "tempfile",
        "textwrap",
        "traceback",
        "types",
        "typing",
        "uuid",
        "warnings",
    }
    THIRD_PARTY_LIBS = {
        # NOTE: this list contains the third party libraries that will be installed by featurebyte client
        "alive_progress",
        "bson",
        "black",
        "cachetools",
        "cryptography",
        "databricks",
        "dateutil",
        "fastapi",
        "jinja2",
        "matplotlib",
        "numpy",
        "pandas",
        "pyarrow",
        "pyarrow.parquet",
        "pydantic",
        "pymongo",
        "pyspark",
        "sqlglot",
        "typeguard",
        "yaml",
        "IPython",
        "mlflow",
        "croniter",
        "pytz",
    }

    # Common libraries that can be imported from both backend and client
    COMMON_LIBS = BUILTIN_MODULES.union(STANDARD_LIBS).union(THIRD_PARTY_LIBS)

    # Common modules that can be imported from both backend and client
    COMMON_MODS = {
        f"featurebyte.{mod}"
        for mod in [
            "common",
            "enum",
            "exception",
            "models",
            "query_graph",
            "schema",
            "utils",
            "logging",
            "core",
            "typing",
        ]
    }

    # Define backend-specific and client-specific modules
    BACKEND_ONLY_MODS = {
        f"featurebyte.{mod}"
        for mod in ["routes", "services", "session", "storage", "tile", "worker", "feature_manager"]
    }
    CLIENT_ONLY_MODS = {
        f"featurebyte.{mod}" for mod in ["api", "config", "datasets", "docker", "list_utility"]
    }

    # Define specific rules for different parts of the project
    # Note that existing violations are handled by either whitelisting or excluding the file from the rule
    RULES: Rules = {
        # import rules for directories used by both backend and client
        "featurebyte/enum": {
            "whitelist": COMMON_LIBS.union({"featurebyte.common.doc_util"}),
            "no_import_from": BACKEND_ONLY_MODS.union(CLIENT_ONLY_MODS),
        },
        "featurebyte/models": {
            "whitelist": COMMON_LIBS.union(COMMON_MODS).union(
                {
                    "featurebyte.session.base",  # models/{request_input.py, observation_table.py}
                    "featurebyte.feature_manager.model",  # models/online_store.py
                }
            ),
            "no_import_from": BACKEND_ONLY_MODS.union(CLIENT_ONLY_MODS),
        },
        "featurebyte/schema": {
            "whitelist": COMMON_LIBS.union(COMMON_MODS).union(
                {
                    "featurebyte.config",  # schema/feature_list.py
                }
            ),
            "no_import_from": BACKEND_ONLY_MODS.union(CLIENT_ONLY_MODS),
        },
        "featurebyte/query_graph": {
            "whitelist": COMMON_LIBS.union(COMMON_MODS).union(
                {
                    "featurebyte.session.base",  # query_graph/sql/{online_serving.py, feature_historical.py}
                    "featurebyte.session.session_helper",  # query_graph/sql/online_serving.py
                    "featurebyte.service.online_store_table_version",  # query_graph/sql/online_serving.py
                    "featurebyte.service.cron_helper",  # query_graph/sql/online_serving.py
                }
            ),
            "no_import_from": BACKEND_ONLY_MODS.union(CLIENT_ONLY_MODS),
        },
        # some backend specific directories import rules
        "featurebyte/session": {
            "no_import_from": CLIENT_ONLY_MODS.union(
                {"featurebyte.services", "featurebyte.routes", "featurebyte.worker"}
            ),
        },
        "featurebyte/services": {
            "no_import_from": CLIENT_ONLY_MODS.union({"featurebyte.routes", "featurebyte.worker"}),
        },
        "featurebyte/routes": {
            "no_import_from": CLIENT_ONLY_MODS.union({"featurebyte.worker"}),
            "exclude_files": {"featurebyte/routes/registry.py"},
        },
        # some client specific directories import rules
        "featurebyte/__init__.py": {
            "whitelist": COMMON_LIBS.union(COMMON_MODS).union(CLIENT_ONLY_MODS),
            "no_import_from": BACKEND_ONLY_MODS,
        },
        "featurebyte/api": {
            "whitelist": COMMON_LIBS.union(COMMON_MODS)
            .union(CLIENT_ONLY_MODS)
            .union(
                {
                    "featurebyte.feature_manager.model",  # featurebyte/api/feature_list.py
                }
            ),
            "no_import_from": BACKEND_ONLY_MODS,
        },
    }
    main(project_root, RULES)
