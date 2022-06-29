"""
Persistent storage using MongoDB
"""
# pylint: disable=protected-access
from __future__ import annotations

from typing import Any, Iterable, List, Literal, Optional, Tuple

import functools
import json
import os
import shutil
import tempfile

from bson import json_util
from bson.objectid import ObjectId
from git import GitCommandError
from git.remote import Remote
from git.repo.base import Repo

from featurebyte.logger import logger

from .persistent import DocumentType, DuplicateDocumentError, Persistent


def _sync_push(func: Any) -> Any:
    """
    Synchronize git conflicts

    Parameters
    ----------
    func: Any
        Function to run

    Returns
    -------
    Any
        Return from function
    """

    @functools.wraps(func)
    def wrapper_decorator(cls: GitDB, *args: int, **kwargs: str) -> Any:
        try:
            value = func(cls, *args, **kwargs)
            cls._push()
        except GitCommandError:
            logger.debug("Git conflict detected. Retrying.")
            cls._reset_branch()
            value = func(cls, *args, **kwargs)
            cls._push()
        return value

    return wrapper_decorator


class GitDB(Persistent):
    """
    Persistent storage using MongoDB
    """

    def __init__(
        self, branch: str = "main", remote_url: Optional[str] = None, key_path: Optional[str] = None
    ) -> None:
        """
        Constructor for MongoDB

        Parameters
        ----------
        branch: str
            Branch to use
        remote_url: Optional[str]
            Remote repository url
        key_path: Optional[str]
            Path to private key
        """
        self._local_path = tempfile.mkdtemp()

        if not remote_url:
            logger.warning("Remote repo not specified.")

        logger.debug("Initializing local repo", extra={"local_path": self._local_path})
        Repo.init(self._local_path)

        self._repo = repo = Repo(path=self._local_path)
        self._branch = branch
        self._refspec = f"refs/heads/{self._branch}:refs/heads/{self._branch}"
        self._ssh_cmd = f"ssh -i {key_path}" if key_path else "ssh"
        self._origin: Optional[Remote] = None
        self._working_tree_dir: str = str(self._repo.working_tree_dir)

        if remote_url:
            # create remote origin if does not exist
            logger.debug("Configuring remote repo", extra={"remote_url": remote_url})
            if not repo.remotes:
                repo.create_remote("origin", remote_url)
            self._origin = repo.remotes.origin

            # confirm repo has correct remote
            assert len(repo.remotes) == 1
            assert self._origin.url == remote_url

        # try to checkout checkout branch
        branch_exists = False
        if self._origin:
            try:
                self._fetch()
                logger.debug("Check out remote branch", extra={"branch": branch})
                repo.create_head(branch, self._origin.refs[branch])
                repo.heads[branch].checkout()
                repo.index.reset(self._origin.refs[branch])
                branch_exists = True
            except (GitCommandError, AttributeError):
                pass

        if not branch_exists:
            logger.debug("Create new branch", extra={"branch": branch})
            # no branch found on remote, create new branch from main
            initial_commit_file_path = os.path.join(self._working_tree_dir, "README.md")
            with open(initial_commit_file_path, "w", encoding="utf-8") as file_obj:
                file_obj.write(
                    "# FeatureByte Git Repo\nRepository for FeatureByte feature engineering SDK"
                )
            repo.index.add([initial_commit_file_path])
            repo.index.commit("Initial commit")
            repo.create_head(branch)
            repo.heads[branch].checkout()
            self._push()

    def __del__(self) -> None:
        """
        Clean up local repo
        """
        local_path = getattr(self, "_local_path", None)
        if local_path:
            logger.debug("Delete local repo", extra={"local_path": local_path})
            shutil.rmtree(local_path)

    def _fetch(self) -> None:
        """
        Fetch latest changes from remote
        """
        # skip if no remote
        if not self._origin:
            return

        logger.debug("Fetch latest from branch", extra={"branch": self._branch})
        with self._repo.git.custom_environment(GIT_SSH_COMMAND=self._ssh_cmd):
            self._origin.fetch(f"refs/heads/{self._branch}:refs/heads/origin/{self._branch}")

    def _reset_branch(self) -> None:
        """
        Reset with latest changes from remote
        """
        # skip if no remote
        if not self._origin:
            return

        logger.debug("Reset branch to remote", extra={"branch": self._branch})
        self._fetch()
        self._repo.git.reset("--hard", f"origin/{self._branch}")

    def _push(self) -> None:
        """
        Push latest changes to remote
        """
        # skip if no remote
        if not self._origin:
            return

        logger.debug("Push changes to remote branch", extra={"branch": self._branch})
        with self._repo.git.custom_environment(GIT_SSH_COMMAND=self._ssh_cmd):
            self._origin.push(self._refspec).raise_if_error()

    def _add_file(
        self,
        dir_name: str,
        document: DocumentType,
        doc_name: Optional[str] = None,
        replace: bool = False,
    ) -> ObjectId:
        """
        Add one file to repo

        Parameters
        ----------
        dir_name: str
            Name of directory to use
        document: DocumentType
            Document to insert
        doc_name: Optional[str]
            Document name
        replace: bool
            Replace existing file

        Returns
        -------
        ObjectId
            Id of inserted document

        Raises
        -------
        DuplicateDocumentError
            Document exists
        """
        # ensure collection dir exists
        dir_path = os.path.join(self._working_tree_dir, dir_name)
        if not os.path.exists(dir_path):
            os.mkdir(dir_path)

        # ensures document id is set
        doc_id = document.get("id")
        if not doc_id:
            doc_id = ObjectId()
            document["id"] = doc_id

        # strip user id
        document.pop("user_id", None)

        # create document
        new_doc_name = str(document.get("name", doc_id))
        doc_path = os.path.join(dir_path, new_doc_name + ".json")

        # handle renaming
        is_renaming = doc_name and doc_name != new_doc_name
        if is_renaming:
            old_doc_path = os.path.join(dir_path, f"{doc_name}.json")
            if os.path.exists(old_doc_path):
                self._repo.index.move([old_doc_path, doc_path])
                self._repo.index.commit(
                    f"Renaming document {dir_name}/{doc_name} -> {dir_name}/{new_doc_name}"
                )

        doc_exists = os.path.exists(doc_path)
        action = "Update" if doc_exists else "Create"
        if doc_exists and not replace:
            raise DuplicateDocumentError(f"Document {dir_name}/{new_doc_name} already exists")

        logger.debug(f"{action} file", extra={"doc_path": doc_path, "replace": replace})
        with open(doc_path, "w", encoding="utf-8") as file_obj:
            json.dump(json.loads(json_util.dumps(document)), file_obj, indent=4)

        # commit changes
        self._repo.index.add([doc_path])
        # put name in commit
        self._repo.index.commit(f"{action} document: {dir_name}/{new_doc_name}")

        return doc_id

    def _remove_file(self, dir_name: str, document: DocumentType) -> None:
        """
        Remove one file from repo

        Parameters
        ----------
        dir_name: str
            Name of directory to use
        document: DocumentType
            Document to insert
        """
        # ensure collection dir exists
        dir_path = os.path.join(self._working_tree_dir, dir_name)
        if not os.path.exists(dir_path):
            return

        # remove document
        doc_name = str(document.get("name", str(document["id"])))
        doc_path = os.path.join(dir_path, doc_name + ".json")

        logger.debug(f"Remove file: {doc_path}")
        if os.path.exists(doc_path):
            os.remove(doc_path)

        # commit changes
        self._repo.index.remove([doc_path])
        # put name in commit
        self._repo.index.commit(f"Remove document: {dir_name}/{doc_name}")

    def _find_files(
        self, dir_name: str, filter_query: DocumentType, multiple: bool = False
    ) -> List[DocumentType]:
        """
        Find one record from collection

        Parameters
        ----------
        dir_name: str
            Name of directory to use
        filter_query: DocumentType
            Conditions to filter on
        multiple: bool
            Return multiple documents

        Returns
        -------
        List[DocumentType]
            Retrieved document
        """
        # check unsupported filters
        self._check_filter(filter_query)

        # strip user id
        filter_query.pop("user_id", None)

        filter_items = filter_query.items()

        collection_dir = os.path.join(self._working_tree_dir, dir_name)

        if not os.path.exists(collection_dir):
            return []

        documents = []
        for path in sorted(os.listdir(collection_dir)):
            with open(os.path.join(collection_dir, path), encoding="utf-8") as file_obj:
                doc: DocumentType = json_util.loads(file_obj.read())
            if filter_items <= doc.items():
                if not multiple:
                    return [doc]
                documents.append(doc)
        return documents

    @staticmethod
    def _check_filter(filter_query: DocumentType) -> None:
        """
        Validate filter is supported

        Parameters
        ----------
        filter_query: DocumentType
            Conditions to filter on

        Raises
        ------
        NotImplementedError
            Filter is unsupported
        """
        # check unsupported filters
        for key in filter_query.keys():
            if "$" in key:
                raise NotImplementedError("$ operators not supported")

    @_sync_push
    def insert_one(self, collection_name: str, document: DocumentType) -> ObjectId:
        """
        Insert record into collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        document: DocumentType
            Document to insert

        Returns
        -------
        ObjectId
            Id of the inserted document
        """
        return self._add_file(dir_name=collection_name, document=document)

    @_sync_push
    def insert_many(
        self, collection_name: str, documents: Iterable[DocumentType]
    ) -> List[ObjectId]:
        """
        Insert records into collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        documents: Iterable[DocumentType]
            Documents to insert

        Returns
        -------
        List[ObjectId]
            Ids of the inserted documents
        """
        doc_ids = []
        for document in documents:
            doc_ids.append(self._add_file(dir_name=collection_name, document=document))
        return doc_ids

    def find_one(self, collection_name: str, filter_query: DocumentType) -> Optional[DocumentType]:
        """
        Find one record from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter_query: DocumentType
            Conditions to filter on

        Returns
        -------
        Optional[DocumentType]
            Retrieved document
        """
        self._reset_branch()
        docs = self._find_files(dir_name=collection_name, filter_query=filter_query)
        if not docs:
            return None
        return docs[0]

    def find(
        self,
        collection_name: str,
        filter_query: DocumentType,
        sort_by: Optional[str] = None,
        sort_dir: Optional[Literal["asc", "desc"]] = "asc",
        page: int = 1,
        page_size: int = 0,
    ) -> Tuple[Iterable[DocumentType], int]:
        """
        Find all records from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter_query: DocumentType
            Conditions to filter on
        sort_by: Optional[str]
            Column to sort by
        sort_dir: Optional[Literal["asc", "desc"]]
            Direction to sort
        page: int
            Page number for pagination
        page_size: int
            Page size (0 to return all records)

        Returns
        -------
        Tuple[Iterable[DocumentType], int]
            Retrieved documents and total count
        """
        sort_col = sort_by or "id"
        self._reset_branch()
        docs = self._find_files(dir_name=collection_name, filter_query=filter_query, multiple=True)
        total = len(docs)

        docs = sorted(
            docs,
            key=lambda doc: str(doc[sort_col]),
            reverse=sort_dir == "desc",
        )

        if page_size > 0:
            skips = page_size * (page - 1)
            docs = docs[skips : (skips + page_size)]

        return docs, total

    def update_one(
        self,
        collection_name: str,
        filter_query: DocumentType,
        update: DocumentType,
    ) -> int:
        """
        Update one record in collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter_query: DocumentType
            Conditions to filter on
        update: DocumentType
            Values to update

        Returns
        -------
        int
            Number of records modified

        Raises
        ------
        NotImplementedError
            update format not supported
        """
        # check unsupported update
        if len(update) > 1 or next(iter(update.keys())) != "$set" or "id" in update["$set"]:
            raise NotImplementedError("update not supported")

        self._reset_branch()
        docs = self._find_files(dir_name=collection_name, filter_query=filter_query)
        if not docs:
            return 0
        doc = docs[0]

        # track original doc name
        doc_name = doc.get("name")
        doc.update(update["$set"])
        self._add_file(dir_name=collection_name, document=doc, doc_name=doc_name, replace=True)
        self._push()
        return 1

    def update_many(
        self,
        collection_name: str,
        filter_query: DocumentType,
        update: DocumentType,
    ) -> int:
        """
        Update many records in collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter_query: DocumentType
            Conditions to filter on
        update: DocumentType
            Values to update

        Returns
        -------
        int
            Number of records modified

        Raises
        ------
        NotImplementedError
            update format not supported
        """
        # check unsupported update
        if len(update) > 1 or next(iter(update.keys())) != "$set" or "id" in update["$set"]:
            raise NotImplementedError("update not supported")

        self._reset_branch()
        docs = self._find_files(dir_name=collection_name, filter_query=filter_query, multiple=True)
        num_updated = 0

        try:
            for doc in docs:
                # track original doc name
                doc_name = doc.get("name")
                doc.update(update["$set"])
                self._add_file(
                    dir_name=collection_name, document=doc, doc_name=doc_name, replace=True
                )
                num_updated += 1
        finally:
            self._push()
        return num_updated

    def delete_one(self, collection_name: str, filter_query: DocumentType) -> int:
        """
        Delete one record from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter_query: DocumentType
            Conditions to filter on

        Returns
        -------
        int
            Number of records deleted
        """
        self._reset_branch()
        docs = self._find_files(dir_name=collection_name, filter_query=filter_query)
        if not docs:
            return 0
        doc = docs[0]

        self._remove_file(dir_name=collection_name, document=doc)
        self._push()
        return 1

    def delete_many(self, collection_name: str, filter_query: DocumentType) -> int:
        """
        Delete many records from collection

        Parameters
        ----------
        collection_name: str
            Name of collection to use
        filter_query: DocumentType
            Conditions to filter on

        Returns
        -------
        int
            Number of records deleted
        """
        self._reset_branch()
        docs = self._find_files(dir_name=collection_name, filter_query=filter_query, multiple=True)
        num_deleted = 0

        for doc in docs:
            self._remove_file(dir_name=collection_name, document=doc)
            num_deleted += 1
        self._push()
        return num_deleted
