from logging import Logger

from _interface import DateTimeFormat, GithubClient, MysqlClient, transform_datetime
from config import SILENT


class CommitsFetcher:
    def __init__(
        self, mysql_client: MysqlClient, github_client: GithubClient, logger: Logger
    ) -> None:
        self.mysql_client = mysql_client
        self.github_client = github_client
        self.logger = logger
        self.repos: list[dict[str, object]] = list()
        self.commits: dict[str, list[dict[str, object]]] = dict()
        self.github_users_id: set[str] = set()
        self.github_users: dict[str, dict[str, object]] = dict()

    def work(self) -> int:
        self.fetch_repos()
        self.fetch_commits()
        self.extract_users()
        self.add_missing_user_in_db()
        tot = self.add_commits_to_database()
        self.update_root_is_reached()
        return tot

    def fetch_repos(self):
        self.logger.info("Fetching repositories from database")
        # TODO: Also get owner name and repo name
        try:
            repos = self.mysql_client.select(
                table_name="repository",
                select_col=[
                    "id",
                    "name",
                    "ownerIdUser",
                    "ownerIdOrganization",
                    "ownerIsOrganization",
                    "rootCommitIsReached",
                    "trackedBranchRef",
                ],
                silent=SILENT,
            )
            owners: list[dict[str, object]] = list()
            organization_ids = [
                repo["ownerIdOrganization"]
                for repo in repos
                if repo["ownerIsOrganization"]
            ]
            owners.extend(
                self.mysql_client.select(
                    table_name="git_organization",
                    select_col=["id", "login"],
                    cond_in={"id": organization_ids},
                    silent=SILENT,
                )
            )
            user_ids = [
                repo["ownerIdUser"] for repo in repos if not repo["ownerIsOrganization"]
            ]
            owners.extend(
                self.mysql_client.select(
                    table_name="git_user",
                    select_col=["id", "login"],
                    cond_in={"id": user_ids},
                    silent=SILENT,
                )
            )
            owner_map = {owner["id"]: owner["login"] for owner in owners}
            self.repos = [dt for dt in repos]
            for repo in self.repos:
                repo.update(
                    {
                        "ownerLogin": owner_map[
                            (
                                repo["ownerIdOrganization"]
                                if repo["ownerIsOrganization"]
                                else repo["ownerIdUser"]
                            )
                        ]
                    }
                )
        except Exception as e:
            self.logger.error(f"could not fetch the repositories, {type(e)=} {str(e)=}")
            raise e
        self.logger.info(f"Fetched {len(self.repos)} repositories")

    def update_root_is_reached(self):
        self.logger.info("updating rootCommitIsReached of repos")
        for repo_id in self.commits:
            self.logger.debug(f"updating rootCommitIsReached of {repo_id=}")
            self.mysql_client.update_by_id(
                table_name="repository", id=repo_id, values={"rootCommitIsReached": "1"}
            )

    def add_commits_to_database(self) -> int:
        self.logger.info("Adding commits to database")
        tot = 0
        for repo_id, commits in self.commits.items():
            self.logger.debug(f"Adding commits to {repo_id=}")
            cnt = 0
            for commit in commits:
                self.logger.debug("Checking if commit is already in database")
                if self.mysql_client.id_exists(
                    table_name="commit", id=str(commit["id"]), silent=SILENT
                ):
                    self.logger.debug(f"Found already existing commit {commit['id']}")
                    continue
                cnt += 1
                self.logger.debug(f"gathering {commit=} information")
                commit["repositoryId"] = repo_id

                author = commit["author"]
                id_author = None
                commit["authorAvatarUrl"] = None
                commit["authorEmail"] = None
                commit["authorName"] = None
                commit["authoredDate"] = transform_datetime(
                    date=str(commit["authoredDate"]),
                    input_format=DateTimeFormat.github,
                    output_formt=DateTimeFormat.bp_co_long,
                )
                if isinstance(author, dict):
                    commit["authorAvatarUrl"] = author["avatarUrl"]
                    commit["authorEmail"] = author["email"]
                    commit["authorName"] = author["name"]
                    if author["user"]:
                        id_author = author["user"]["id"]
                if id_author:
                    self.logger.debug(f"existing user, {self.github_users[id_author]}")
                    commit["authorAvatarUrl"] = str(
                        self.github_users[id_author]["avatarUrl"]
                    )
                    commit["authorEmail"] = str(self.github_users[id_author]["email"])
                    commit["authorName"] = (
                        str(self.github_users[id_author]["login"])
                        if str(self.github_users[id_author]["login"])
                        else str(self.github_users[id_author]["name"])
                    )
                commit["authorId"] = id_author

                committer = commit["committer"]
                id_committer = None
                commit["committerAvatarUrl"] = None
                commit["committerEmail"] = None
                commit["committerName"] = None
                commit["committedDate"] = transform_datetime(
                    date=str(commit["committedDate"]),
                    input_format=DateTimeFormat.github,
                    output_formt=DateTimeFormat.bp_co_long,
                )
                if isinstance(committer, dict):
                    commit["committerAvatarUrl"] = committer["avatarUrl"]
                    commit["committerEmail"] = committer["email"]
                    commit["committerName"] = committer["name"]
                    if committer["user"]:
                        id_committer = committer["user"]["id"]
                if id_committer:
                    self.logger.debug(
                        f"existing user, {self.github_users[id_committer]}"
                    )
                    commit["committerAvatarUrl"] = str(
                        self.github_users[id_committer]["avatarUrl"]
                    )
                    commit["committerEmail"] = str(
                        self.github_users[id_committer]["email"]
                    )
                    commit["committerName"] = (
                        str(self.github_users[id_committer]["login"])
                        if str(self.github_users[id_committer]["login"])
                        else str(self.github_users[id_committer]["name"])
                    )
                commit["committerId"] = id_committer

                columns = [
                    "id",
                    "repositoryId",
                    "additions",
                    "deletions",
                    "authoredDate",
                    "authorAvatarUrl",
                    "authorEmail",
                    "authorId",
                    "authorName",
                    "committedDate",
                    "committerAvatarUrl",
                    "committerEmail",
                    "committerId",
                    "committerName",
                ]
                commit_to_insert = {col: commit[col] for col in columns}
                self.logger.debug(f"Inserting {commit_to_insert} in db")
                self.mysql_client.insert_one(
                    table_name="commit",
                    values=commit_to_insert,
                    silent=SILENT,
                )
            tot += cnt
        return tot

    def add_missing_user_in_db(self):
        self.logger.info(f"Adding missing users in database")
        res = self.mysql_client.select(
            table_name="git_user",
            select_col=["id", "avatarUrl", "email", "name", "login"],
            cond_in={"id": list(self.github_users_id)},
            silent=SILENT,
        )
        for user in res:
            self.github_users[str(user["id"])] = user
        existing_ids = {id for id in self.github_users}
        missing_ids = self.github_users_id.difference(existing_ids)
        self.logger.info(
            f"Over the {len(self.github_users_id)} git users, {len(missing_ids)} are not in Database. Fetching github api"
        )
        for id in missing_ids:
            self.logger.debug(f"Fetching {id=}")
            user_info = self.get_git_user_info(id=id)
            user_info["id"] = id
            self.github_users[id] = user_info
            self.logger.debug(f"Got {user_info}, inserting into database")
            self.mysql_client.insert_one(
                table_name="git_user", values=user_info, silent=SILENT
            )
            self.logger.debug("Insertion done")

    def get_git_user_info(self, id: str) -> dict[str, object]:
        query = f"""
            query {{
                node(id:"{id}") {{
                    ... on User {{
                        avatarUrl
                        email
                        name
                        login
                    }}
                }}
            }}
        """
        res = self.github_client.graphql_post(query=query, silent=SILENT)
        return res["node"]

    def get_next_commits(
        self,
        owner_name: str,
        name: str,
        ref: str,
        end_cursor: str | None,
        since: str = "",
        until: str = "",
    ) -> tuple[list[dict[str, object]], str, bool]:
        if end_cursor is None:
            end_cursor = "null"
        else:
            end_cursor = '"' + end_cursor + '"'
        if since:
            since = ', since:"' + since + '"'
        if until:
            until = ', until:"' + until + '"'
        query = f"""
            query {{
                repository(owner: "{owner_name}", name: "{name}") {{
                    ref(qualifiedName: "{ref}") {{
                        target {{
                            ... on Commit {{
                                history(first: 10, after:{end_cursor}{since}{until}) {{
                                    pageInfo {{
                                        hasNextPage
                                        endCursor
                                    }}
                                    nodes {{
                                        id
                                        additions
                                        deletions
                                        author {{
                                            avatarUrl
                                            email
                                            name
                                            user {{
                                                id
                                            }}
                                        }}
                                        authoredDate
                                        committer {{
                                            avatarUrl
                                            email
                                            name
                                            user {{
                                                id
                                            }}
                                        }}
                                        committedDate
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
            }}"""
        resp = self.github_client.graphql_post(query=query, silent=SILENT)[
            "repository"
        ]["ref"]["target"]["history"]
        commits = resp["nodes"]
        end_cursor = str(resp["pageInfo"]["endCursor"])
        has_next_page = resp["pageInfo"]["hasNextPage"]
        return commits, end_cursor, has_next_page

    def fetch_commits(self):
        for repo in self.repos:
            # 1. Look for the first and last commit we have of the repo in the table
            repo_id = str(repo["id"])
            self.logger.info(
                f"Looking into db for most and least recent commits of {repo_id=}"
            )
            most_recent_commit = self.mysql_client.select(
                table_name="commit",
                select_col=["id", "committedDate"],
                cond_eq={"repositoryId": repo_id},
                order_by="committedDate",
                ascending_order=False,
                silent=SILENT,
                limit=1,
            )
            oldest_commit = self.mysql_client.select(
                table_name="commit",
                select_col=["id", "committedDate"],
                cond_eq={"repositoryId": repo_id},
                order_by="committedDate",
                ascending_order=True,
                silent=SILENT,
                limit=1,
            )
            if most_recent_commit and oldest_commit:
                msg = f"Found the most recent commit at date {most_recent_commit[0]['committedDate']}. "
                msg += f"Found the oldest commit at date {oldest_commit[0]['committedDate']}"
            else:
                msg = "No records of recent and oldest commits founded"
            self.logger.info(msg)

            # 2. Fetch commits
            # 2.1 Fetch from start until most_recent_commit (if exists)
            repo_name = str(repo["name"])
            repo_tracked_branch_ref = str(repo["trackedBranchRef"])
            repo_owner_name = str(repo["ownerLogin"])
            self.commits[repo_id] = list()
            self.logger.info(
                f"starting fetching of branch ref {repo_tracked_branch_ref} of {repo_name=}, {repo_owner_name=}"
            )

            most_recent_date = (
                transform_datetime(
                    date=str(most_recent_commit[0]["committedDate"]),
                    output_formt=DateTimeFormat.github,
                    input_format=DateTimeFormat.bp_co_long,
                )
                if most_recent_commit
                else "1970-01-01T00:00:00Z"
            )
            end_cursor = None
            has_next_page = True
            while has_next_page:
                commits, end_cursor, has_next_page = self.get_next_commits(
                    owner_name=repo_owner_name,
                    name=repo_name,
                    ref=repo_tracked_branch_ref,
                    end_cursor=end_cursor,
                    since=most_recent_date,
                )
                self.logger.debug(
                    f"found {len(commits)} commits, next request starting from {end_cursor=}. {has_next_page=}"
                )
                self.commits[repo_id].extend(commits)
            self.logger.info("Fetched until the most recent commit.")

            # 2.2 If oldes_commit exists and the root is not reached, fetch until the root
            repo_root_is_reached = str(repo["rootCommitIsReached"]) == "1"
            if oldest_commit and not repo_root_is_reached:
                oldest_date = transform_datetime(
                    date=str(oldest_commit[0]["committedDate"]),
                    output_formt=DateTimeFormat.github,
                    input_format=DateTimeFormat.bp_co_long,
                )
                end_cursor = None
                has_next_page = True
                while has_next_page:
                    commits, end_cursor, has_next_page = self.get_next_commits(
                        owner_name=repo_owner_name,
                        name=repo_name,
                        ref=repo_tracked_branch_ref,
                        end_cursor=end_cursor,
                        until=oldest_date,
                    )
                    self.logger.debug(
                        f"found {len(commits)} commits, next request starting from {end_cursor=}"
                    )
                    self.commits[repo_id].extend(commits)
            self.logger.info("Fetched until root.")
            self.logger.info(
                f"Fetched a total of {len(self.commits[repo_id])} commits."
            )

    def extract_users(self):
        self.logger.info("Starting author and committer extraction")
        for repo_id in self.commits:
            self.logger.info(f"Starting author extraction on {repo_id=}")
            self.logger.info(f"Got {len(self.commits[repo_id])} commits")
            for commit in self.commits[repo_id]:
                author = commit["author"]
                if isinstance(author, dict):
                    user = author["user"]
                    self.logger.debug(f"Found author with id: {user}")
                    if user:
                        self.github_users_id.add(user["id"])
                committer = commit["committer"]
                if isinstance(committer, dict):
                    user = committer["user"]
                    self.logger.debug(f"Found committer with id : {user}")
                    if user:
                        self.github_users_id.add(user["id"])
        self.logger.info("Author and committer extraction done")
