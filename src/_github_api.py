from datetime import datetime
from logging import Logger

from requests import Session

from _config import GITHUB_TOKEN, base_logger


class GithubRequestException(Exception):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(f"Error when requesting Github Api, {detail}")


class GithubWrongAttributesException(Exception):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(f"wrong attributes when requesting github, {detail}")


class GithubClient:
    def __init__(self, logger: Logger | None = None, token: str | None = None) -> None:
        self.logger = logger if logger else base_logger
        self.session = Session()
        self.token = token if token else GITHUB_TOKEN
        self.date_format = "%Y-%m-%dT%H:%M:%SZ"

    def close(self):
        self.session.close()

    def graphql_post(self, query: str, silent: bool = False) -> dict:
        if not silent:
            self.logger.debug(f"Github api executed: {query}")
        headers = {"Authorization": f"token {self.token}"}
        resp = self.session.post(
            url="https://api.github.com/graphql", headers=headers, json={"query": query}
        )
        if not resp.status_code == 200:
            message = f"could not get response from Github {resp.status_code=}."
            self.logger.error(message)
            raise GithubRequestException(detail=message)
        try:
            resp_dict = resp.json()
        except Exception as e:
            message = f"could not serialized Github response : {type(e)=}, {str(e)=}."
            self.logger.warning(message)
            raise GithubRequestException(detail=message)
        if not isinstance(resp_dict, dict) or "data" not in resp_dict:
            message = f"got response without data : {str(resp_dict)=}"
            self.logger.warning(message)
            raise GithubRequestException(detail=message)
        if not silent:
            self.logger.debug(f"Got response from github {resp_dict}")
        return resp_dict["data"]
