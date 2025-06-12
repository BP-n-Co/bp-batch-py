from logging import Logger

from requests import Session

from _config import GITHUB_TOKEN, base_logger


class GithubServerError(Exception):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(f"problem when requesting Github Api, {detail}")


class GithubNoDataResponseError(Exception):
    def __init__(self, detail: str | None = None) -> None:
        super().__init__(f"got no data response {detail}")


class GithubClient:
    def __init__(self, logger: Logger | None = None, token: str | None = None) -> None:
        self.logger = logger if logger else base_logger
        self.session = Session()
        self.token = token if token else GITHUB_TOKEN
        self.date_format = "%Y-%m-%dT%H:%M:%SZ"

    def close(self):
        self.session.close()

    def graphql_post(self, query: str, silent=False) -> dict:
        if not silent:
            self.logger.debug(f"posting to github {query=}")

        headers = {"Authorization": f"token {self.token}"}
        resp = self.session.post(
            url="https://api.github.com/graphql", headers=headers, json={"query": query}
        )

        if not silent:
            self.logger.debug(f"got from github {resp.content=}")

        if not resp.status_code == 200:
            message = f"could not get response from Github {resp.status_code=}."
            self.logger.warning(message)
            raise GithubServerError(detail=message)
        try:
            resp_dict = resp.json()
        except Exception as e:
            message = f"could not serialized Github response : {type(e)=}, {str(e)=}."
            self.logger.warning(message)
            raise GithubServerError(detail=message)
        if not isinstance(resp_dict, dict) or "data" not in resp_dict:
            message = f"{query=} got response without data : {str(resp_dict)=}"
            self.logger.warning(message)
            raise GithubNoDataResponseError(detail=message)
        return resp_dict["data"]
