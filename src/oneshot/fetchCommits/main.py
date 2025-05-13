import traceback

from _interface import GithubClient, MysqlClient
from config import logger
from core import CommitsFetcher


def main() -> int:
    mysql_client = MysqlClient(logger=logger)
    github_client = GithubClient(logger=logger)
    fetcher = CommitsFetcher(
        logger=logger, mysql_client=mysql_client, github_client=github_client
    )
    return fetcher.work()


if __name__ == "__main__":
    logger.info("Starting commits fetching and insertion job.")
    try:
        inserted = main()
    except Exception as e:
        inserted = 0
        logger.error(
            f"failed to fetch and insert commits. {type(e)}, {str(e)}, {traceback.print_exc()}"
        )
    logger.info(f"Fetching and insertion succeded, inserted {inserted} commits.")
