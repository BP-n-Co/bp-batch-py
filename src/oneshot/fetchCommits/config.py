from _interface import get_logger

# TODO: change to the one in _config if turned to batch
ENV = "local"
SILENT = False

logger = get_logger(name="FetchCommitsLogger", env=ENV)
