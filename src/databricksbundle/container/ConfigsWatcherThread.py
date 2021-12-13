import threading
import time
from pathlib import Path
from typing import List
from databricksbundle.container.WatcherLogger import WatcherLogger


class ConfigsWatcherThread(threading.Thread):
    def __init__(self, configs_dir: str, watcher_logger: WatcherLogger, callback, polling_interval=1):
        threading.Thread.__init__(self)
        self._configs_dir = configs_dir
        self._watcher_logger = watcher_logger
        self._callback = callback
        self._polling_interval = polling_interval

        def excepthook(args):
            watcher_logger.error("Configs watcher failed")
            watcher_logger.error(str(args))

        threading.excepthook = excepthook

    def run(self):
        from datetime import datetime as dt

        self._watcher_logger.info(f"Watching of {self._configs_dir} started")

        def get_config_file_paths(base_path: str):
            return list(Path(base_path).rglob("*.yaml"))

        def get_files_with_timestamp(paths: List[Path]):
            return {str(path): path.stat().st_mtime for path in paths}

        files_with_timestamp_previous = get_files_with_timestamp(get_config_file_paths(self._configs_dir))

        while True:
            files_with_timestamp_new = get_files_with_timestamp(get_config_file_paths(self._configs_dir))

            for path, timestamp in files_with_timestamp_previous.items():
                if path not in files_with_timestamp_new:
                    self._watcher_logger.info(f"Existing file deleted: {path}")
                    self._callback()
                    break

                if files_with_timestamp_new[path] > timestamp:
                    time_string = dt.fromtimestamp(files_with_timestamp_new[path]).strftime("%m.%d.%Y_%H:%M:%S")
                    self._watcher_logger.info(f"File changed: {path}, timestamp: {time_string}")
                    self._callback()
                    break

            new_files_only = set(files_with_timestamp_new.keys()) - set(files_with_timestamp_previous.keys())

            if new_files_only != set():
                self._watcher_logger.info(f"New file(s) found: {new_files_only}")
                self._callback()

            files_with_timestamp_previous = files_with_timestamp_new

            time.sleep(self._polling_interval)
