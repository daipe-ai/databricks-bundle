from databricksbundle.container.ConfigsWatcherThread import ConfigsWatcherThread
from databricksbundle.container.WatcherLogger import WatcherLogger


def watch_configs(configs_dir: str, init_container: callable):
    watcher_logger = WatcherLogger()

    def prepare_container():
        try:
            init_container()
            watcher_logger.info("New container ready")
        except Exception as e:
            watcher_logger.error("Container initialization failed")
            watcher_logger.debug(str(e))

    watcher_thread = ConfigsWatcherThread(configs_dir, watcher_logger, prepare_container)
    watcher_thread.start()

    return watcher_logger, watcher_thread
