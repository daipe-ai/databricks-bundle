from databricksbundle.detector import is_databricks_repo
from databricksbundle.notebook.GithubLinkGenerator import GithubLinkGenerator
from logging import Logger


# inspired by https://stackoverflow.com/questions/40110540/jupyter-magic-to-handle-notebook-exceptions
def set_notebook_error_handler(logger: Logger):  # noqa: 5302
    # pylint: disable=import-outside-toplevel
    import IPython

    # pylint: disable=invalid-name
    def custom_exc(shell, etype, evalue, tb, tb_offset=None):
        logger.error("Notebook exception", exc_info=True)

        if is_databricks_repo():
            source_file_github_url = GithubLinkGenerator().generate_link_from_traceback(tb)
            if source_file_github_url:
                logger.error("The source file of the error can be found in the link below:\n" + source_file_github_url)

        # still show the error within the notebook, don't just swallow it
        shell.showtraceback((etype, evalue, tb), tb_offset=tb_offset)

    # this registers a custom exception handler for the whole current notebook
    IPython.get_ipython().set_custom_exc((Exception,), custom_exc)
