from logging import Logger


# inspired by https://stackoverflow.com/questions/40110540/jupyter-magic-to-handle-notebook-exceptions
def set_notebook_error_handler(logger: Logger):  # noqa: 5302
    import IPython

    def custom_exc(shell, etype, evalue, tb, tb_offset=None):
        logger.error("Notebook exception", exc_info=True)

        # still show the error within the notebook, don't just swallow it
        shell.showtraceback((etype, evalue, tb), tb_offset=tb_offset)

    # this registers a custom exception handler for the whole current notebook
    IPython.get_ipython().set_custom_exc((Exception,), custom_exc)
