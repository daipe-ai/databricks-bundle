import tomlkit
import traceback

from inspect import getsource
from pathlib import Path
from typing import Callable, Union, Optional


class GithubLinkGenerator:
    def __get_repo_name(self, module_name: str) -> str:
        special_repos = {"daipecore": "daipe-core", "featurestorebundle": "feature-store-bundle"}

        if module_name in special_repos:
            return special_repos[module_name]

        return self.__get_repo_name_with_bundle(module_name)

    def __get_repo_name_with_bundle(self, module_name: str) -> str:
        index_of_bundle = module_name.find("bundle")

        return module_name[:index_of_bundle] + "-" + module_name[index_of_bundle:]

    def __get_module_version(self, module_name: str) -> str:
        lockfile_path = Path.cwd().joinpath("poetry.lock")

        # pylint: disable=unspecified-encoding
        with lockfile_path.open("r") as f:
            config = tomlkit.parse(f.read())

        for module in config["package"]:  # pyre-ignore[16]
            if module["name"] == module_name:
                return module["version"]

        raise Exception("Specified module could not be found, make sure the the module is imported and developed by DAIPE")

    def __get_display_html(self):
        # pylint: disable=import-outside-toplevel
        import IPython

        ipython = IPython.get_ipython()

        if not hasattr(ipython, "user_ns") or "displayHTML" not in ipython.user_ns:
            raise Exception("displayHTML cannot be resolved")

        return ipython.user_ns["displayHTML"]

    def __get_github_url_from_module(self, module: Union[type, Callable]) -> str:
        module_path = module.__module__
        module_name_parent = module_path.split(".")[0]
        github_repo_name = self.__get_repo_name(module_name_parent)
        module_file_path = module_path.replace(".", "/") + ".py"
        version = self.__get_module_version(github_repo_name)

        return self.__generate__github_url(github_repo_name, version, module_file_path)

    def __generate__github_url(self, repo_name: str, version: str, file_path: str, line_number: Optional[str] = None) -> str:
        github_path = "https://github.com/daipe-ai/"
        line_number = "" if line_number is not None else line_number

        return f"{github_path}{repo_name}/blob/v{version}/src/{file_path}#L{line_number}"

    def __get_file_path_from_stack_trace(self, stack_trace: traceback.FrameSummary) -> str:
        filename_with_error = stack_trace.filename

        return filename_with_error.split("site-packages")[1]

    def __is_daipe_module(self, stack_trace: traceback.FrameSummary) -> bool:

        return "site-packages" in stack_trace.filename

    def generate_link_from_module(self, module: Union[type, Callable]) -> object:
        base_module_name = module.__module__.split(".")[-1]
        display_html = self.__get_display_html()
        module_github_url = self.__get_github_url_from_module(module)
        html_method_def = getsource(module).replace("\n", "<br>" + "&nbsp;" * 4)
        html_link = f'<a href="{module_github_url}">Github source file for module {base_module_name}</a>'
        html_string = f"{html_method_def}<br>{html_link}"

        return display_html(html_string)

    def generate_link_from_traceback(self, traceback_input) -> str:
        last_stack_trace = traceback.extract_tb(traceback_input)[-1]

        if not self.__is_daipe_module(last_stack_trace):
            return ""

        file_path = self.__get_file_path_from_stack_trace(last_stack_trace)
        module_name = file_path.split("/")[1]
        github_repo_name = self.__get_repo_name_with_bundle(module_name)

        try:
            version = self.__get_module_version(github_repo_name)
            return self.__generate__github_url(github_repo_name, version, file_path, str(last_stack_trace.lineno))

        except Exception as excep:
            if str(excep) == "Specified module could not be found, make sure the the module is imported and developed by DAIPE":
                return ""
            raise excep
