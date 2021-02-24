import glob
import os
import sys
import traceback

import hcl2
import requests
from checkovhelm.checkov.terraform.checks.data.registry import data_registry
from checkovhelm.checkov.terraform.checks.provider.registry import provider_registry
from checkovhelm.checkov.terraform.checks.resource.registry import resource_registry
from checkovhelm.checkov.terraform.runner import Runner as tf_runner

#from Helmdb.slack_integrator import slack_manager
#from Helmdb.GithubHelmRepoManager import GithubHelmRepoManager
from github import Github, Repository, GithubException, UnknownObjectException


class CheckovRun(GithubHelmRepoManager):
    def __init__(self, repo, path):
        super().__init__(repo, path)

    def checkov_scan(self, repo: Repository, scan_time: str) -> (list, list):
        download_success = self.download_github_repo()
        result_lst = []
        results = tf_runner()
        empty_resources = {}
        if download_success:
            try:
                results_scan = results.run(root_folder=self.code_path, external_checks_dir=None, files=None)
                res = results_scan.get_dict()
                for passed_check in res["results"]["passed_checks"]:
                    check = [
                        provider,
                        offset,
                        self.name,
                        str(self.check_category(passed_check["check_id"])).lstrip("CheckCategories."),
                        passed_check["check_id"],
                        passed_check["check_name"],
                        passed_check["check_result"]["result"],
                        passed_check["resource"].split(".")[0]]
                    check.extend(self.add_meta(scan_time))
                    result_lst.append(check)
                for failed_check in res["results"]["failed_checks"]:
                    check = [
                        provider,
                        offset,
                        self.name,
                        str(self.check_category(failed_check["check_id"])).lstrip("CheckCategories."),
                        failed_check["check_id"],
                        failed_check["check_name"],
                        failed_check["check_result"]["result"],
                        failed_check["resource"].split(".")[0]]
                    check.extend(self.add_meta(scan_time))
                    result_lst.append(check)
                if results_scan.is_empty():
                    check = [
                        provider,
                        offset,
                        self.name,
                        "empty scan",
                        "empty scan",
                        "empty scan",
                        "empty scan",
                        "empty scan"]
                    check.extend(self.add_meta(scan_time))
                    result_lst.append(check)
                    empty_resources = self.module_resources()
            except Exception:
                print('unexpected error in scan')
                exc_type, exc_value, exc_traceback = sys.exc_info()
                tb = traceback.format_exception(exc_type, exc_value, exc_traceback)
                slack_manager.add_error(tb, offset, provider, self.downloads, self.name)
                check = [
                    provider,
                    offset,
                    self.name,
                    'unexpected error in checkov scan',
                    "Error in scan",
                    "Error in scan",
                    "Error in scan",
                    "Error in scan"]
                check.extend(self.add_meta(scan_time))
                result_lst.append(check)
        else:
            r = requests.get(code_url)
            error = f'HTTPError: {r.status_code}, in url: {code_url}'
            check = [
                provider,
                offset,
                self.name,
                error,
                "HTTPError in module",
                "HTTPError in module",
                "HTTPError in module",
                "HTTPError in module"]
            check.extend(self.add_meta(scan_time))
            result_lst.append(check)
        return result_lst, self.checkov_meta(offset, scan_time), empty_resources

    def add_meta(self, scan_time):
        meta = [
            self.module_id,
            self.description,
            self.company_name,
            self.downloads,
            self.namespace,
            self.published,
            self.is_verified,
            scan_time]
        return meta

    def check_category(self, check_id):
        if (resource_registry.get_check_by_id(check_id)) is not None:
            return resource_registry.get_check_by_id(check_id).categories[0]
        elif (data_registry.get_check_by_id(check_id)) is not None:
            return data_registry.get_check_by_id(check_id).categories[0]
        elif (provider_registry.get_check_by_id(check_id)) is not None:
            return provider_registry.get_check_by_id(check_id).categories[0]

    def checkov_meta(self, offset: int, scan_time: str) -> list:
        download_success = self.download_github_repo()
        if download_success:
            try:
                results = tf_runner()
                results_scan = results.run(root_folder=self.code_path, external_checks_dir=None, files=None)
                res = results_scan.get_dict()
                summary_lst = [
                    self.provider,
                    offset,
                    self.company_name,
                    self.name,
                    "None",
                    res["summary"]["passed"],
                    res["summary"]["failed"],
                    res["summary"]["parsing_errors"],
                    self.module_id,
                    self.description,
                    self.downloads,
                    self.published,
                    self.is_verified,
                    scan_time,
                    self.count_tf_files(),
                    self.module_resources(),
                    self.module_dataobjects()]
            except:
                summary_lst = [
                    self.provider,
                    offset,
                    self.company_name,
                    self.name,
                    "Unexpected ERROR",
                    0,
                    0,
                    0,
                    self.module_id,
                    self.description,
                    self.downloads,
                    self.published,
                    self.is_verified,
                    scan_time,
                    self.count_tf_files(),
                    self.module_resources(),
                    self.module_dataobjects()]
        else:
            summary_lst = [
                self.provider,
                offset,
                self.company_name,
                self.name,
                "HTTP ERROR: 404 - source code not found",
                0,
                0,
                0,
                self.module_id,
                self.description,
                self.downloads,
                self.published,
                self.is_verified,
                scan_time,
                self.count_tf_files(),
                self.module_resources(),
                self.module_dataobjects()]
        return summary_lst

    def module_resources(self):
        module_re_dict = {"tf_UnexpectedToken": 0}
        for filename in glob.glob(f"{self.code_path}/**.tf", recursive=True):
            if os.path.isdir(filename):
                for file in glob.glob(f"{filename}/**.tf", recursive=True):
                    with open(file, "r", encoding="utf8") as file:
                        try:
                            tf_definition = hcl2.load(file)
                            if "resource" in tf_definition.keys():
                                for key in tf_definition["resource"][0].keys():
                                    key = "re_" + key
                                    if key in module_re_dict.keys():
                                        module_re_dict[key] += 1
                                    else:
                                        module_re_dict[key] = 1
                        except:
                            module_re_dict["tf_UnexpectedToken"] += 1
            else:
                with open(filename, "r", encoding="utf8") as file:
                    try:
                        tf_definition = hcl2.load(file)
                        if "resource" in tf_definition.keys():
                            for key in tf_definition["resource"][0].keys():
                                key = "re_" + key
                                if key in module_re_dict.keys():
                                    module_re_dict[key] += 1
                                else:
                                    module_re_dict[key] = 1
                    except:
                        module_re_dict["tf_UnexpectedToken"] += 1
        for filename in glob.glob(f"{self.code_path}/modules/**/**.tf", recursive=True):
            if os.path.isdir(filename):
                for file in glob.glob(f"{filename}/**.tf", recursive=True):
                    with open(file, "r", encoding="utf8"):
                        try:
                            tf_definition = hcl2.load(file)
                            if "resource" in tf_definition.keys():
                                for key in tf_definition["resource"][0].keys():
                                    key = "re_" + key
                                    if key in module_re_dict.keys():
                                        module_re_dict[key] += 1
                                    else:
                                        module_re_dict[key] = 1
                        except:
                            module_re_dict["tf_UnexpectedToken"] += 1
            else:
                with open(filename, "r", encoding="utf8") as file:
                    try:
                        tf_definition = hcl2.load(file)
                        if "resource" in tf_definition.keys():
                            for key in tf_definition["resource"][0].keys():
                                key = "re_" + key
                                if key in module_re_dict.keys():
                                    module_re_dict[key] += 1
                                else:
                                    module_re_dict[key] = 1
                    except:
                        module_re_dict["tf_UnexpectedToken"] += 1
        return module_re_dict

    def module_dataobjects(self):
        module_do_dict = {}
        for filename in glob.glob(f"{self.code_path}/**.tf", recursive=True):
            if os.path.isdir(filename):
                pass
            else:
                with open(filename, "r", encoding="utf8") as file:
                    try:
                        tf_definition = hcl2.load(file)
                        if "data" in tf_definition.keys():
                            for key in tf_definition["data"][0].keys():
                                key = "do_" + key
                                if key in module_do_dict.keys():
                                    module_do_dict[key] += 1
                                else:
                                    module_do_dict[key] = 1
                    except:
                        pass
        for filename in glob.glob(f"{self.code_path}/modules/**/**.tf", recursive=True):
            if os.path.isdir(filename):
                pass
            else:
                with open(filename, "r", encoding="utf8") as file:
                    try:
                        tf_definition = hcl2.load(file)
                        if "data" in tf_definition.keys():
                            for key in tf_definition["data"][0].keys():
                                key = "do_" + key
                                if key in module_do_dict.keys():
                                    module_do_dict[key] += 1
                                else:
                                    module_do_dict[key] = 1
                    except:
                        pass
        return module_do_dict

    def count_tf_files(self):
        tf_files_count = 0
        for root, d_names, f_names in os.walk(self.path):
            for file in f_names:
                if file.endswith('.tf'):
                    tf_files_count += 1
        return tf_files_count


""" 
checks table columns:
    provider | offset | module_name | check_category | check_id | check_name | check_result (PASSED/FAILED) | 
    check_resource | module_id | company_name | description | downloads | namespace | published | is_verified |
    scan_time

summary table columns
    provider | offset | company name | module name | errors in scan | passed checks | failed checks | parsing errors | 
    module id | description | downloads | published | is verified | scan_time | tf_files

"""
