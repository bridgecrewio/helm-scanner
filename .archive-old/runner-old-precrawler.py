import datetime
import time
import os
from github import Github
from collections import defaultdict
from dateutil import rrule

from srchelm import result_writer
from srchelm import s3_uploader
from srchelm.checkov_wrapper import CheckovRun
from srchelm.slack_integrator import slack_manager

RESULTS_PATH = f'{os.path.abspath(os.path.curdir)}/results'
SCAN_TIME = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')


resultsDict = {}
starsDict = defaultdict(list)

try:
    GITHUB_TOKEN = os.environ['GITHUB_TOKEN']
except KeyError:
    GITHUB_TOKEN = config['GitHub']['Token']


def ratelimit_time_to_sleep(pygithub_future_time):
    gitHub_reset_in_seconds = datetime.datetime.fromtimestamp(pygithub_future_time).timestamp()
    current_time = datetime.datetime.now().timestamp()
    sleep_time = int(gitHub_reset_in_seconds-current_time)
    if sleep_time > 0:
        return(sleep_time + 10)
    else:
        return 0

def scan_files():
    count = 0
            # search_code(query, sort=NotSet, order=NotSet, highlight=False, **qualifiers)
            # Calls:	
            # GET /search/code

            # Parameters:	
            # query – string
            # sort – string (‘indexed’)
            # order – string (‘asc’, ‘desc’)
            # highlight – boolean (True, False)
            # qualifiers – keyword dict query qualifiers
            # Return type:	
            # github.PaginatedList.PaginatedList of github.ContentFile.ContentFile
            #
            # ContentFile
            # https://pygithub.readthedocs.io/en/latest/github_objects/ContentFile.html#github.ContentFile.ContentFile
            # https://docs.github.com/en/free-pro-team@latest/rest/reference/repos#contents
    
    # Maximum per page seems to be 100, doesn't hurt to set it higher.
    gitHub = Github(GITHUB_TOKEN, per_page=1000)
    github_query = 'filename:Chart.yaml apiVersion'
    
    print("CRAWL | Beginning github search by month from 2015-01 to workaround Github 1000 item search limit. ")
    crawl_start_date = datetime.datetime.strptime('2015-01-01', '%Y-%m-%d')
    crawl_end_date = datetime.datetime.now()

    #for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date):
    github_helm_charts = gitHub.search_code(github_query,"indexed","asc")

    # Issues.
        # 1. Github API will only return 1000 search results per search. People "time slicing" to get around this
        # 2. Github API per-min rate limits. Will need sleep/backoff logic for results this large.
    print(f'Found {github_helm_charts.totalCount} repo(s)')
 
    for contentFile in github_helm_charts:
        print("{} | Remaining requests: {}. Limit: {}".format(count,gitHub.rate_limiting[0],gitHub.rate_limiting[1]))
        if gitHub.rate_limiting[0] < 10:
            sleep_time = ratelimit_time_to_sleep(gitHub.rate_limiting_resettime)
            print("Sleeping for {} seconds due to rate limits.".format(sleep_time))
            time.sleep(sleep_time)
        # Object definition: https://pygithub.readthedocs.io/en/latest/github_objects/ContentFile.html
        # Check for Repo's already in dict (May have multiple results due to multiple charts in same repo).
        if contentFile.repository.clone_url not in resultsDict:
            print("{} | Adding Repo Name: {}. Stars: {}".format(count,contentFile.repository.name,contentFile.repository.stargazers_count))
            print("{} | Clone URL: {}. Chart path: {}".format(count,contentFile.repository.clone_url,contentFile.path))
            resultsDict[contentFile.repository.clone_url] = contentFile
            starsDict[contentFile.repository.stargazers_count].append(contentFile.repository.clone_url)
        else:
            print("{} | Repo already in results.".format(count))
        count = count + 1
    print(starsDict)

    checks_table = []
    summary_table = []
    all_resources = []
    empty_resources = {}
    all_dataobj = []
    # We dont need this provider, as we're not querying Terraform registry.

    for provider in ['aws', 'azure', 'azurerm', 'azuread', 'gcp', 'google', 'kubernetes']:
        print(f'Scanning {provider}')
        offset = 0
        while True:
            print(f"Scanning {provider}'s offset #{offset}")
            checkov_wrapper = CheckovRun(provider, RESULTS_PATH, offset)
            check_results, module_results, empty_scan_res = checkov_wrapper.checkov_scan(provider, offset, checkov_wrapper.code_url(),
                                                                                         SCAN_TIME)
            checks_table.extend(check_results)
            summary_table.append(module_results)
            for resource in empty_scan_res.keys():
                if resource == 'tf_UnexpectedToken':
                    continue
                resource_name = resource.replace('re_', '')
                if resource_name in empty_resources.keys():
                    empty_resources[resource_name] += empty_scan_res[resource]
                else:
                    empty_resources[resource_name] = empty_scan_res[resource]
            for resource in checkov_wrapper.module_resources().keys():
                if resource not in all_resources:
                    all_resources.append(resource)
            for dataobject in checkov_wrapper.module_dataobjects().keys():
                if dataobject not in all_dataobj:
                    all_dataobj.append(dataobject)
            checkov_wrapper.delete_registry_source_code()
            print(f"finished scanning {provider}'s offset #{offset}, module: {checkov_wrapper.name}")
            if checkov_wrapper.is_last_offset():
                print(f"all {offset} modules of {provider} were downloaded and scanned successfully")
                break
            offset += 1
    return checks_table, summary_table, all_resources, empty_resources, all_dataobj


def run():
    checks_table, summary_table, all_resources, empty_resources, all_dataobj = scan_files()
    result_writer.print_csv(summary_table, checks_table, empty_resources, RESULTS_PATH)
    #slack_manager.send_to_slack()
    if os.environ.get('RESULT_BUCKET'):
        print(f'Uploading results to {os.environ["RESULT_BUCKET"]}')
        s3_uploader.upload_results(RESULTS_PATH, SCAN_TIME)
