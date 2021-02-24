
"""
Credits & Licence
=================
The original source is released under the MIT license.
Copyright (c) 2019-2020 Mikhail Iailoian, Mubasher Chaudhary, Lasse Moench, Marian Assenmacher, Avishek Chatterjee
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

Original Source: https://github.com/leopart-hq/leopart/blob/723a4c11b61df19ee1ed67c320306b49761345d3/crawler.py

HELM GitHub Crawler
===================
Modified by: Matt Johnson <matt@bridgecrew.io> 
This crawler searches GitHub for repositories that contain CNCF Helm charts.
The crawler handles paging, API limit backoffs and works around the 1000 entry search limit by searching for relevant commits first using time slices.
Commits are then translated into a repo, and the repo checked for duplicates in a dictionary.
Repo details are stored, with a secondary dictionary indexed by GitHub stars.
"""
from github import Github, GithubException, UnknownObjectException
import time
from dateutil import rrule
import datetime
import calendar
from configparser import ConfigParser
from collections import defaultdict
import logging.handlers
import os
import pickle
from urllib.parse import urlparse

class GitHubCrawler:

    def __init__(self):
        config = ConfigParser()

        # check whether we have a custom config file
        if os.path.exists('config/crawler.config'):
            config.read('config/crawler.config')
        else:
            config.read('config/default_crawler.config')

        self.RATE_LIMIT_SAFETY = int(config['DEFAULT']['RATE_LIMIT_SAFETY'])
        try:
            self.GITHUB_TOKEN = os.environ['GITHUB_TOKEN']
        except KeyError:
            self.GITHUB_TOKEN = config['GitHub']['Token']

        self.config = config
        self.abuse_count = 0

        logger = logging.getLogger("GitHubCrawler")
        # Logger has to have 'lowest' Level. Nothing underneath this level is logged.
        logger.setLevel(logging.DEBUG)
        filehandler = logging.handlers.RotatingFileHandler(config['DEFAULT']['Logfile'], maxBytes=1024000, backupCount=1)

        filehandler.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        logger.addHandler(filehandler)
        logger.addHandler(ch)

        self.logger = logger

    # Very quickly returns saved pickle data for mocking a run
    def crawlMock(self, crawlStartDate: str):
        """
        This will iterate over all repositories on GitHub for our search terms. It uses slicing
        over the creation date of the repository to circumvent GitHubs 1000 items per response limit.
        """
        # Maximum per page seems to be 100, but it doesn't hurt to set it higher
        g = Github(self.GITHUB_TOKEN, per_page=1000)

        repoStatsEnabled = False
        self.resultsDict = {}
        self.starsDict = defaultdict(list)
        self.repoStatsDict = {}
        #crawlSearchTerms = "Chart.yaml apiVersion"
        crawlSearchTerms = "https://artifacthub.io/badge/repository"
        innerRepoSearchTerms ="filename:Chart.yaml apiVersion"
        total_num_of_results = 0
        num_of_repos_saved = 0
        num_of_duplicate_repos_discarded = 0

        try:
            with open('crawler_status_mock.pickle', 'rb') as f:
                crawler_status = pickle.load(f)
                # Begin from next (first non-complete) month. We may be adding a day too much here, but that shouldn't
                # matter as we only use year and month
                start_date = datetime.datetime.strptime(
                    crawler_status['last_month_completed'], '%Y-%m'
                ) + datetime.timedelta(days=31)
                total_num_of_results = crawler_status['total_num_of_results']
                num_of_repos_saved = crawler_status['num_of_repos_saved']
                self.logger.warning(f"LOADING MOCK DATA. {crawler_status['total_num_of_results']} search results checked for dupes of which "
                            f"{crawler_status['num_of_repos_saved']} repo's with helm charts found so far. NOT RESUMING LIVE SEARCH. "
                            f"{start_date.strftime('%Y-%m')}.")
                with open('results_dict_mock.pickle', 'rb') as f:
                    self.resultsDict = pickle.load(f)
                with open('stars_dict_mock.pickle', 'rb') as f:
                    self.starsDict = pickle.load(f)
                print(f'Total Loaded repos: {len(self.resultsDict)}')
        except IOError:
            self.logger.warning("No MOCK LOAD FILES FOUND, RETURNING EMPTY RESULTS DICT.")
        print(f'Items in dict: {len(self.resultsDict)}')
        return self.resultsDict,self.starsDict,crawlStartDate,total_num_of_results,num_of_repos_saved


    def crawl(self, crawlStartDate: str):
        """
        This will iterate over all repositories on GitHub for our search terms. It uses slicing
        over the creation date of the repository to circumvent GitHubs 1000 items per response limit.
        """
        # Maximum per page seems to be 100, but it doesn't hurt to set it higher
        g = Github(self.GITHUB_TOKEN, per_page=1000)

        repoStatsEnabled = False
        self.resultsDict = {}
        self.starsDict = defaultdict(list)
        self.repoStatsDict = {}
        crawlSearchTerms = "https://artifacthub.io/badge/repository"
        innerRepoSearchTerms ="filename:Chart.yaml apiVersion"
        total_num_of_results = 0
        num_of_repos_saved = 0
        num_of_duplicate_repos_discarded = 0

        try:
            with open('crawler_status.pickle', 'rb') as f:
                crawler_status = pickle.load(f)
                # Begin from next (first non-complete) month. We may be adding a day too much here, but that shouldn't
                # matter as we only use year and month
                start_date = datetime.datetime.strptime(
                    crawler_status['last_month_completed'], '%Y-%m'
                ) + datetime.timedelta(days=31)
                total_num_of_results = crawler_status['total_num_of_results']
                num_of_repos_saved = crawler_status['num_of_repos_saved']
                self.logger.warning(f"Found crawler status file. {crawler_status['total_num_of_results']} search results checked for dupes of which "
                            f"{crawler_status['num_of_repos_saved']} repo's with helm charts found so far. Resuming search from "
                            f"{start_date.strftime('%Y-%m')}.")
                with open('results_dict.pickle', 'rb') as f:
                    self.resultsDict = pickle.load(f)
                with open('stars_dict.pickle', 'rb') as f:
                    self.starsDict = pickle.load(f)
                print(f'Total Loaded repos: {len(self.resultsDict)}')
        except IOError:
            self.logger.warning("No crawler status file found, beginning search from {}".format(crawlStartDate))
            start_date = datetime.datetime.strptime(crawlStartDate, '%Y-%m-%d')

        self.logger.warning("Beginning NEW search from {}".format(crawlStartDate))
        start_date = datetime.datetime.strptime(crawlStartDate, '%Y-%m-%d')

        #end_date = datetime.datetime.now()
        end_date = datetime.date.fromisoformat('2019-12-30')


        try:
            # TODO: This has to be made more intelligent so that we can reduce the timespan if we get more than 1000 results
            for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date):
                thisTimeframeCount = 0
                # Find out last day of month
                last_day = calendar.monthrange(dt.year, dt.month)
                crawlTimeFrame = f'{dt.strftime("%Y-%m")}-01..{dt.strftime("%Y-%m")}-{last_day[1]}'

                self.logger.info(f'Crawling GitHub repositories between {dt.strftime("%Y-%m")}-01 and '
                            f'{dt.strftime("%Y-%m")}-{last_day[1]}')

                # Search code for helm Chart files
                # We can't do time slices on code_search, but we CAN on commit search, which is near enough! 
                # Although we can't do Filename: here, so hoping "Chart.yaml and "ApiVersion" is enough
                # We'll narrow down later
                self.__check_rate_limit(g, rl_limit=self.RATE_LIMIT_SAFETY, total_num_of_results=total_num_of_results,
                                                num_of_repos_saved=num_of_repos_saved, dt=dt)
                commitSearchResults = g.search_commits(f'{crawlSearchTerms} committer-date:{dt.strftime("%Y-%m")}-01..{dt.strftime("%Y-%m")}-{last_day[1]}')
    
                try:
                    if commitSearchResults.totalCount >= 1000:
                        self.logger.warning(f"Warning! More than 1000 results included for month {dt.strftime('%Y-%m')}. "
                                    f"We missed some repositories!")
                except Exception as e:
                    self.logger.warning(f"Exception accessing totalCount: {e}")

                try:
                    for commit in commitSearchResults:
                        self.__check_rate_limit(g, rl_limit=self.RATE_LIMIT_SAFETY, total_num_of_results=total_num_of_results,
                                                num_of_repos_saved=num_of_repos_saved, dt=dt)
                        
                        # We've used commits to get timeslices, but we need repo objects.
                        # build repo name using raw_data.html_url and request repo to give us clone URI
                        # Compare clone_uri against our dict, if we already have it, skip, no need to make more GH API calls.
                        commitUrl = commit.raw_data['html_url']
                        commitPath = urlparse(commitUrl).path
                        repo = commitPath.split("/")
                        self.logger.debug("{} | {} | Commit is part of: {}/{}".format(crawlTimeFrame, thisTimeframeCount,repo[1], repo[2]))

                        self.__check_rate_limit(g, rl_limit=self.RATE_LIMIT_SAFETY, total_num_of_results=total_num_of_results,
                                                        num_of_repos_saved=num_of_repos_saved, dt=dt)
                        repoObject = g.get_repo(f'{repo[1]}/{repo[2]}')
                    
                        # This is an unknown repo, check we actually have a Chart.yaml.
                        # Outside the if so we can count duplicate matches for the total_num_of_results in else.
                        self.__check_rate_limit(g, rl_limit=self.RATE_LIMIT_SAFETY, total_num_of_results=total_num_of_results,
                                                        num_of_repos_saved=num_of_repos_saved, dt=dt)
                        codeSearchResults = g.search_code( 
                                                f'{innerRepoSearchTerms} repo:{repo[1]}/{repo[2]}')
                        if repoObject.clone_url not in self.resultsDict:
                            # We only need the first result here, as searching a single repo at this point.
                            # If there is a single result, we have a valid Chart.yaml, add the repo to our dict and save a load of API transactions.
                            if codeSearchResults.totalCount > 0:
                                self.logger.debug("{} | {} | Adding Repo Name: {}. Stars: {}".format(crawlTimeFrame, thisTimeframeCount,codeSearchResults[0].repository.name,codeSearchResults[0].repository.stargazers_count))
                                self.logger.debug("{} | {} | Clone URL: {}. Chart path: {}".format(crawlTimeFrame, thisTimeframeCount,codeSearchResults[0].repository.clone_url,codeSearchResults[0].path))
                                # DICT FORMAT: KEY:CloneURL ITEM(LIST):[repository object, stargazers_count, watchers_count, repo description, forks_count, open_issues_count]
                                self.resultsDict[codeSearchResults[0].repository.clone_url] = [codeSearchResults[0].repository, codeSearchResults[0].repository.stargazers_count, codeSearchResults[0].repository.watchers_count, codeSearchResults[0].repository.description, codeSearchResults[0].repository.forks_count, codeSearchResults[0].repository.id, codeSearchResults[0].repository.open_issues_count]
                                self.starsDict[codeSearchResults[0].repository.stargazers_count].append(codeSearchResults[0].repository.clone_url)
                                num_of_repos_saved += 1
                                # Collect Repo clone,fork,download,refferrer stats
                                if repoStatsEnabled:
                                    repo = codeSearchResults[0].repository
                                    self.__collect_repo_stats(repo, total_num_of_results, num_of_repos_saved, dt)
                            else:
                                self.logger.debug("{} | {} | False positive in commit search: {}/{} Does not contain a valid Chart.yaml".format(crawlTimeFrame, thisTimeframeCount, repo[1], repo[2]))
                        
                        #Update counter of "total results processed" based on all the valid search results we've skipped
                        total_num_of_results += codeSearchResults.totalCount
                        #Logging counter
                        thisTimeframeCount += 1
        
                except GithubException as e:
                    self.__handle_github_exception(e)
                    pass

                self.logger.info(f"Reached the end of month {dt.strftime('%Y-%m')}. Saving data to results_dict.pickle and stars_dict.pickle and status to "
                            f"crawler_status.pickle. Restarting the crawler will resume from this position.")
                self.__save_list_of_repos(self.resultsDict, self.starsDict, total_num_of_results, num_of_repos_saved, dt)

            self.logger.info('Reached current date. Stopping and writing to file...')
            self.__save_list_of_repos(self.resultsDict, self.starsDict, total_num_of_results, num_of_repos_saved, dt)
        except KeyboardInterrupt:
            self.logger.warning("Received Keyboard interrupt, stopping...")
        print(f'Items in dict: {len(self.resultsDict)}')
        return self.resultsDict,self.starsDict,crawlStartDate,total_num_of_results,num_of_repos_saved

    def __check_rate_limit(self, g, rl_limit, total_num_of_results, num_of_repos_saved, dt):
        """
        Checks the current rate limit at GitHub and pauses if necessary to not exhaust the ratelimit
        :param g: the GitHub object that contains meta info, e.g. rate limit, rate limit resettime
        :param rl_limit: Contains the rate limit safety margin (so we don't get blocked for abuse)
        :param total_num_of_results: The number of search results processed so far
        :param num_of_repos_saved: The number non duplicate helm-containing repo's found
        :param dt: The month that is currently being crawled
        """
        rate_limit_remaining = g.rate_limiting[0]
        rate_limit_reset_time = g.rate_limiting_resettime
        self.logger.debug(f'Rate limit remaining: {rate_limit_remaining}')

        if rate_limit_remaining < rl_limit:
            self.logger.info(f"Rate limit is reached, pausing until rate limit is refreshed, which will be at "
                        f"{datetime.datetime.fromtimestamp(rate_limit_reset_time)}.")
            self.logger.info(f"Status: Searched {total_num_of_results} results and saved {num_of_repos_saved} unique repo's for processing."
                        f"Currently searching {dt.strftime('%Y-%m')}")
            self.__update_status(total_num_of_results, num_of_repos_saved, dt)
            self.logger.warning(f"Ratelimit pause until {datetime.datetime.fromtimestamp(rate_limit_reset_time + 5)}")
            time.sleep(rate_limit_reset_time - time.time() + 5)
        else:
            self.__update_status(total_num_of_results, num_of_repos_saved, dt)


    def __update_status(self, total_num_of_results, num_of_repos_saved, dt):
        """
        Print logging output
        :param total_num_of_results: The number of search results processed so far
        :param num_of_repos_saved: The number non duplicate helm-containing repo's found
        :param dt: The month that is currently being crawled
        """
        self.logger.debug(f"Search results processed: {total_num_of_results}")
        self.logger.debug(f"Unique repos saved: {num_of_repos_saved}")
        self.logger.debug(f"Currently crawling {dt.strftime('%Y-%m')}")
        if self.abuse_count > 0:
            self.logger.warning(f"Abuse limit count: {self.abuse_count}")


    def __save_list_of_repos(self, resultsDict, starsDict, total_num_of_results, num_of_repos_saved, dt):
        """
        Save unique repositories that have been crawled, create a status dict so that the crawling can be continued if it
        is cancelled in the meantime. This method is only called at the end of each month-slice, to allow for easy
        continuation (just start with the next month)
        :param resultsDict: A dict of repo searched objects, keyed by the repo's clone url.
        :param starsDict: A dict categorising repo's by stars, key is stars, contains list of clone URL's to be used with resultsDict
        :param total_num_of_results: Total number of search results processed
        :param num_of_repos_saved: Number of unique repo's saved
        :param dt: The month that is currently being crawled
        """
        self.logger.info(f"Crawling finished for month {dt.strftime('%Y-%m')}")
        self.logger.info(f"Search results crawled (total): {total_num_of_results}. Unique repos found (total): {num_of_repos_saved}")
        with open('crawler_status.pickle', 'wb') as f:
            status_dict = {
                'last_month_completed': dt.strftime('%Y-%m'),
                'total_num_of_results': total_num_of_results,
                'num_of_repos_saved': num_of_repos_saved
            }
            pickle.dump(status_dict, f, pickle.HIGHEST_PROTOCOL)

        with open('results_dict.pickle', 'wb') as f:
            pickle.dump(resultsDict, f, pickle.HIGHEST_PROTOCOL)

        with open('stars_dict.pickle', 'wb') as f:
            pickle.dump(starsDict, f, pickle.HIGHEST_PROTOCOL)

    def __collect_repo_stats(self, repo, total_num_of_results, num_of_repos_saved, dt):
        try:
            self.__check_rate_limit(g, rl_limit=self.RATE_LIMIT_SAFETY, total_num_of_results=total_num_of_results,
                        num_of_repos_saved=num_of_repos_saved, dt=dt)
            repoCloneWeekly = repo.get_clones_traffic()
            self.__check_rate_limit(g, rl_limit=self.RATE_LIMIT_SAFETY, total_num_of_results=total_num_of_results,
                        num_of_repos_saved=num_of_repos_saved, dt=dt)
            repoViewsWeekly = repo.get_views_traffic()
            self.__check_rate_limit(g, rl_limit=self.RATE_LIMIT_SAFETY, total_num_of_results=total_num_of_results,
                        num_of_repos_saved=num_of_repos_saved, dt=dt)
            repoReferrersFortnight = repo.get_top_referrers()
            self.__check_rate_limit(g, rl_limit=self.RATE_LIMIT_SAFETY, total_num_of_results=total_num_of_results,
                        num_of_repos_saved=num_of_repos_saved, dt=dt)
            repoPopularContentFortnight = repo.get_top_paths() 
            self.repoStatsDict[repo.clone_url].append([repoCloneWeekly,repoViewsWeekly,repoReferrersFortnight,repoPopularContentFortnight])

        except GithubException as e:
            self.__handle_github_exception(e)
            pass
    def __handle_github_exception(self, e):
        """
        Check if exception was thrown due to an abuse detection. If so, wait 5 minutes.
        :param e: The error message
        """
        self.logger.debug(f"GithubException: {e}")
        if e.data["message"].startswith("You have triggered an abuse detection mechanism."):
            self.abuse_count += 1
            self.logger.warning(f"Abuse rate limit pause until {datetime.datetime.fromtimestamp(time.time() + 300)}, "
                        f"abuse count: {self.abuse_count}")
            time.sleep(300)