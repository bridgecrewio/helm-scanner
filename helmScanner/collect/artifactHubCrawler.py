
"""
ArtifactHub.io HELM Crawler v0.4
================================
Matt Johnson <matt@bridgecrew.io> 
Steve Giguere <eurogig@gmail.com>

:env ARTIFACTHUB_TOKEN: API token from artifacthub.io
:env ARTIFACTHUB_TOKEN_SECRET: API secret from artifacthub.io
"""

import logging as helmscanner_logging
import logging.handlers
import os
import pickle
from urllib.parse import urlparse

#ArtifacrHubCrawler Imports
import requests
from requests.exceptions import HTTPError

class ArtifactHubCrawler:

    def __init__(self):

        logfile = "./artifacthub-crawler.log"

        # Logging Setup
        logger = helmscanner_logging.getLogger()
        logger.setLevel(helmscanner_logging.INFO)
        filehandler = helmscanner_logging.handlers.RotatingFileHandler(logfile, maxBytes=1024000, backupCount=1)
        filehandler.setLevel(helmscanner_logging.INFO)
        ch = helmscanner_logging.StreamHandler()
        ch.setLevel(helmscanner_logging.INFO)
        logger.addHandler(filehandler)
        logger.addHandler(ch)
        self.logger = logger

        try:
            self.ARTIFACTHUB_TOKEN = os.environ['ARTIFACTHUB_TOKEN']
        except KeyError:
            logger.warning("No env ARTIFACTHUB_TOKEN found")
            exit()

        try:
            self.ARTIFACTHUB_TOKEN_SECRET = os.environ['ARTIFACTHUB_TOKEN_SECRET']
        except KeyError:
            logger.warning("No env ARTIFACTHUB_TOKEN_SECRET found")
            exit()

    def crawl(self):
        """
        crawl uses the HELM search functioanlity of artifacthub.io to find all helm *repositories* which may contain multiple charts.
        It then queries each repository to find charts, and uses the direct download link for each chart to get the latest .tgz.
        The chart is extracted and location recorded.
        Testing/Debugging: We also then dump the dictionary to a pickle file: artifactHubCrawler.crawl.pickle, which was historically useful for inspecting the data post-run.
        
        :return crawlDict: A dictionary of crawled and discovered HELM charts, their repo details, and the local filesystem tmpdir location of the extracted chart.
        :return totalRepos: Integer stat of total repo's discovered 
        :return totalPackages: Integer stat of total Chart's within all discovered repo's 

        """
        crawlDict = {}
        totalPackages = 0
        reposPerRequest = 60
        start_record = os.environ.get('START_RECORD',default=0)
        max_records = os.environ.get('MAX_RECORDS',default=250)
        helmscanner_logging.info("Artifacthub Helm crawler started.")
        try:
            currentRepo = 0
            headers = {'X-API-KEY-ID': self.ARTIFACTHUB_TOKEN, 'X-API-KEY-SECRET': self.ARTIFACTHUB_TOKEN_SECRET}
            helmscanner_logging.info("Receiving latest ArtifactHub repo results.")
            response = requests.get(f"https://artifacthub.io/api/v1/repositories/search?offset={start_record}&limit={reposPerRequest}&kind=0", headers=headers)
            response.raise_for_status()
            maxRepos = int(response.headers["pagination-total-count"]) if max_records > int(response.headers["pagination-total-count"]) else max_records
            self.logger.info(f"Found max repos {maxRepos}")
            jsonResponse = response.json()
            totalRepos = len(jsonResponse)
            offset = start_record + reposPerRequest
            while (maxRepos > totalRepos):
                # Get the rest of the repos
                response = requests.get(f"https://artifacthub.io/api/v1/repositories/search?offset={offset}&limit={reposPerRequest}&kind=0", headers=headers)
                response.raise_for_status()
                jsonResponse += response.json()
                totalRepos += len(response.json())
                offset += reposPerRequest

            self.logger.info(f"Found {totalRepos} Helm repositories.")
            for repoResult in jsonResponse:
                thisRepoDict = {}
                currentRepo += 1
                try:
                    repoOrgName = repoResult['organization_name']
                except:
                    repoOrgName = repoResult['user_alias']
                try:
                    # Packages within a repo
                    self.logger.info(f"{currentRepo}/{totalRepos} | Processing Repo {repoResult['name']} by {repoOrgName}")
                    packagesQueryURI = f"https://artifacthub.io/api/v1/packages/search?limit=60&facets=false&kind=0&repo={repoResult['name']}"
                    response = requests.get(packagesQueryURI, headers=headers)
                    chartPackages = response.json()
                    chartPackagesInRepo = len(chartPackages['packages'])
                    self.logger.debug(f"{currentRepo}/{totalRepos} | found {chartPackagesInRepo} packages.")
                    thisRepoDict = {"repoName": repoResult['name'], "repoOrgName": repoOrgName, "repoCrawlResultsID": currentRepo, "repoTotalPackages": chartPackagesInRepo, "repoRaw": repoResult, "repoPackages": [] }
                    currentChartPackage = 0
                    for chartPackage in chartPackages['packages']:
                        currentChartPackage += 1
                        totalPackages +=1
                        try:
                            # Downloads and package version details for each package.
                            response = requests.get(f"https://artifacthub.io/api/v1/packages/helm/{repoResult['name']}/{chartPackage['name']}", headers=headers)
                            chartVersionResponse = response.json()
                            self.logger.debug(f"        R: {currentRepo}/{totalRepos} | P: {currentChartPackage}/{chartPackagesInRepo} | Chart {chartPackage['name']} latest version: {chartVersionResponse['version']} URL: {chartVersionResponse['content_url']}")
                            thisRepoDict['repoPackages'].append(chartVersionResponse)
                        except HTTPError as http_err:
                           helmscanner_logging.warning(f'HTTP error occurred: {http_err}')
                        except Exception as err:
                            helmscanner_logging.warning(f'Other error occurred: {err}')
                except HTTPError as http_err:
                    helmscanner_logging.warning(f'HTTP error occurred: {http_err}')
                except Exception as err:
                    helmscanner_logging.warning(f'Other error occurred: {err}')
                #Save this repo's packages into our main crawler dict.
                crawlDict[currentRepo] = thisRepoDict
        except HTTPError as http_err:
            helmscanner_logging.warning(f'HTTP error occurred: {http_err}')
        except Exception as err:
            helmscanner_logging.warning(f'Other error occurred: {err}')
        with open('artifactHubCrawler.crawl.pickle', 'wb') as f:
            pickle.dump(crawlDict, f, pickle.HIGHEST_PROTOCOL)
        return crawlDict, totalRepos, totalPackages 

    def mockCrawl(self):
        """
        Loads in the latest pickled dict produced by crawl() from artifactHubCrawler.crawl.pickle' and returns the dictionary.
        
        :return crawlDict: A dictionary of crawled and discovered HELM charts (Loaded from a pickle file) their repo details, and the local filesystem tmpdir location of the extracted chart.
        :return totalRepos: Always zero for the mockCrawl()
        :return totalPackages: Always zero for the mockCrawl()
        """
        crawlDict = {}
        with open('artifactHubCrawler.crawl.pickle', 'rb') as f:
            crawlDict = pickle.load(f)
        return crawlDict, 0, 0 