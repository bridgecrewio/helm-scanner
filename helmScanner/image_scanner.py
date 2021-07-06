#!/usr/bin/python3

from sys import argv

import csv
import docker 
import os
import stat
import platform
import requests
from datetime import datetime, timedelta
import subprocess  # nosec
import json
import logging
from multithreader import multithreadit

# Get magic from checkov to build the headers
from checkov.common.util.dict_utils import merge_dicts
from checkov.common.util.http_utils import get_auth_header, get_default_get_headers, get_default_post_headers

TWISTCLI_FILE_NAME = 'twistcli'
DOCKER_IMAGE_SCAN_RESULT_FILE_NAME = 'docker-image-scan-results.json'
BC_API_URL = "https://www.bridgecrew.cloud/api/v1"
BC_API_KEY = ""
BC_SOURCE = "helm-scanner"


class ImageScanner():
    def __init__(self):
        #super(ImageScanner, self).__init__()
        self.cmds = []
        self.cli = docker.from_env()
        docker_image_scanning_base_url = f"{BC_API_URL}/vulnerabilities/docker-images"
        self.docker_image_scanning_proxy_address=f"{docker_image_scanning_base_url}/twistcli/proxy"
        self.download_twistcli(TWISTCLI_FILE_NAME,docker_image_scanning_base_url)
        try:
            BC_API_KEY = self.ARTIFACTHUB_TOKEN_SECRET = os.environ['BC_API_KEY']
        except KeyError:
            logging.warning("No env BC_API_KEY found")
            exit()

    def _scan_image(self, helmRepo, docker_image_id): 

        docker_cli = docker.from_env()
        try:
            img = docker_cli.images.get(docker_image_id)
        except:
            logging.info("Not found locally so pulling...")
            try:
                [image,tag]=docker_image_id.split(':')
                logging.info("Pulling {0}:{1}".format(image,tag))
                img = docker_cli.images.pull(image,tag)
            except:
                logging.info(f"Can't pull image {docker_image_id}")
                return
        # Create Dockerfile.  Only required for platform reporting
        hist = img.history()
        cmds = self._parse_history(hist)
        cmds.reverse()
        self._save_dockerfile(cmds)

        DOCKER_IMAGE_SCAN_RESULT_FILE_NAME = f".{img.id}.json"
        command_args = f"./{TWISTCLI_FILE_NAME} images scan --address {self.docker_image_scanning_proxy_address} --token b0e5278a-d2c3-5685-a9a1-5d114780526e --details --output-file {DOCKER_IMAGE_SCAN_RESULT_FILE_NAME} {docker_image_id}".split()
        logging.info("Running scan")
        logging.info(command_args)
        subprocess.run(command_args)  # nosec
        logging.info(f'TwistCLI ran successfully on image {docker_image_id}')
        # if twistcli worked our json file should be there
        if os.path.isfile(DOCKER_IMAGE_SCAN_RESULT_FILE_NAME):
            with open(DOCKER_IMAGE_SCAN_RESULT_FILE_NAME) as docker_image_scan_result_file:
                self.parse_results(helmRepo, docker_image_id, img.id,json.load(docker_image_scan_result_file)) 
            os.remove(DOCKER_IMAGE_SCAN_RESULT_FILE_NAME)

    def _scan_images(self, helmRepo, imageList): 

        multithreadit(self._scan_image, helmRepo, imageList)
        return
        for docker_image_id in imageList:
            try:
                self.img = self.cli.images.get(docker_image_id)
            except:
                logging.info("Not found locally so pulling...")
                try:
                    [image,tag]=docker_image_id.split(':')
                    logging.info("Pulling {0}:{1}".format(image,tag))
                    self.img = self.cli.images.pull(image,tag)
                except:
                    logging.info(f"Can't pull image {docker_image_id}")
                    return
            # Create Dockerfile.  Only required for platform reporting
            self.hist = self.img.history()
            self._parse_history()
            self.cmds.reverse()
            self._save_dockerfile()

            command_args = f"./{TWISTCLI_FILE_NAME} images scan --address {self.docker_image_scanning_proxy_address} --token b0e5278a-d2c3-5685-a9a1-5d114780526e --details --output-file {DOCKER_IMAGE_SCAN_RESULT_FILE_NAME} {docker_image_id}".split()
            logging.info("Running scan")
            logging.info(command_args)
            subprocess.run(command_args)  # nosec
            logging.info(f'TwistCLI ran successfully on image {docker_image_id}')
            with open(DOCKER_IMAGE_SCAN_RESULT_FILE_NAME) as docker_image_scan_result_file:
                self.parse_results(helmRepo, docker_image_id, img.id,json.load(docker_image_scan_result_file)) 


    def _save_dockerfile(self,cmds):
        file = open(".BCDockerfile","w")
        for i in cmds:
            file.write(i)
        file.close()

    def _parse_history(self, hist, rec=False):
        first_tag = False
        actual_tag = False
        cmds = []
        for i in hist:
            if i['Tags']:
                actual_tag = i['Tags'][0]
                if first_tag and not rec:
                    break
                first_tag = True
            step = i['CreatedBy']
            if "#(nop)" in step:
                to_add = step.split("#(nop) ")[1]
            else:
                to_add = ("RUN {}".format(step))
            to_add = to_add.replace("&&", "\\\n    &&")
            cmds.append(to_add.strip(' '))
        if not rec:
            cmds.append("IMAGE {}".format(actual_tag))
        return cmds

    def download_twistcli(self, cli_file_name, docker_image_scanning_base_url):
        if os.path.exists(cli_file_name):
            return
        os_type = platform.system().lower()
        headers = merge_dicts(
            get_default_get_headers(BC_SOURCE, "HELM_SCANNER"),
            get_auth_header(BC_API_KEY)
        )
        response = requests.request('GET', f"{docker_image_scanning_base_url}/twistcli/download?os={os_type}", headers=headers)
        open(cli_file_name, 'wb').write(response.content)
        st = os.stat(cli_file_name)
        os.chmod(cli_file_name, st.st_mode | stat.S_IEXEC)
        logging.info(f'TwistCLI downloaded and has execute permission')

    def parse_results(self, helmRepo, docker_image_name, image_id, twistcli_scan_result):
        headerRow = ['combined name','Image Name','Image Tag','Total', 'Critical', 'High', 'Medium','Low']  
        filenameVulns = f"results/{image_id}.csv"
        filenameSummary = f"results/{image_id}_summary.csv"
        [imageName,imageTag] = docker_image_name.split(':')
        # Create Summary
        with open(filenameSummary, 'w') as f: 
            write = csv.writer(f) 
            write.writerow(headerRow) 
            row = [
                helmRepo,
                imageName,
                imageTag,
                twistcli_scan_result['results'][0]['vulnerabilityDistribution']['total'],
                twistcli_scan_result['results'][0]['vulnerabilityDistribution']['critical'],
                twistcli_scan_result['results'][0]['vulnerabilityDistribution']['high'],
                twistcli_scan_result['results'][0]['vulnerabilityDistribution']['medium'],
                twistcli_scan_result['results'][0]['vulnerabilityDistribution']['low'] ]
            write.writerow(row) 
        # Create Vulns Doc (if required)  
        if twistcli_scan_result['results'][0]['vulnerabilityDistribution']['total'] > 0:
            headerRow = ['combined name','Image Name','Image Tag','CVE ID', 'Status', 'Severity', 'Package Name','Package Version','Link','CVSS','Vector','Description','Risk Factors','Publish Date']           
            with open(filenameVulns, 'w') as f: 
                write = csv.writer(f) 
                write.writerow(headerRow) 
                for x in twistcli_scan_result['results'][0]['vulnerabilities']:
                    try:
                        link = x['link']
                    except:
                        link = ''
                    row = [
                        helmRepo,
                        imageName,
                        imageTag,
                        x['id'],
                        x.get('status', 'open'),
                        x['severity'],
                        x['packageName'],
                        x['packageVersion'],
                        link,
                        x.get('cvss'),
                        x.get('vector'),
                        x.get('description'),
                        x.get('riskFactors'),
                        (datetime.now() - timedelta(days=x.get('publishedDays', 0))).isoformat() ]
                    write.writerow(row) 



imageScanner = ImageScanner()
