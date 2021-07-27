import json
import os
import logging as helmscanner_logging

import boto3

s3 = boto3.client('s3')


def upload_results_to_s3(results_path, scan_time, partialUpload):
    subdirList = ['checks','summaries','deps','containers','container_summaries']
    for subdir in subdirList:
        for filename in os.listdir(f"{results_path}/{subdir}"):
            helmscanner_logging.info(f'Found file: {subdir}/{filename}')
            if filename.lower().endswith('.csv'):
                helmscanner_logging.info(f'Uploading file: {subdir}/{filename}')
                try: 
                    s3.upload_file(f'{results_path}/{subdir}/{filename}', os.environ['RESULT_BUCKET'], f'results/{scan_time}/{subdir}/{filename}')
                    helmscanner_logging.info(f'Uploaded {subdir}/{filename}')
                    if partialUpload:
                        helmscanner_logging.info(f'Partial upload selected, renaming {subdir}/{filename} to {subdir}/{filename}.uploaded')
                        os.rename(filename, f"{filename}.uploaded")
                except Exception as e:
                    helmscanner_logging.error(f'Failed to upload via boto3. Error was: {e}')
    for filename in os.listdir(f"{results_path}/dockerfiles"):
        if filename.lower().endswith('.Dockerfile'):
                helmscanner_logging.info(f'Uploading file: dockerfiles/{filename}')
                try: 
                    s3.upload_file(f'{results_path}/dockerfiles/{filename}', os.environ['RESULT_BUCKET'], f'results/{scan_time}/dockerfiles/{filename}')
                    helmscanner_logging.info(f'Uploaded {subdir}/{filename}')
                    if partialUpload:
                        helmscanner_logging.info(f'Partial upload selected, renaming dockerfiles/{filename} to dockerfiles/{filename}.uploaded')
                        os.rename(filename, f"{filename}.uploaded")
                except Exception as e:
                    helmscanner_logging.error(f'Failed to upload via boto3. Error was: {e}')


def upload_results(results_path, scan_time, partialUpload):
    upload_results_to_s3(results_path, scan_time, partialUpload)
