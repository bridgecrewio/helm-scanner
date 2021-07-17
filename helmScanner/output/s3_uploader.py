import json
import os
import logging as helmscanner_logging

import boto3

s3 = boto3.client('s3')


def upload_results_to_s3(results_path, scan_time, partialUpload):
    for filename in os.listdir(results_path):
        helmscanner_logging.info(f'Found file: {filename}')
        if filename.lower().endswith('.csv'):
            helmscanner_logging.info(f'Uploading file: {filename}')
            try: 
              s3.upload_file(f'{results_path}/{filename}', os.environ['RESULT_BUCKET'], f'results/{scan_time}/{filename}')
              helmscanner_logging.info(f'Uploaded {filename}')
              if partialUpload:
                helmscanner_logging.info(f'Partial upload selected, renaming {filename} to {filename}.uploaded')
                os.rename(filename, f"{filename}.uploaded")
            except Exception as e:
              helmscanner_logging.error(f'Failed to upload via boto3. Error was: {e}')


def upload_results(results_path, scan_time, partialUpload):
    upload_results_to_s3(results_path, scan_time, partialUpload)
