import json
import os
import logging as helmscanner_logging

import boto3

s3 = boto3.client('s3')


def upload_results_to_s3(results_path, scan_time):
    for filename in os.listdir(results_path):
        if filename.lower().endswith('.csv'):
            s3.upload_file(f'{results_path}/{filename}', os.environ['RESULT_BUCKET'], f'results/{scan_time}/{filename}')
            helmscanner_logging.info(f'Uploaded {filename}')



def upload_results(results_path, scan_time):
    upload_results_to_s3(results_path, scan_time)
