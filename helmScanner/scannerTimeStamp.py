"""
Timestamp for helm-scanner
==========================

Creates a timestamp at init, always gives back the same timestamp within a given run.
Used for output of S3 directory structure, and inside CSV's outputs where records 
need assigning to a given run of helm-scanner.

"""

from datetime import datetime

helmScannerArtifactTimestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")

def currentRunTimestamp():
    return helmScannerArtifactTimestamp

currentRunTimestamp = currentRunTimestamp()