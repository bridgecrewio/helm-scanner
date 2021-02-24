# Infrastructure as Code Security Scanner for Helm Hubs

## What is it?
This is a work-in-progress codebase designed to automate discovering, security scanning, and providing easy access to the results for publically available Helm charts.

Currently, the scanner enumerates all Helm charts from repositories listed in [https://artifacthub.io](https://artifacthub.io), downloads the latest version of each and scans them with the Checkov static analysis tool (against Chekov's built in Kubernetes checks).

The results are then outputted as CSV's for individual checks and multiple summary formats. We are using these at present to feed AWS Quicksight, with a view to moving this data from regular scans into an API.



