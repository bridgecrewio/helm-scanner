import pandas as pd

def print_csv(sum_table, chk_table, helmdeps_lst, empty_resources, path, repo, orgRepoFilename, globaldepslist, globaldepsusage):
    # create checks table:
    checks_frame = pd.DataFrame(chk_table, columns=[ 'runner timestamp', 'combined name', "repository name", "package name", "package latest version",
                                                    'package created at', 'package is signed', 'security report created timestamp', 'helm chart', 'resource is operator',
                                                    'check catagory', 'check id', 'check name', 'check result', 'file path', "check class", "resource id", "repository id", "repository digest", "repository tracking ts", "repository verified", "repository official", "repository scanning disbled" ])
    checks_frame.to_csv(f'{path}/checks-table-{orgRepoFilename}.csv')
    

    # create summary table:
    summary_frame = pd.DataFrame(sum_table, columns=[ 'runner timestamp', 'combined name', "repository name", "package name", "package latest version",
                                                     'package created at', 'package is signed', 'security report created timestamp', 'helm chart', 'resource is operator', 'scan status', 
                                                     'passed checks', 'failed checks', 'parsing errors' ])
    summary_frame.to_csv(f'{path}/summarytable-{orgRepoFilename}.csv')


    # Chart Deps
                #  dep_item = [
                #             repoChartPathName, #Current chart combined repo/path
                #             repo['repoName'],  #Current chart reponame
                #             chartPackage['name'], #Current chart chartname
                #             chartPackage['version'], #Current chart version
                #             current_dep.values()[0], #dep dict chart_name
                #             current_dep.values()[1], #dep dict chart_version
                #             current_dep.values()[2], #dep dict chart_repo
                #             current_dep.values()[3]  #dep dict chart_status
                #         ]

    chart_deps_frame = pd.DataFrame(helmdeps_lst, columns=[ 'runner timestamp', 'combined name', "repository name", "package name", "package latest version",
                                                     'dep helm chart', 'dep helm version', 'dep repo', 'dep chart status' ])
    chart_deps_frame.to_csv(f'{path}/deps-table-{orgRepoFilename}.csv')

    # Global deps statistics
    global_deps_stats = pd.DataFrame.from_dict(globaldepsusage, orient='index')
    global_deps_stats.to_csv(f'{path}/global-deps-table.csv')

    global_deps_list = pd.DataFrame.from_dict(globaldepslist, orient='index')
    global_deps_list = global_deps_list.transpose()
    global_deps_list.to_csv(f'{path}/global-deps-list.csv')