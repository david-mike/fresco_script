#!/usr/local/bin/python3.5
###################################################################################################
# Description: this script is to filter out jobs in accessible queues that have excited and don't
#              have runtime overlap with any other jobs on a same node.
#
# Environment Requirement: by default, a TORQUE PBS Accounting file named 'log.csv' should be
#                          present and an output file named 'filtered.csv' will be generated in the
#                          same directory as this script is located in, or users may change the
#                          paths of the source file and output file at the beginning of the code)
#
# Author: Jizhou Chen - JZChen@purdue.edu
#
# Last Modified: 03/03/2019
####################################################################################################
import csv
import sys

# must use python3.6 and above because lower versions will mess up the order of data stored in dic

# following file paths can be modified if needed
accounting_file = "accounting-filtered/" + sys.argv[1]
performance_file = "./fresco/performance/" + sys.argv[2]
outputFile = "performance-filtered/" + sys.argv[2][:sys.argv[2].index('.')] + "-filtered.csv"

# add header
with open(performance_file) as performance_csv:
        header = performance_csv.readline()

with open(outputFile,'w') as output_csv:
        output_csv.write(header)

job_set = set()

with open(accounting_file, 'r') as unfilteredList:
        csvReader = csv.DictReader(unfilteredList)

        for job in csvReader:
                job_set.add(job['jobID'])

with open(performance_file, 'r') as performanceList:
        cr = csv.DictReader(performanceList)
        headers = cr.fieldnames
        with open(outputFile, 'a') as output:
                #fieldnames = ['jobID', 'phead', 'aa', 'aaa']
                csvWriter = csv.DictWriter(output, headers, delimiter = ',')
                for job in cr:
                        if job['jobID'] in job_set:
                                #print(job)
                                csvWriter.writerow(job)
print (outputFile+" done!\n")
