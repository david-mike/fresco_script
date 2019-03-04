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
from datetime import datetime

# must use python3.6 and above because lower versions will mess up the order of data stored in dic

# following file paths can be modified if needed
inputFile = "fresco/accounting/" + sys.argv[1]
outputFile = "accounting-filtered/" + sys.argv[1][:sys.argv[1].index('.')] + "-filtered.csv"
availableQueues = ['QUEUE2']
exitedJobs = {}     # key: jobID, values: record for each job demonstrated in log file
jobsOnNode = {}     # key: string (node name), value: list ( list of tuples(jobId, start, end) )


def time_parse(time):
    return datetime.strptime(time, '%m/%d/%Y %H:%M:%S')



# add header
with open(inputFile) as input_csv:
    header = input_csv.readline()

with open(outputFile,'w') as output_csv:
    output_csv.write(header)

with open(inputFile, 'r') as unfilteredList:
    csvReader = csv.DictReader(unfilteredList)

    for job in csvReader:
        if (job['queue'] in availableQueues) and (job['jobevent'] == 'E'):      # filter out successfully exited jobs with Exit_status of 0
            exitedJobs[job['jobID']] = (job.values())       # add jobs to exitedJobs{} dictionary

            tempNodeStartIndex = 0
            while tempNodeStartIndex != -1:
                tempNodeEndIndex = job['exec_host'].find("/", tempNodeStartIndex)     # a node string ends by '/'
                tempNode = job['exec_host'][tempNodeStartIndex: tempNodeEndIndex]     # find a new node residing in exec_host string

                # if tempNode is not in jobOnNode dictionary, create one
                jobStartTime = time_parse(job['start'])
                jobEndTime = time_parse(job['end'])
                if tempNode not in jobsOnNode:
                    jobsOnNode[tempNode] = [(job['jobID'], jobStartTime, jobEndTime)]

                # if tempNode is in jobOnNode dictionary but current job is not in tempNode, append current job to corresponding tempNode
                elif (job['jobID'], jobStartTime, jobEndTime) not in jobsOnNode[tempNode]:
                    jobsOnNode[tempNode].append((job['jobID'], jobStartTime, jobEndTime))

                tempNodeStartIndex = job['exec_host'].find("NODE", tempNodeEndIndex)

    for node in jobsOnNode:
        jobsOnNode[node] = sorted(jobsOnNode[node], key=lambda time: (time[1], time[2]))        # primary sort: job start time, secondary sort: job end time
        jobNumOnCurrentNode = len(jobsOnNode[node])

        # delete jobs with runtime overlap
        for currentIndex in range(jobNumOnCurrentNode - 1):
            nextIndex = currentIndex + 1
            while (nextIndex < jobNumOnCurrentNode) and (jobsOnNode[node][nextIndex][1] <= jobsOnNode[node][currentIndex][2]):
                exitedJobs.pop(jobsOnNode[node][currentIndex][0], None)
                exitedJobs.pop(jobsOnNode[node][nextIndex][0], None)
                nextIndex += 1
    with open(outputFile, 'a') as filteredList:
        csvWriter = csv.writer(filteredList)

        for job in exitedJobs:
            csvWriter.writerow(exitedJobs[job])
			
print (outputFile+" done!\n")
