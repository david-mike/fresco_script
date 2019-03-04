#!/bin/bash

##############################################################################################
#Description: this scrip is to automate processing of hpc accouting data with py script
#Author: Jizhou Chen JZChen@purdue.edu
#Last modified: 1/25/2019
##############################################################################################

ACCT_FILE_LIST=$(ls ./fresco/accounting/)

for ACCT_FILE in $ACCT_FILE_LIST; do
	python3.7 JobFilter.py $ACCT_FILE
	#break
done

touch ACCT_PROCESS_DONE
