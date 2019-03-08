#!/bin/bash

FOLDER_LIST=$(ls /usr/local/hpc/performance-filtered/)

for FOLDER in $FOLDER_LIST; do	
	mkdir /usr/local/hpc/performance-filtered-no-prefix/$FOLDER	
	FILE_LIST=$(ls /usr/local/hpc/performance-filtered/$FOLDER)
	for FILE in $FILE_LIST; do
		sed -e '/[0-9]/s/jobID//g' -e '/[0-9]/s/NODE//g' -e '/[0-9]/s/DEVICE//g' /usr/local/hpc/performance-filtered/$FOLDER/$FILE > /usr/local/hpc/performance-filtered-no-prefix/$FOLDER/$FILE
		echo "/usr/local/hpc/performance-filtered-no-prefix/$FOLDER/$FILE is done !\n"
	done
done

touch no-prefix-done
		


