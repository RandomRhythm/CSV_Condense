# coding: utf-8
from __future__ import print_function
import csv
import os
import operator
import itertools
import time
import datetime
from collections import defaultdict
import sys
import io


dictTrack = dict()
dictFirstL = dict()
dictLastL = dict()
dictOutput = dict()

maxInt = 100000000 #csv.field_size_limit
strCVStoParse = "D:\\logs\\fileforanalysis.csv" #input file
strOutPath = "D:\\logs\\analysisoutput.txt" #output file
strSeparatorChar = "," #\t for tab delimeter
strQuoteChar = "" #set "" to not use quote char
#strdateFormat = "%Y-%m-%dT%H:%M:%S.%fZ"; #2018-12-27T15:19:53.141Z
#strdateFormat = "%Y-%m-%d %H:%M:%S"; #2018-12-27 00:00:00
#strdateFormat = "%Y-%m-%d %H:%M:%S.%f";
#strdateFormat = "%m/%d/%Y %H:%M"; # 12/27/2018 14:47
#strdateFormat = "%m/%d/%Y %I:%M:%S %p"; # 12/27/2018 2:47:52 PM
strdateFormat = "%m/%d/%Y %I:%M:%S %p"; # 12/27/2018 2:47:52 PM
#strdateFormat = "%b %d, %Y, %I:%M:%S %p"; # Dec 27, 2018, 2:47:52 PM
boolEpoch = False; #need to convert time from epoch
boolTrackCount = True; #tracks count event if duplicate time stamp
boolOldestFirst = True;#oldest item listed first in csv
#86400 seconds in day #3600 seconds in hour 
intTimeBreak = 14400; #If time dif from last event is greater than this value then reset tracking and ouput 
intDateColumn = 0; #which column contains the date we want tracked
boolTimeInNextColumn = False #Date and time are separated columns
intKeyColumn = 1; #key used for tracking 
listKeyColumns = [] # if a list is specified then this list of columns is used instead of intKeyColumn. Example listKeyColumns = [1,7]. Blank value = []
intColumnCount = 5;#Number of columns in CSV. If column count varies then set this to the lowest number of columns
boolRowSample = True #Output a sample row with the condensed output (set to false if there are formatting issues with the input CSV to avoid carrrying the issue over)
boolFirstRow = True #Output first row or last row as sample with condensed output
boolIncludeSingleEvent = True #Output for key with no match
boolRemoveInvalidCharsFromDate = False #if errors are encountered parsing dates then set this to True
boolCaseSensitive = False # do all tracking and comparison in lowercase
boolTruncateAtChar = False #truncate key at first space
strTruncateChar = "\t"

def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def FirstOrSecond(boolFirst, strFirstLine, strSecondLine):
    if boolFirst == True:
        return returnPipe(strFirstLine);
    else:
        return returnPipe(strSecondLine);

def removeInvalidChars(strTmpDateTime):
    tmpDateTimeOut = "";
    for dtChar in strTmpDateTime:
        if dtChar == "0" or dtChar == "1" or dtChar == "2" or dtChar == "3" or dtChar == "4" or dtChar == "5" or dtChar == "6" or dtChar == "7" or dtChar == "8" or dtChar == "9" or dtChar == " " or dtChar == "-" or dtChar == "/" or dtChar == ":" or dtChar == ".":
            tmpDateTimeOut = tmpDateTimeOut + dtChar
    if tmpDateTimeOut[-1] == " ":
        tmpDateTimeOut = tmpDateTimeOut[:-1]
    return tmpDateTimeOut

def returnPipe(listRow):
    strReturnRow = "";
    for n in range(len(listRow)):
        strReturnRow = strReturnRow + "|" + listRow[n]
    return strReturnRow

def writeCSV(fHandle, rowOut):
    fHandle.write("\"" + rowOut.replace("|", "\",\"") + "\"\n")


csv.field_size_limit(maxInt)
with io.open(strOutPath, "w", encoding="utf-8") as f:

    with open(strCVStoParse, "rt", encoding='utf-8') as csvfile:
        if strQuoteChar == "":
          reader = csv.reader(csvfile, delimiter=strSeparatorChar,quoting=csv.QUOTE_NONE) #, quotechar='"'
        else:
          reader = csv.reader(csvfile, delimiter=strSeparatorChar,quotechar=strQuoteChar) #, quotechar='"'
        keyitem = "";
        for row in reader:
            logDateTime = None
            keyitem = ""
            logDtime = ""
            if len(row) == intColumnCount:
                print("decrease column count by one") #exclude non conforming entry
            if len(row) > intColumnCount:
                if len(listKeyColumns) > 0:
                    for listItem in listKeyColumns:
                        if keyitem == "":
                            keyitem = row[listItem]
                        else:
                            keyitem = keyitem + "|" + row[listItem]
                else:
                    keyitem = row[intKeyColumn];
                logDtime = row[intDateColumn];
                if boolTimeInNextColumn == True:
                    logDtime = logDtime + " " + row[intDateColumn +1]
            else:
                tmpErrorOut = "";
                for rowEntry in row:
                    if tmpErrorOut == "":
                        tmpErrorOut = rowEntry
                    else:
                        tmpErrorOut = tmpErrorOut + strSeparatorChar + rowEntry
                eprint ("Row does not contain enough columns: " + str(len(row)) + "   ")
            try:
                if boolEpoch == False:
                    if boolRemoveInvalidCharsFromDate == True:
                        logDtime = removeInvalidChars(logDtime)
                    logDateTime = time.strptime(logDtime, strdateFormat)
                else:
                    logDateTime = datetime.datetime.fromtimestamp(float(logDtime))

            except ValueError:
                #try:
                    strTmpOut = "DateTime Conversion Exception!:  ";
                    for colitem in row:
                        strTmpOut = strTmpOut + "|" + colitem;
                    eprint (strTmpOut)
                    logDtime = "";
                    #logDateTime = time.strptime(row[7], "%Y-%m-%d %H:%M:%S.%f")
                    #keyitem = row[5];
                #except ValueError:
                #    print ("date parse error:" + keyitem + "|" + logDtime)
                    
            if boolEpoch == True and logDateTime != None:
                logDateTime = logDateTime.timetuple();
            if boolTruncateAtChar == True:
              if keyitem.find(strTruncateChar) > -1:
                keyitem = keyitem[:keyitem.find(strTruncateChar)]
            if boolCaseSensitive == False:
                keyitem = keyitem.lower();
            if keyitem in dictTrack and logDtime != "":

                if boolOldestFirst == True:
                    timeNewest = logDateTime;
                    timeCompare = dictTrack[keyitem];
                else:
                    timeNewest = dictTrack[keyitem];
                    timeCompare = logDateTime;


                diffTime = int((time.mktime(timeNewest) - time.mktime(timeCompare))) ; 


            
                if (diffTime > 1 and diffTime < intTimeBreak) or (boolTrackCount == True and diffTime >= 0 and diffTime < intTimeBreak) :
                    if keyitem in dictOutput:
                        dictOutput[keyitem] += 1;
                        dictLastL[keyitem] = time.strftime('%Y-%m-%dT%H:%M:%SZ', logDateTime);
                        dictLastL[keyitem + "_Alternate"] = row;
                        #print (keyitem + "|" + str(diffTime))
                    else:
                        dictOutput[keyitem] = 0;
                        dictFirstL[keyitem] = dictTrack[keyitem];
                        dictFirstL[keyitem + "_Alternate"] = row;
                        dictLastL[keyitem] = time.strftime('%Y-%m-%dT%H:%M:%SZ', logDateTime);
                        dictLastL[keyitem + "_Alternate"] = row;

                elif keyitem in dictOutput:
                    if dictOutput[keyitem] > 1:
                        lineOutput = keyitem + "|" + str(dictOutput[keyitem]) + "|" + dictLastL[keyitem] + "|" + time.strftime('%Y-%m-%dT%H:%M:%SZ', dictFirstL[keyitem])
                        if boolRowSample == True:
                          lineOutput = lineOutput + FirstOrSecond(boolFirstRow,dictFirstL[keyitem + "_Alternate"],dictLastL[keyitem + "_Alternate"])
                        writeCSV(f, lineOutput)
                    del dictOutput[keyitem]
                    del dictFirstL[keyitem]
                    if keyitem in dictLastL:
                        del dictLastL[keyitem]
                        del dictLastL[keyitem + "_Alternate"]
                    del dictFirstL[keyitem + "_Alternate"]
            if logDtime != "":
                dictTrack[keyitem] = logDateTime;
                if boolIncludeSingleEvent == True and keyitem not in dictFirstL:
                    dictFirstL[keyitem] = dictTrack[keyitem];
                    dictFirstL[keyitem + "_Alternate"] = row;
                    dictOutput[keyitem] = 0;

                            
    for outputline in dictOutput:
        if outputline in dictLastL:
            strTmpLast =  dictLastL[outputline];
            strTmpLastAlt = dictLastL[outputline + "_Alternate"]
        else:
            strTmpLast = "N/A"
            strTmpLastAlt = "N/A"
        strOutputRow = outputline + "|" + str(dictOutput[outputline]) + "|" + time.strftime('%Y-%m-%dT%H:%M:%SZ', dictFirstL[outputline]) + "|" +  strTmpLast
        if boolRowSample == True:
          strOutputRow = strOutputRow + FirstOrSecond(boolFirstRow,dictFirstL[outputline + "_Alternate"],strTmpLastAlt)
        writeCSV(f,strOutputRow)
            



