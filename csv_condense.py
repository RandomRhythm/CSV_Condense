import csv
import os
import operator
import itertools

from collections import defaultdict

strinputFile = "D:\\logs\\fileforanalysis.csv" #Input file to process
strOutPath = "D:\\analysis\\output.csv" #output file
boolTruncateAtChar = True #truncate key at first space
strTruncateChar = "\t"
boolIncludeCount = True
boolQuoteOutput = True #add quotes around each field
dictHeader = dict() #add the column header for each column that will be tracked

# the following are example column headers. Remove the # to use or add your own
# if your CSV has no header row then set the dictionary header to equal the column number. Example: dictHeader["Dst VM"] = 1;
#dictHeader["Dst VM"] = "";
#dictHeader["Src VM"] = "";
#dictHeader["Dst IP"] = "";
#dictHeader["IP Address"] = "";
#dictHeader["Remote Port"] = "";
#dictHeader["Domain"] = "";
#dictHeader["Success"] = "";

#Kansa scheduled task
#dictHeader["TaskName"] = ""
#dictHeader["Task To Run"] = ""

#Cb Response Network Activity
#dictHeader["User Name"] = "";
#dictHeader["Sensor ID"] = "";
#dictHeader["IP Address"] = "";
#dictHeader["Domain"] = "";

#Cb Response Hash Dump
#dictHeader["Path"] = "";
#dictHeader["Publisher"] = "";
#dictHeader["Company"] = "";
#dictHeader["Product"] = "";
#dictHeader["Internal Name"] = "";
#dictHeader["Original File Name"] = "";

#Cb Response module activity
#dictHeader["MD5"] = "";
#dictHeader["File Path"] = "";

#dictHeader["Computer"] = "";
#dictHeader["Sensor ID"] = "";
def writeCSV(fHandle, rowOut):
  if boolQuoteOutput == True:
    fHandle.write("\"" + rowOut.replace("|", "\",\"") + "\"\n")
  else:
    fHandle.write(rowOut.replace("|", ",") + "\n")

dictOutput = dict()
intRowCount = 0;
with open(strinputFile, "rt", encoding="utf-8") as csvfile: #, encoding="utf-16"
    reader = csv.reader(csvfile, delimiter=',', quotechar='\"')
    keyitem = "";
    for row in reader:
        if intRowCount == 0:
            
            for headItem in row:
                if headItem in dictHeader:
                    dictHeader[headItem] = intRowCount;
                intRowCount += 1;
        else:
            keyitem = ""
            for columnloc in dictHeader:
               if dictHeader[columnloc] != '': #if found in header
                 if keyitem == "":
                    if len(row) >= dictHeader[columnloc]:
                       keyitem = row[dictHeader[columnloc]];
                       if boolTruncateAtChar == True:
                        if keyitem.find(strTruncateChar) > -1:
                          keyitem = keyitem[:keyitem.find(strTruncateChar)]
                    else:
                       f2 = open(strinputFile + ".error", 'a+', encoding="utf-16")
                       f2.write("".join(row) + "\n") #write header row
                 else:
                    keyitem = keyitem + "|" + row[dictHeader[columnloc]];

            if keyitem in dictOutput:
                dictOutput[keyitem] += 1;
            else:
                dictOutput[keyitem] = 1;
with io.open(strOutPath, "w", encoding="utf-8") as f: #encoding="utf-16"
  for outputline in dictOutput:
      if boolIncludeCount == True:
        outputline = outputline + "|" + str(dictOutput[outputline])
      writeCSV(f,outputline)
