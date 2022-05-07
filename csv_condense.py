import csv
import os
import operator
import itertools
import io
from collections import defaultdict


strinputFile = "D:\\logs\\fileforanalysis.csv" #Input file or folder to process
strOutPath = "D:\\analysis\\output.csv" #output file
boolTruncateAtChar = True #truncate key at first space
strTruncateChar = "\t"
boolIncludeCount = True
boolQuoteOutput = True #add quotes around each field
inputEncoding = "utf-8"
outputEncoding = "utf-8"
intNumericAddColumn = 9; #Column containing numeric value to add up. Set to -1 to disable.
boolHasHeader = True #if no header in CSV then set to false and provide integer values for dictHeader
boolIgnoreCountErrror = True #skip counting entries where intNumericAddColumn is not numeric
maxInt = 100000000 #csv.field_size_limit
dictHeader = dict() #add the column header for each column that will be tracked
dictNumeric = dict()

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
listFilePaths = []

if os.path.isdir(strinputFile):  
  for (dirpath, dirnames, filenames) in os.walk(strinputFile):
    for file in filenames:
        scanPath = os.path.join(dirpath, file)
        print(scanPath + "\n")
        listFilePaths.append(scanPath)
elif os.path.isfile(strinputFile):  
  listFilePaths.append(strinputFile)
else:
  print("Bad file path?")
  
for strinputFile in listFilePaths:
  boolFirstRow = boolHasHeader;
  csv.field_size_limit(maxInt)
  intRowCount = 0
  with open(strinputFile, "rt", encoding=inputEncoding) as csvfile: #, encoding="utf-16"
      reader = csv.reader(csvfile, delimiter=',', quotechar='\"')
      keyitem = "";
      for row in reader:
          if boolFirstRow == True: #parse header row
              boolFirstRow= False
              if boolHasHeader == True:
                for headItem in row:
                    if headItem in dictHeader:
                        dictHeader[headItem] = intRowCount;
                    intRowCount += 1;
              for headItem in dictHeader:
                  if not isnumeric(dictHeader[headItem]):
                    print("Missing header value \"" + headItem + "\"")
          else: #not blank row
              keyitem = ""
              for columnloc in sorted(dictHeader, key=dictHeader.get): #sort off value to keep column alignment consistent with source
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
                   elif len(row) <= dictHeader[columnloc]: #Error! can't grab value
                    print("Row not as long as header: " + str(len(row)) + "<=" + str(dictHeader[columnloc]) + "\n" + " ".join(row))
                    print(row)
                   else: #aggregate key names
                      keyitem = keyitem + "|" + row[dictHeader[columnloc]];
              if intNumericAddColumn > -1 and intNumericAddColumn < len(row): #intNumericAddColumn has a value and is less than the row count
                if row[intNumericAddColumn].isnumeric(): #Column we are counting has a numeric value
                  intNumeric = int(row[intNumericAddColumn])
                  if keyitem in dictNumeric: #aggregate value
                    dictNumeric[keyitem] = int(dictNumeric[keyitem]) + int(intNumeric) #math
                  else: #new addition to dict
                    dictNumeric[keyitem] = int(intNumeric)
                else: #non-numeric value for intNumericAddColumn
                  print("Error! intNumericAddColumn is Not numeric: " + row[intNumericAddColumn] + "\n" + keyitem + "\n" + str(intNumericAddColumn) + "|" + str(len(row)) + "\n" + " ".join(row))
              elif intNumericAddColumn > len(row) -1 and boolIgnoreCountErrror == True:
                print("ingoring row: " + " ".join(row) + "\nfrom file " + strinputFile + "\non line " + str(intRowCount))
              elif intNumericAddColumn > len(row) -1:
                print("Error: intNumericAddColumn is greather than the CSV column count. \n" + str(intNumericAddColumn) + ">=" + str(len(row)) + "Disabling numeric add function\n" + strinputFile + "\n" + " ".join(row))
                intNumericAddColumn = -1
              elif intNumericAddColumn != -1 and keyitem not in dictNumeric: #should only hit here if something is wrong
                print("not sure how we got here. Check your input file(s) and config? " + keyitem + "\n" + str(intNumericAddColumn) + "|" + str(len(row)) + "\n" + " ".join(row))
              if keyitem in dictOutput:
                  dictOutput[keyitem] += 1;
              else:
                  dictOutput[keyitem] = 1;
          intRowCount +=1
with io.open(strOutPath, "w", encoding=outputEncoding) as f: #encoding="utf-16"
  for lineEntry in dictOutput:
      if boolIncludeCount == True: #Count of entries combined
        outputline = lineEntry + "|" + str(dictOutput[lineEntry])
      else:
        outputline = lineEntry 
      if intNumericAddColumn > -1: #Integer aggregation
        strCount = "|" + str(dictNumeric[lineEntry])
        outputline = outputline + strCount
      writeCSV(f,outputline)




