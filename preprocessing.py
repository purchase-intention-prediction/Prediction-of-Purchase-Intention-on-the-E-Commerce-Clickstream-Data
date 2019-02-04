import sys
import multiprocessing
from datetime import datetime
from datetime import timedelta
import xlsxwriter

SESSIONID = 0
TIMESTAMP = 1
ITEMID = 2
CATEGORY = 3
PRICE = 3
QUANTITY = 4


BOUNCEVISIT_TH = timedelta(seconds = 15)

def findMaxID(fileName):
    #FilePointer = open('yoochoose-clicks.dat','r')
    with open(fileName, 'r', encoding="utf-8") as FilePointer:
        maxID = int(FilePointer.readline().split(",")[0])
        for line in FilePointer:
            sID = int(line.split(",")[0])
            if maxID < sID:
                maxID = sID
    return maxID

def findBuyList(fileBuys):
    buyList = bytearray(findMaxID(fileBuys)+1)
    print("MAX ID in ",fileBuys," -->",findMaxID(fileBuys))
    with open(fileBuys, 'r', encoding="utf-8") as FilePointer:
        nextRecord = FilePointer.readline().split(",")
        for line in FilePointer:
            prev = nextRecord
            nextRecord = line.split(",")
            while prev[0] == nextRecord[0]:
                prev = nextRecord
                nextRecord = FilePointer.readline().split(",")
            buyList[int(prev[0])]=1

    return buyList

def findAll():
    buyList = findBuyList('yoochoose-buys.dat')
    print(len(buyList))
    FilePointer = open('yoochoose-clicks.dat', 'r', encoding="utf-8")
    CSVFile = open('allAttributes.csv', 'w', encoding="utf-8")
    CSVFile.write("SessionID,Session_Duration,Bounce_Visit,PageView,Distinct_PageView,Distinct_Category,Promo_SpecOffer,Class\n")
    istop = 0
    nextRecord = FilePointer.readline().replace("\n","").split(",")
    for line in FilePointer:
        bounceVisit = 0 #"NO"
        specOffer = 0 #"NO"
        classLabel = 0 #"NO"
        pageViewList = [nextRecord[ITEMID]]
        categoryList = []
        if nextRecord[CATEGORY] != "0":
            categoryList.append(nextRecord[CATEGORY])
        prev = nextRecord
        sessionStart = datetime.strptime(nextRecord[TIMESTAMP], "%Y-%m-%dT%H:%M:%S.%fZ")
        #day = sessionStart.weekday()
        #hour = sessionStart.hour
        nextRecord = line.replace("\n","").split(",")
        while prev[SESSIONID] == nextRecord[SESSIONID]:
            pageViewList.append(nextRecord[ITEMID])
            if nextRecord[CATEGORY] != "0":
                categoryList.append(nextRecord[CATEGORY])
            prev = nextRecord
            nextRecord = FilePointer.readline().replace("\n","").split(",")
        sessionEnd = datetime.strptime(prev[TIMESTAMP], "%Y-%m-%dT%H:%M:%S.%fZ")
        if (sessionEnd - sessionStart) < BOUNCEVISIT_TH:
            bounceVisit = 1 #"YES"
        if len(categoryList) > 0:
            specOffer = 1
        for x in categoryList:
            if x !="S":
                if int(x) > 10000000: #8-10 digit
                    specOffer = 2
        if "S" in categoryList:
            specOffer = 3 #"YES"
        if int(prev[SESSIONID]) < len(buyList) and buyList[int(prev[SESSIONID])] == 1:
            classLabel = 1 #'YES'
        totalTime = (sessionEnd-sessionStart).total_seconds()
        totalTime = ( totalTime / 60 )
        totalTime = round(totalTime,3)
        CSVFile.write("%s,%s,%s,%s,%s,%s,%s,%s\n" % (prev[SESSIONID], totalTime, bounceVisit, len(pageViewList), len(set(pageViewList)), len(set(categoryList)), specOffer, classLabel))
        """istop = istop +1
        if istop == 10:
            CSVFile.close()
            input()"""
    CSVFile.close()
    FilePointer.close()
    
def runInParallel(*functions):
    proc=[]
    for fn in functions:
        process = multiprocessing.Process(target=fn)
        process.start()
        proc.append(process)
    for process in proc:
        process.join()

if __name__ == "__main__":
    #runInParallel(findAll())
    if len(sys.argv) < 3:
        print("Usage python3 preprocessing.py <clicks file>  <buys file>")
        exit(-1)
    clicks = sys.argv[1]
    buys = sys.argv[2]
    print(len(sys.argv))
    if clicks != "yoochoose-clicks.dat" or buys != "yoochoose-buys.dat":
        print("Usage python3 preprocessing.py <clicks file>  <buys file>")
        exit(-1)
    findAll()
    print("Program has finished!")

