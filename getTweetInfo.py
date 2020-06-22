import simplejson
import json
from geopy.geocoders import Nominatim
import httplib
import mysql.connector
"""
def fileSort():
    jsonmapping = json.load(open("mapping.json"))
    print(jsonmapping)
    for k in jsonmapping:
        print(jsonmapping[k])
"""
class CountyData(object):

    def __init__(self, name, over18, medAge, perUrPop, popTot, stNum, coNum, mIncome, coD):

        self.CountyName = name
        self.Over18 = over18
        self.MedianAge = medAge
        self.PercentUrbanPop = perUrPop
        self.TotalPopulation = popTot
        self.StateNumber = stNum
        self.CountyNumber = coNum
        self.MeanIncome = mIncome
        self.CongressionalDistrict = coD




def makeFinalCoFiles():
    with open("stateFileList.json", "r") as f:
        states = simplejson.load(f)
    checkST = []
    for state in states:
        if len(state) < 3:
            if state not in checkST:
                print("in " + state)
                checkST.append(state)
                stateFl = states[state]
                cFile = stateFl[2]
                iFile = stateFl[3]
                fFile = stateFl[4]
                with open(cFile, "r") as f1:
                    counties = simplejson.load(f1)
                with open(iFile, "r") as f2:
                    incomes = simplejson.load(f2)
                for county in counties:
                    print("in " + county[4])
                    if "NAME" not in county:
                        dist = []
                        cNum = county[6]
                        sNum = county[5]
                        for income in incomes:
                            if income[3] == cNum:
                                print("income match")
                                districts = open("./ByState/congDist.txt", "r")
                                for row in districts:
                                    print("getting CD")
                                    sep = row.split(',')
                                    st = sep[0]
                                    co = sep[1]
                                    d = sep[2]
                                    if (sNum in st) and (cNum in co):
                                        dist.append(d)
                        if (county[3] > 0):
                            if (county[2] > 0):
                                perUrPop = float(county[2])/ float(county[3])
                        else:
                            perUrPop = 0.0
                        c = CountyData(county[4], county[0], county[1], perUrPop, county[3], county[5], county[6], income[1], dist)
                        with open(fFile, "a") as f4:
                            simplejson.dump(c.__dict__, f4)
                            f4.write(",\n")




def getTweetInfo(tID):
    mydb = mysql.connector.connect(
        host = "localhost",
        user = "root",
        passwd = "S20ReUUSC",
        database = "uscreus20"
    )
    conn = httplib.HTTPSConnection('geo.fcc.gov')
    jsonmapping = json.load(open("testFiles.json"))
    check = []
    mycursor = mydb.cursor()
    ql_select_Query = """SELECT * FROM tweetsAndCo WHERE tweet_id=(%s)"""
    mycursor.execute(ql_select_Query, (tID,))
    records = mycursor.fetchall()
    for row in records:
        tweet_id = row[0]
        county_name = row[1]
        state_name = row[2]
        tweet = row[3]
    conn.close()
    file = "./ByState/%s/%sFinalCountyData.json" % (state_name, state_name)
    with open(file, "r") as f:
        counties = simplejson.load(f)
    for county in counties:
        cN = county['CountyName']
        cN = cN.split()
        if cN[0] == county_name:
            print(county)
            break;



if __name__ == '__main__':
    #makeFinalCoFiles()
    getTweetInfo(1272679529472053248)
