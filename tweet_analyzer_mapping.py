import simplejson
import json
from geopy.geocoders import Nominatim
from geopy import Point
import mysql.connector
import httplib

#This class allows for an easy dictionary to be made when writing to the MySQL database
class CountyData(object):

    def __init__(self, name, over18, medAge, perUrPop, popTot, stNum, coNum):

        self.name = name
        self.over18 = over18
        self.medAge = medAge
        self.perUrPop = perUrPop
        self.popTot = popTot
        self.stNum = stNum
        self.coNum = coNum
        #self.numTweeters
        #self.numTweets


#This function does not get alot of use anymore. Originally, it was being used to sort tweets into counties based on the coordinates presented in their bounding box, but the server proved to be too slow.
def findCounty(lat,long, coNamFile):
    NonType = type(None)
    geolocator = Nominatim(user_agent = "ISIREU20EDaniel", timeout = None)
    point = Point(long, lat)
    location = geolocator.reverse(point)
    if type(location.address) != NonType:
        loca = location.raw
        locAD = loca['address']
        con = locAD.get('county')
        if con:
            locCounty = locAD['county']
            with open(coNamFile, 'r') as f:
                places = simplejson.load(f)
            for k in places:
                name = k[0]
                nameF = name.split(',')
                nameFinal = nameF[0]
                if locCounty in nameFinal:
                    print(nameFinal)
                    return nameFinal
    return False

#This function pulls in a json file name and returns the number of tweets in it.
def countINDIVTweets(v):
    countT = 0
    with open( v ,'r') as f:
        distros_dict = simplejson.load(f)
    for tweet in distros_dict:
        if "timestamp_ms" in tweet:
            countT = countT + 1
    return countT

#This function counts the tweets in each file in the stateFileList, writes those counts to a tweetsPerState, and returns the total count.
def countBATCHTweets():
    jsonmapping = json.load(open("stateFileList.json"))
    check = []
    count = 0
    for k in jsonmapping:
        if jsonmapping[k] not in check:
            count = count + countINDIVTweets(jsonmapping[k])
            with open('./tweetsByState1/tweetsPerState.json', 'a') as f:
                f.write(k + " : " + str(count) + ",\n")
            check.append(jsonmapping[k])
    with open('./tweetsByState1/tweetsPerState.json', 'a') as f:
        f.write("]")
    return count

#This funciton is used when sorting the foundTweetsBrackets into state files.
def fileSort():
    jsonmapping = json.load(open("stateFileList.json"))
    check = []
    with open('foundTweetsBrackets.json', 'r') as f:
        distros_dict = simplejson.load(f)
    caught = 0
    for tweet in distros_dict:
        print(tweet['id'])
        place = tweet['place']
        try:
            name = place['full_name']
            caught = 0
            for k in jsonmapping:
                files = jsonmapping[k]
                rawFile = files[0]
                if k in name:
                    caught = 1
                    with open(rawFile, 'a') as f:
                        simplejson.dump(tweet, f)
                        f.write(",\n")
                    if rawFile not in check:
                        check.append(rawFile)
            if caught == 0:
                with open("lostTweets.json", 'a') as f:
                    simplejson.dump(tweet, f)
                    f.write(",\n")
        except simplejson.KeyError:
            pass

    print(str(countINDIVTweets("lostTweets.json")))
    """
    for i in check:
        with open(i, 'a') as f:
            f.write("]")
"""

#This function pulls in a file name and returns the number of individual users in that file
def countUsers(v):
    users = []
    with open(v, 'r') as f:
        distros_dict = simplejson.load(f)

    for tweets in distros_dict:
        user = tweets['user']
        username = user['screen_name']
        print(username)
        if username not in users:
            users.append(username)
    return len(users)

#This function counts the individual users in each of the state files and returns the total number of uniquie users.
def countBATCHUsers():
    jsonmapping = json.load(open("stateFileList.json"))
    check = []
    count = 0
    for k in jsonmapping:
        print ("in " + str(k))
        if jsonmapping[k] not in check:
            print ("in " + str(k))
            count = count + countUsers(jsonmapping[k])
            with open('./tweetsByState1/usersPerState.json', 'a') as f:
                f.write(k + " : " + str(countUsers(jsonmapping[k])) + ",\n")
            check.append(jsonmapping[k])
    with open('./tweetsByState1/tweetsPerState.json', 'a') as f:
        f.write("Total: " + str(count))
        f.write("]")
    return count

#This function is used to pull in "raw" tweet files
def cleanData():
    NoneType = type(None)
    with open('foundTweets8.json', 'r') as f:
        distros_dict = simplejson.load(f)
    count = 0
    nonUS = 0
    noPlace = 0
    for tweet in distros_dict:
        place = tweet['place']
        if type(place) == NoneType:
            noPlace = noPlace + 1
            with open('noPlaceTweets.json', 'a') as f:
                simplejson.dump(tweet, f)
                f.write(",\n")
        else:
            count = count + 1
            print(count)
            country = place['country_code']
            if "US" not in country:
                nonUS = nonUS + 1
            else:
                with open('foundTweetsBrackets.json', 'a') as f:
                    simplejson.dump(tweet, f)
                    f.write(",\n")
    with open('foundTweetsBrackets.json', 'a') as f:
        f.write("]")
    with open('noPlaceTweets.json', 'a') as f:
        f.write("]")
    print("nonUS: " + str(nonUS))
    print("noPlace: " + str(noPlace))

#This function is also obsolete. It allowed us to find the middle of the coordinates to determine which county the tweet was made in
def getCoord(tweet, coNameFile):
    place = tweet['place']
    bBox = place['bounding_box']
    bType = bBox['type']
    bCord = bBox['coordinates']
    coor1 = bCord[0][0]
    coor2 = bCord[0][1]
    coor3 = bCord[0][2]
    coor4 = bCord[0][3]
    lats = (coor1[0] + coor3[0])/2
    longs = (coor1[1] + coor3[1])/2
    county = findCounty(lats, longs, coNameFile)
    return county

#This function 1) determines if a tweet is already in the MySQL database, finds its county, and sends the tweet to postDB()
def getLoc():
    mydb = mysql.connector.connect(
        host = "localhost",
        user = "root",
        passwd = "S20ReUUSC",
        database = "uscreus20"
    )
    conn = httplib.HTTPSConnection('geo.fcc.gov')
    jsonmapping = json.load(open("stateFileList.json"))
    check = []
    for k in jsonmapping:
        files = jsonmapping[k]
        rawFile = files[0]
        countyFile = files[1]
        countyNames = files[5]
        print ("in " + str(k))
        if rawFile not in check:
            with open(rawFile, 'r') as f:
                distros_dict = simplejson.load(f)
            for tweet in distros_dict:
                id = tweet["id"]
                print(id)
                mycursor = mydb.cursor()
                ql_select_Query = """SELECT * FROM tweetsAndCo WHERE tweet_id=(%s)"""
                mycursor.execute(ql_select_Query, (id,))
                records = mycursor.fetchall()
                print("CHECKING: " + str(mycursor.rowcount))
                if mycursor.rowcount == 0:
                    place = tweet['place']
                    bBox = place['bounding_box']
                    bType = bBox['type']
                    bCord = bBox['coordinates']
                    coor1 = bCord[0][0]
                    coor2 = bCord[0][1]
                    coor3 = bCord[0][2]
                    coor4 = bCord[0][3]
                    lat = (coor1[0] + coor3[0])/2
                    long = (coor1[1] + coor3[1])/2
                    req = "/api/census/block/find?latitude=%s&longitude=%s&censusYear=2010&format=json" % (long, lat)
                    conn.request('GET',req)
                    res = conn.getresponse()
                    resp = simplejson.loads(str(res.read()))
                    block = resp.get('County')
                    coName = block.get('name')
                    state = resp.get('State')
                    code = state.get('code')
                    print(str(coName) + ", " + str(code))
                    if coName != "None":
                        tweet["County"] = coName
                        tweet["State"] = code
                        postDB(tweet)
                    """
                    loc = getCoord(tweet, countyNames)
                    tweet["county"] = loc
                    tweet["state"] = k
                    if loc:
                        postDB(tweet)
                    """
            check.append(rawFile)


#This function is pretty much obsolete, but it allowed us to see how many tweets had points that we could use
def countPoints():
    countP = 0
    NoneType = type(None)
    with open('./TweetsByState1/CAtweets.json', 'r') as f:
        distros_dict = simplejson.load(f)
    for tweet in distros_dict:
        place = tweet['place']
        bBox = place['bounding_box']
        bType = bBox['type']
        bCord = bBox['coordinates']
        #findLoc(bCord)
        coor1 = bCord[0][0]
        coor2 = bCord[0][1]
        coor3 = bCord[0][2]
        coor4 = bCord[0][3]
        lats = (coor1[0] + coor3[0])/2
        longs = (coor1[1] + coor3[1])/2
        if findCounty(lats, longs):
            countP = countP + 1
    return countP

#This funciton was used to consolidate county data to be written into the CountyDataFinal files for easy access
def compileCountyData():
    with open("StateFileList.json","r") as f:
        stateList = simplejson.load(f)
    for state in stateList:
        files = stateList[state]
        rawTweets = files[0]
        idToCounty = files[1]
        rawCountyData = files[2]
        meanIncome = files[3]
        finalCountyData = files[4]
        countyNames = files[5]
        with open(rawCountyData, 'r') as f1:
            countyData = simplejson.load(f1)
        with open(countyNames, 'r') as f2:
            countyName = simplejson.load(f2)
        with open(meanIncome, 'r') as f3:
            countyIncome = simplejson.load(f3)
        for info in countyData:
            if 'state' not in info[5]:
                years18Up = info[0]
                medianAge = info[1]
                urbanClusters = info[2]
                totalPop = info[3]
                fullName = info[4]
                stateNumber = info[5]
                countyNumber = info[6]
                for name in countyName:
                    for income in countyIncome:
                        conNum = income[3]
                        if conNum == countyNumber:
                            coMeanIncome = income[1]
                            break;
                    if name[2] == countyNumber:
                        s = name[0].split(',')
                        coName = s[0]
                        if len(s) > 1:
                            stName = s[1]
                        coNum = name[2]
                        stNum = name[1]
                        if int(urbanClusters) > 0:
                            percentUrbanPop = float(totalPop) / float(urbanClusters)
                        else:
                            percentUrbanPop = 0

                        c = CountyData(coName, years18Up, medianAge, percentUrbanPop, totalPop, stNum, coNum )
                        with open(finalCountyData, "a") as f4:
                            simplejson.dump(c.__dict__, f4)
                            f4.write(",\n")

#This funciton inserts new tweets into the MySQL database
def postDB(tweet):
    mydb = mysql.connector.connect(
        host = "localhost",
        user = "root",
        passwd = "S20ReUUSC",
        database = "uscreus20"
    )
    id = tweet["id"]
    county = tweet["County"]
    state = tweet["State"]
    mycursor = mydb.cursor()
    postQuery = """INSERT IGNORE INTO tweetsAndCo (tweet_id, county_name, state_name, tweet) VALUES (%s, %s, %s, %s)"""
    tweet1 = simplejson.dumps(tweet)
    tweetNow = (id, county, state, tweet1)
    try:
        mycursor.execute(postQuery, tweetNow)
    except mysql.connector.errors.IntegrityError:
        pass
    mycursor.execute(postQuery, tweetNow)
    mydb.commit()
    print("DONE " + str(id))
    return 0











if __name__ == '__main__':
    #cleanData()
    fileSort()
    #print(str(countBATCHTweets()))
    #writePerState()
    #print(str(countINDIVTweets("./TweetsByState1/VTtweets.json")))
    #print(str(countPoints()))
    #countUsers("./TweetsByState1/VTtweets.json")
    #countBATCHUsers()
    #getLoc()
    #compileCountyData()
    #postDB()
