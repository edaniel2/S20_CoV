import simplejson

def fileSort():
    jsonmapping = json.load(open("stateFileList.json"))
    with open('foundTweetsBrackets.json', 'r') as f:
        distros_dict = simplejson.load(f)
    for tweet in distros_dict:
        place = tweet['place']
        name = place['full_name']
        for k, v in jsonmapping:
            if k in name:
                with open(v, 'a') as f:
                    simplejson.dump(tweet, f)
                    f.write(",\n")
    switch = 0
    for k, v in jsonmapping:
        if k in name:
            if switch == 0:
                with open(v, 'a') as f:
                    f.write("]")
                    switch = 1
            else:
                switch = 1

if __name__ == '__main__':
    fileSort()
