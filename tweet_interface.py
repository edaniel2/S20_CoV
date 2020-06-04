
#Coding hints from : https://stackoverflow.com/questions/56836617/how-to-loop-through-a-text-file-and-find-the-matching-keywords-in-python3

def count():

    word = "created_at"
    countT = 0
    with open('./tweetsByState/CAtweets.json','r') as source:
       for line in source:
           countT = countT + 1
    return countT



if __name__ == "__main__":
    count = count()
    print count
