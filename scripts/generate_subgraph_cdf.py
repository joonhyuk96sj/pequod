import sys
import fileinput
import operator

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit()        
    fname = sys.argv[1]
        
    users = {}
    for line in fileinput.input([fname]):
        parsing  = line.split('\n')[0].split()
        if len(parsing) != 2:
            continue
        user      = int(parsing[0])
        nfollower = int(parsing[1])
        cnt = users.get(user)
        if cnt == None:
            users[user] = nfollower
        else:
            sys.exit()

    cnt = 1
    for user, nfollwer in sorted(users.items(), key=operator.itemgetter(1)):
        if cnt%10== 0 or cnt>1822870:
            print(user, cnt, nfollwer)
        cnt += 1