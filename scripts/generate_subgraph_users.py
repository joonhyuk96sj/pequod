import sys
import fileinput

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit()        
    fname = sys.argv[1]
        
    users = {}
    for line in fileinput.input([fname]):
        parsing  = line.split('\n')[0].split()
        user     = int(parsing[0])
        follower = int(parsing[1])
    
        cnt = users.get(user)
        if cnt == None:
            users[user] = 0
        else:
            users[user] = cnt + 1

    for user, nfollwer in sorted(users.items()):
        print(user, nfollwer)
    print("Total # of users", len(users))