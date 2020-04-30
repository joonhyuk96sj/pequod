import sys
import fileinput

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit()

    users = {}
    user_file = sys.argv[1] 
    idx = 0   
    for line in fileinput.input([user_file]):
        parsing = line.split('\n')[0].split()
        if len(parsing) != 2:
            continue
        user = int(parsing[0])
        cnt = users.get(user)
        if cnt == None:
            users[user] = idx
            idx += 1
        else:
            sys.exit()

    print(len(users))
    fname = sys.argv[2]
    for line in fileinput.input([fname]):
        parsing  = line.split('\n')[0].split()
        user     = int(parsing[0])
        follower = int(parsing[1])
    
        if user in users.keys() and follower in users.keys():
            print(users.get(user), users.get(follower))