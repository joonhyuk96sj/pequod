
import sys
import fileinput

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit()
    fname = sys.argv[1]

    nusers_total  = 40103281
    nusers_target = 50000
    interval = int(nusers_total/nusers_target)

    cnt = 0
    for line in fileinput.input([fname]):
        parsing = line.split('\n')[0].split()
        if len(parsing) != 2:
            continue
        user = int(parsing[0])
        nfollower = int(parsing[1])
        if cnt % interval == 0:
            print(user, nfollower)
            cnt = 0
        cnt += 1
        