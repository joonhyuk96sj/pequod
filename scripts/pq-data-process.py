import sys
import fileinput
import operator

users = 1800000

def init_dict(dict_data):
    global users
    for i in range(users):
        dict_data[i] = 0

def print_dict(dict_type, dict_data):
    print(dict_type)
    for key, value in sorted(dict_data.items()):
        print(key, value)
    print("\n")    

def print_dict_all(dict_type, dict_pos, dict_sub, dict_sub_r, dict_sscan, dict_pscan):
    print(dict_type)
    for key, value in sorted(dict_pos.items(), key=operator.itemgetter(1)):
        print(key, value, dict_sub[key], dict_sub_r[key], dict_sscan[key], dict_pscan[key])
    print("\n")    

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit()        
    fname = sys.argv[1]
    
    d_sub   = {}
    d_sub_r = {}
    d_pos   = {}
    d_sscan = {}
    d_pscan = {}
    init_dict(d_sub)
    init_dict(d_sub_r)
    init_dict(d_pos)
    init_dict(d_sscan)
    init_dict(d_pscan)

    # ----------------------------------   
    for line in fileinput.input([fname]):
        parsing = line.split('\n')[0].split() 
        if len(parsing) <= 1:
            continue
        if parsing[0] != "[DB]":
            continue
        if parsing[1] != "PUT" and parsing[1] != "SCAN":
            continue

        key = parsing[2]
        key_info = key.split('|')
        key_type = key_info[0]
        key_user = int(key_info[1])

        if parsing[1] == "PUT":
            if key_type == 's':    
                cnt = d_sub.get(key_user)
                d_sub[key_user] = cnt+1
                
                key_user_2 = int(key_info[2])
                cnt = d_sub_r.get(key_user_2)
                d_sub_r[key_user_2] = cnt+1

            elif key_type == 'p':
                cnt = d_pos.get(key_user)
                d_pos[key_user] = cnt+1
                key_time = int(key_info[2])
                if key_time >= 999999999:
                    break
            else:
                print("Invalid Type")
                break
        else:
            if key_type == 's':        
                cnt = d_sscan.get(key_user)
                d_sscan[key_user] = cnt+1    

            elif key_type == 'p':
                cnt = d_pscan.get(key_user)
                d_pscan[key_user] = cnt+1

            else:
                print("Invalid Type")
                break

    # ----------------------------------   
    #print_dict("sub dict",   d_sub)
    #print_dict("sub-r dict", d_sub_r)
    #print_dict("pos dict",   d_pos)
    #print_dict("sscan dict", d_sscan)
    #print_dict("pscan dict", d_pscan)
    print_dict_all("ALL dict", d_pos, d_sub, d_sub_r, d_sscan, d_pscan)
    