# list of experiments exported to calling script
exps = []

# add experiments to the global list.
# a local function that keeps the internal variables from becoming global
def define_experiments():
    binary = False
    binaryflag = "" if binary else "--no-binary"
    partfunc = "twitternew" if binary else "twitternew-text"
    fetch = "--fetch"
    
    serverCmd = "./obj/pqserver"
    initCmd = "%s --twitternew --verbose %s --initialize --no-populate --no-execute" % (serverCmd, binaryflag)
    populateCmd = "%s --twitternew --verbose %s --no-initialize --no-execute --popduration=2000" % (serverCmd, binaryflag)
    clientCmd = "%s --twitternew --verbose %s --no-initialize --no-populate %s" % (serverCmd, binaryflag, fetch) 
    
    users = "--nusers=1500"
    clientBase = "%s %s --duration=100000000 --pactive=70 --pread=100 --psubscribe=0 --ppost=1 --plogout=5" % (clientCmd, users)

    exps.append({'name': "KVSDB-scenario-test", 
                 'defs': [{'def_part': partfunc,
                           'def_db_type': "postgres",
                           #'def_db_writearound': True
                           'def_db_sql_script': "scripts/exp/cache.sql",
                           'backendcmd': "%s --evict-inline --mem-lo=50 --mem-hi=75" % (serverCmd),
                           'cachecmd': "%s --evict-inline --mem-lo=50 --mem-hi=75" % (serverCmd),
                           'initcmd': "%s" % (initCmd),
                           'populatecmd': "%s %s" % (populateCmd, users),
                           'clientcmd': "%s" % (clientBase)}]})

define_experiments()
