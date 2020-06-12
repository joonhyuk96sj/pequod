# list of experiments exported to calling script
exps = []

# add experiments to the global list.
# a local function that keeps the internal variables from becoming global
def define_experiments():
	binary = False
	binaryflag = "" if binary else "--no-binary"
	partfunc = "twitternew" if binary else "twitternew-text"
	fetch = "--fetch"
	
	serverCmd = "./obj/pqserver --round-robin=1024"
	appCmd = "./obj/pqserver --twitternew --verbose"
	initCmd = "%s %s --initialize --no-populate --no-execute" % (appCmd, binaryflag)
	populateCmd = "%s --twitternew --verbose %s --no-initialize --no-execute --popduration=0" % (serverCmd, binaryflag)
	clientCmd = "%s %s %s --no-initialize --popduration=0" % (appCmd, binaryflag, fetch) 
	
	exp = {'name': "compare", 'defs': []}
	users = "--graph=twitter_graph_50K.dat"
	clientBase = "%s %s --no-binary --pactive=70 --duration=1000000000 --checklimit=35000000 " \
				 "--ppost=1 --pread=100 --psubscribe=10 --plogout=5" % \
				 (clientCmd, users)
	
	exps.append({'name': "KVSDB-scenario-basic", 
				 'defs': [{'def_part': partfunc,
						   'def_db_type': "postgres",
						   'def_db_sql_script': "scripts/exp/cache.sql",
						   'backendcmd': "%s --evict-periodic --mem-lo=20 --mem-hi=25" % (serverCmd),
						   'cachecmd': "%s --evict-periodic --mem-lo=15 --mem-hi=20" % (serverCmd),
						   'initcmd': "%s" % (initCmd),
						   'populatecmd': "%s %s" % (populateCmd, users),
						   'clientcmd': "%s" % (clientBase)}]})

	exps.append(exp)
 
define_experiments()
