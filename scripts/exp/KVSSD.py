# list of experiments exported to calling script
exps = []

# add experiments to the global list.
# a local function that keeps the internal variables from becoming global
def define_experiments():
	binary = True
	binaryflag = "" if binary else "--no-binary"
	partfunc = "twitternew" if binary else "twitternew-text"
	fetch = "--fetch"
	
	serverCmd = "./obj/pqserver --round-robin=1024"
	appCmd = "./obj/pqserver --twitternew --verbose"
	initCmd = "%s %s --initialize --no-populate --no-execute" % (appCmd, binaryflag)
	clientCmd = "%s %s %s --no-initialize --popduration=0" % (appCmd, binaryflag, fetch) 
   
	# cache comparison experiment
	# can be run on a multiprocessor.
	#
	# the number of cache servers pequod uses should be the same as the number of 
	# clients used to access postgres through the DBPool and the same as the
	# number of redis instances.
	# fix %active at 70, post:check ratio at 1:100 and 50 timeline checks per user. 
	exp = {'name': "compare", 'defs': []}
	users = "--graph=twitter_graph_1.8M.dat"
	initBase = "%s --no-binary" % (initCmd)
	clientBase = "%s %s --no-binary --pactive=70 --duration=1000000000 --checklimit=62795845 " \
				 "--ppost=1 --pread=100 --psubscribe=10 --plogout=5" % \
				 (clientCmd, users)
	'''
	exp['defs'].append(
        {'name': "pequod",
         'def_part': "twitternew-text",
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s" % (serverCmd),
         'initcmd': "%s" % (initBase),
         'clientcmd': "%s" % (clientBase)})
	'''
	exp['defs'].append(
		{'name': "postgres",
		 'def_db_type': "postgres",
		 'def_db_sql_script': "scripts/exp/twitter-pg-schema.sql",
		 'def_db_in_memory': True,
		 'def_db_compare': True,
		 'def_db_flags': "-c synchronous_commit=off -c fsync=off " + \
						 "-c full_page_writes=off  -c bgwriter_lru_maxpages=0 " + \
						 "-c shared_buffers=10GB  -c bgwriter_delay=10000 ",
		 'populatecmd': "%s --initialize --no-execute --popduration=0 --no-binary --dbshim --dbpool-max=10 --dbpool-depth=100 " % (appCmd),
		 'clientcmd': "%s --initialize --no-populate --dbshim --dbpool-depth=100" % (clientBase)})
   
	exp['plot'] = {'type': "bar",
				   'data': [{'from': "client",
							 'attr': "wall_time"}],
				   'lines': ["postgres"],
				   'ylabel': "Runtime (s)"}
	exps.append(exp)
 
define_experiments()
