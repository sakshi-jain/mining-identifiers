
compress_flag = False
day_based = False
printable_only = False
stage_map = { 
	'all' : 0,
	'conn' : 1,
	'sort' : 2,
	'unique' : 3,
	'persistent' : 4,
	'context' : 5
			}
stage_list = []
debug = False
test = False

def set_compress(flag):
	global compress_flag
	compress_flag = flag

def set_day_based(flag):
	global day_based
	day_based = flag

def set_printable_only(flag):
	global printable_only
	printable_only = flag

def add_to_stages_list(stage):
	global stage_list
	if stage == 'all':
		stage_list.extend(stage_map.keys())
	else:
		stage_list.append(stage)

def set_debug(flag):
	global debug
	debug = flag

def set_test(flag):
	global test
	test = flag

