import os
import sys
sys.path.append("../")
import logging
import config
import ipaddress
from datetime import datetime

# Assumes one trace file per day
def extract_contents():
    for trace in [ f for f in os.listdir(config.INPUT_NETWORK_TRACES) if f.endswith(config.TRACE_SUFFIX)]:
        logging.info("Processing trace %s" % (trace))
        # Create a directory for the trace and store the contents there
        outpath = os.path.join(config.NETWORK_TRACES_CONTENTS,trace + "_contents")
        if not os.path.exists(outpath):
            os.makedirs(outpath)
        cd_command = 'cd %s' % (outpath)
        bro_command = '%sbro -bC -f "tcp and ((src net %s and tcp[0:2] > 1024) or (dst net %s and tcp[2:2] > 1024))" -r %s %s/tcpdump_local.bro LOCAL_SUBNET=%s' \
                      % (config.BRO_PATH,config.LOCAL_SUBNET, config.LOCAL_SUBNET, os.path.join(config.INPUT_NETWORK_TRACES,trace), os.getcwd(),config.LOCAL_SUBNET)
	os.system("%s; %s" % (cd_command, bro_command))
    logging.info("Done extracting contents from traces")

def get_localIP_to_id_mapping(outdir):
    ip_list = [str(ip) for ip in list(ipaddress.ip_network(unicode(config.LOCAL_SUBNET)).hosts())]
    mapping = dict() 
    with open(outdir + "mapping", 'w') as mapfile:
    	for ipCount, ip in enumerate(ip_list):
	    mapping[ip] = str(ipCount)
	    mapfile.write("%d %s \n" %(ipCount, ip))
    return mapping


def read_trace_to_dayno_mapping(mappingfile):
    trace_to_dayno_mapping = dict()
    with open(mappingfile) as f:
        for line in f:
            trace, day = line.strip().split("\t")
            trace_to_dayno_mapping[trace] = day
    return trace_to_dayno_mapping


def extract_IPs_from_filename(filename):
    return [x.split(":")[0].strip("contents_") for x in filename.split("-")]


def reorganize_files():
    logging.info("Starting to reorganize files")
    out_dir = config.PARENT_CONTENT_FOLDER
    ids_with_data = set()
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    localIP_to_id_mapping = get_localIP_to_id_mapping(out_dir)
    trace_to_dayno_mapping = read_trace_to_dayno_mapping(config.TRACE_TO_DAY_MAPPING) 
    for trace in [ f for f in os.listdir(config.NETWORK_TRACES_CONTENTS) if f.endswith("_contents")]:
	trace_name = trace.split("_contents")[0]
	if trace_name != "skip.trace":
	    continue
	if trace_name not in trace_to_dayno_mapping.keys():
            logging.info("Skipping trace %s. Not found in trace_to_day mapping file" %(trace_name))
            continue
        logging.info("Reorganizing content files of trace %s" %(trace_name))
        cFolder = os.path.join(config.NETWORK_TRACES_CONTENTS, trace)
        connFile = cFolder + '/conn.log'
        fileList = os.listdir(cFolder)
        for i in fileList:
	    IPs = extract_IPs_from_filename(i)
            for IP in IPs:
		if IP in localIP_to_id_mapping.keys():
		    ids_with_data.add(localIP_to_id_mapping[IP])
		    cDir = out_dir + localIP_to_id_mapping[IP] + '/' + trace_to_dayno_mapping[trace_name] + '/'
		    if not os.path.exists(cDir):
			os.makedirs(cDir)
		    mvCommand = "cp "+ cFolder + "/" + i + " " + cDir
		    os.system(mvCommand)
        for ip, IPcount in localIP_to_id_mapping.iteritems():
            cDir = out_dir + str(IPcount) + '/' +  trace_to_dayno_mapping[trace_name] + '/'
            connLogSplitCommand = "fgrep -w " + ip + " " + connFile + " > " + cDir + "conn.log"
            os.system(connLogSplitCommand)	
	logging.info("Done reorganizing content files of trace %s" %(trace_name))

    if not os.path.exists(config.MAIN_OUTPUT_PATH):
	os.makedirs(config.MAIN_OUTPUT_PATH)
    with open(config.DATA_PATHS, "w") as f:
	for id in ids_with_data:
	    f.write("%s%s/\n" %(config.PARENT_CONTENT_FOLDER,id))

if __name__ == "__main__":
    logging.basicConfig(filename='preprocessing.log', filemode = 'w', format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG)
    logging.info("Started")
    extract_contents()
    reorganize_files()
    logging.info("Done")
