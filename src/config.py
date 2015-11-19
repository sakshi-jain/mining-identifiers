from os import path


# Path to folder containing network traces [Should contain one trace file per day]
INPUT_NETWORK_TRACES = #'/home/username/traces/'

# Path to a tab-separated file containing mappings of trace names to day numbers
# See sample file in identifier-tracking/sample/trace-to-day-mapping.txt
TRACE_TO_DAY_MAPPING = #'/home/username/trace-to-day-mapping.txt'

# Subnet of the local network on which network traffic was collected
LOCAL_SUBNET = #'a.b.c.0/23'

# Suffix for trace files [For example, ".pcap", ".trace"]
TRACE_SUFFIX = #'.trace'

# Path to Bro installation
BRO_PATH = #'/usr/local/bro/'

# Path to folder for storing TCP payloads extracted from network traces
ALL_CONTENT = #'/home/username/identifier-tracking/trace-contents/'

# All intermediary crunches along with final set of identifiers with context
# will be dumped in this folder.
MAIN_OUTPUT_PATH = #'/home/username/identifier-tracking/master-crunch/'


################################################################################
#   IMP: The following configurations must not be changed by the user
################################################################################

# Output path to store TCP payloads organized by days
NETWORK_TRACES_CONTENTS = path.join(ALL_CONTENT, 'contents_orig/')

# Output path to store TCP payloads organized by users and days
PARENT_CONTENT_FOLDER = path.join(ALL_CONTENT, 'contents_reorg/')

# Count of buckets the dataset is divided into. Currently this
# tool supports bucketing according to first ASCII character of
# the sliding window
DIRECTORY_COUNT = 256

# List of bucket numbers
DIRECTORY_NUM = [i for i in xrange(DIRECTORY_COUNT)]

# Only byte stream upto this threshold value is used for analysis
FILE_SIZE_THRESHOLD = 50*(10**6) #50 MB

# Path where output of individual filtering steps are saved
MASTER_PATH = path.join(MAIN_OUTPUT_PATH, 'intermediary_crunch/')

# File where paths to all individual user data is saved
DATA_PATHS = path.join(MAIN_OUTPUT_PATH, '/dataPaths.sample')

# Path where the output of connection and string based filtering is saved.
# This folder contains identifiers with the associated context
CONTEXT_FOLDER = path.join(MAIN_OUTPUT_PATH, '/user_data_recrunch/')

# Prefix for each type of filtering output

# Prefix for identifiers with context
CONTEXT_FILENAME_PRE = 'context_'

# Prefix for file containing a mapping from a unique identifier to
# a content file
ID2CFILE = 'id_2_cfile_'

# Prefix for file containing sliding window output of connection and
# string based filtering
FINAL = 'final_'

# Prefix for output of connection based filtering
AFT_CONNECTION_FILT = 'user_'

# Prefix for output of persistence based filtering
AFT_PERSISTENCE_FILT = 'dfilt_'

# Prefix for output of unqiueness filtering
AFT_UNIQUENESS_FILT = 'cfilt_'

# Path to the output of "Cookie and Path" filteting
CONTEXT_FILTERED = path.join(MAIN_OUTPUT_PATH, '/context_filtered')

# Final output path containing identifiers with their context. The analyst
# must look at this folder for final set of selected identifiers to be used for
# manual analysis
CONTEXT_FILTERED_PERSISTENCE = path.join(MAIN_OUTPUT_PATH, '/identifiers_with_context')
