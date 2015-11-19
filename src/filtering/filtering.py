__author__ = 'Sakshi Jain'
__email__ = 'sakshi.jain@eecs.berkeley.edu'

'''
Main filtering step. Applies two filters:
a. Connection based filtering
b. String based filtering
Once filtering is done, this code processes the identifier strings to
print files containing the context along with strings.
'''

import sys
sys.path.append("../")
import server_functions
import connection_filtering
from os import listdir, system, path
from datetime import datetime
import global_options
import string_filtering
import change_format
import get_metadata
import logging
import config
import sys
import re



def load_unique_servers(userID):
    return [line.rstrip('\n') for line in open(path.join(config.CONTEXT_FOLDER, 'uniqueServers_%s' % str(userID)))]


def readPaths():
    if global_options.test:
        return [line.rstrip('\n') for line in open(config.DATA_PATHS_TST)]
    return [line.rstrip('\n') for line in open(config.DATA_PATHS)]

def setup_env():	
    if path.exists(config.MASTER_PATH) == False:
        system('mkdir ' + config.MASTER_PATH)
    if path.exists(config.CONTEXT_FOLDER) == False:
        system('mkdir ' + config.CONTEXT_FOLDER)
    system('rm -r ' + config.MASTER_PATH)
    system('rm -r ' + config.CONTEXT_FOLDER)


def parseArguments(args):
    if not args:
        return
    option = args[0]
    if option == "--compress":
        global_options.set_compress(True)
        parseArguments(args[1:])
    elif option == '--day':
        global_options.set_day_based(True)
        parseArguments(args[1:])
    elif option == '--printable_only':
        global_options.set_printable_only(True)
        parseArguments(args[1:])
    elif option == '--test':
        global_options.set_test(True)
        parseArguments(args[1:])
    elif option == '--debug':
        global_options.set_debug(True)
        parseArguments(args[1:])
    elif option == '--run':
        while True:
            args = args[1:]
            if not args:
                break
            run_arg = args[0]
            if run_arg not in global_options.stage_map.keys():
                break
            global_options.add_to_stages_list(run_arg)
        parseArguments(args)

    elif option == '-h' or option == '--help':

        print """
        Usage: python filtering.py [options]
        --run <arg ...>    Provide the list of stages to run from amongst
                           'all', 'conn', 'sort', 'unique', 'persistent', 'context'.
                           For e.g. --run all                         will run all stages
                                    --run unique persistent context   will run unique, persistent and context
        --day              Turn on day based flow. Note: Only supports --run all
        --compress         Turn on compression
        --help, -h         Display this help information
        --printable_only   Discard all non-printable characters
        --debug            Keeps intermediary files from filtering steps
        --test             Test the code on test case

        Examples:
        1. python filtering.py --run persitent context --compress --printable_only
              will run persitent filter and context filter, with file compression turned on
              and also discarding non-printable characters.
        """

    else:
        raise Exception('Unknown command line option.')


def get_day_list(sample_path):
    '''
    Computes the list of days for the users in dataset
    :param sample_path: data path of one of the users
    :return: list of names of day based folders
    '''

    return_list = []
    for day in listdir(sample_path):
        if re.match('day_*', day) != None:
            return_list.append(day)
    return return_list

def main():
    user_paths = readPaths()
    logging.info('I have the pathlist')

    if 'conn' in global_options.stage_list:
        setup_env()

        # gets all the unique servers to each user and prints them to a file called uniqueServers

        server_functions.get_unique_servers(user_paths)
        logging.info('Done getting unique servers : ' + str(datetime.now()))

        day_list = get_day_list(user_paths[0])

        for day in day_list:
            dayID = day.split('_')[1]

            # Connection Based Filtering

            for userID, filepath in enumerate(user_paths):
                serverList = load_unique_servers(userID)
                content_path = path.join(filepath, day)
                if path.exists(content_path):
                    connection_filtering.main(content_path, userID, dayID, serverList)
                    logging.info('Connection based filtering completed for ' + str(userID))

            # String Based Filtering: applying uniqueness filter

            if global_options.day_based:
                #sorting all the files
                change_format.main_sort(config.AFT_CONNECTION_FILT)

                bins = listdir(config.MASTER_PATH)
                count = 1
                bin_count = len(bins)
                for bin in bins: #256 such folders
                    string_filtering.main(path.join(config.MASTER_PATH, bin), 'unique')
                    logging.info('Done count: ' + str(count) + ' out of ' + str(bin_count))
                    count += 1
                system('cd %s ; find . | grep /user | xargs rm; cd ..' % (config.MASTER_PATH))
                # system('rm ' + path.join(config.MASTER_PATH, '*/user_*'))

    bins = listdir(config.MASTER_PATH)
    bin_count = len(bins)

    if not global_options.day_based:
        if 'sort' in global_options.stage_list:
            change_format.main_sort(config.AFT_CONNECTION_FILT) #sorting all the files
        if 'unique' in global_options.stage_list:
            count = 1
            for bin in bins: #256 such folders
                string_filtering.main(path.join(config.MASTER_PATH, bin), 'unique')
                logging.info('Done bins: ' + str(count) + ' out of ' + str(bin_count))
                count += 1
            if not global_options.debug:
                system('cd %s ; find . | grep /user | xargs rm; cd ..' % (config.MASTER_PATH))


    # sort all files generated after uniqueness filtering
    if global_options.day_based:
        change_format.main_sort(config.AFT_UNIQUENESS_FILT)


    if 'persistent' in global_options.stage_list:
        count = 1
        for bin in bins: #256 such folders
            string_filtering.main(path.join(config.MASTER_PATH, bin), 'persistent')
            logging.info('Done bins: ' + str(count) + ' out of ' + str(bin_count))
            count += 1
        if not global_options.debug:
            system('cd %s ; find . | grep /cfilt_ | xargs rm; cd ..' % (config.MASTER_PATH))

    logging.info('Completed persistent filtering')

    # Building context again from filtered strings for context based filtering

    if 'context' in global_options.stage_list:
        host_count = 0
        ref_count = 0
        for userID, filepath in enumerate(user_paths):
            print userID, filepath
            host, ref =  get_metadata.main(userID, filepath)
            host_count += host
            ref_count += ref
            logging.info('Completed getting context for ' + str(userID))



if __name__ == "__main__":
    logging.basicConfig(filename='filtering.log', filemode = 'w', format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG)
    logging.info('Started')
    parseArguments(sys.argv[1:])
    if not global_options.stage_list:
        print 'No stages to run. Please specify using --run command. See python %s -h for help' % sys.argv[0]
        exit()
    if global_options.day_based and 'all' not in global_options.stage_list:
        print '--day only supports --run all'
        exit()
    main()
    logging.info('Done')




