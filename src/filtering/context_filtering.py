'''
Filters all context files in the config.CONTEXT_FOLDER to remove identifier strings occurring
in paths or cookies. Writes the filtered context to config.CONTEXT_FILTERED folder.
The second part reads this folder to apply persistence based filter, i.e. to remove all strings
occurring for less than 3 days.
'''

import sys
sys.path.append("../")
from os import listdir, system, path
import config
import re



def get_host(content_filepath):
    '''
    Pulls the host from the corresponding content file
    :param content_filepath: path of the content file
    :return: line containing host value
    '''
    content_file = open(content_filepath.strip(), 'rb')
    for line in content_file:
        if line.strip().split(':')[0].lower() == 'host':
            return line
    print 'No Host found in content file: ' + content_filepath
    return None


def get_day_from_filepath(path):
    return [x for x in path.split("/") if x.startswith("day_")][0]


def get_all_trackers(line):
    '''
    :param line: input context line to search identifiers in
    :return: list of identifiers found
    '''
    trackers = []
    escape_begin = '\x1b[0;31;40m'
    escape_end = '\x1b[0m'
    begin_pos = line.find(escape_begin)
    while begin_pos != -1:
        end_pos = line.find(escape_end)
        if end_pos == -1:
            trackers.append(line[begin_pos + len(escape_begin):])
            break
        trackers.append(line[begin_pos + len(escape_begin):end_pos])
        line = line[end_pos + len(escape_end):]
        begin_pos = line.find(escape_begin)
    return trackers



def is_param_interesting(line, fwrite):
    '''
    Check if the identifier string is in parameter
    :param line: context line to be analysed
    :return: True if there exists at least one string that is in param
    '''
    escape_begin = '\x1b[0;31;40m'
    escape_end = '\x1b[0m'
    param_begin = line.find('?')
    if param_begin == -1:
        return False
    param = line[param_begin + 1:]
    if param.find(escape_begin) != -1 or param.find(escape_end) != -1:
        # fwrite.write("Is param interesting: " + line + ' ' + str(param.find(escape_begin)) + ' ' + str(param.find(escape_end)))
        return True
    return False

def clean_line(line):
    '''
    Removes the escape characters from the line
    :param line: input line
    :return: cleaned line
    '''
    escape_begin = '\x1b[0;31;40m'
    escape_end = '\x1b[0m'
    line = line.replace(escape_begin, '')
    line = line.replace(escape_end, '')
    return line


def get_tracker_persistence_distribution():
    '''
    Computes the distribution of the tracker persistence (in days) over all trackers
    found in recrunch files.
    Dictionary keyed on tracker and value is list of days it occurs on

    '''
    tracker_persistence_stat = dict()
    seen_trackers = set()
    total_context_lines = 0
    day_no = -1
    for context_file in listdir(config.CONTEXT_FOLDER):
        if re.match(config.CONTEXT_FILENAME_PRE + '*', context_file) != None:
            context_fpath = config.CONTEXT_FOLDER + context_file
            context_fname = context_file

            with open(context_fpath) as f:
                for line in f:
                    if line.strip() == '' or line.strip().find('_' * 10) == 0:
                        continue

                    if line.strip().find(config.PARENT_CONTENT_FOLDER) == 0:
                        current_contentfile_raw = line
                        day_no = get_day_from_filepath(current_contentfile_raw)
                        continue
                    total_context_lines += 1
                    trackers = get_all_trackers(line)

                    for tracker in trackers:
                        if not tracker_persistence_stat.has_key(tracker):
                            tracker_persistence_stat[tracker] = set()
                        if day_no != -1:
                            tracker_persistence_stat[tracker].add(day_no)

    return tracker_persistence_stat


def filter_tracker_by_days(trackertoDays):
    '''
    if a line contains atleast one identifier which exists for more than 2 days, the line is retained
    :param trackertoDays: Dictionary from identifier to list of days it occurs on
    :return: Nothing
    '''
    tracker_days_threshold = 2

    for context_file in listdir(config.CONTEXT_FILTERED):
        context_fpath = path.join(config.CONTEXT_FILTERED, context_file)
        context_fname = context_file
        context_file = open(context_fpath.strip(), 'rb')
        if path.exists(config.CONTEXT_FILTERED_PERSISTENCE) == False:
            system('mkdir ' + config.CONTEXT_FILTERED_PERSISTENCE)
        fwrite = open(path.join(config.CONTEXT_FILTERED_PERSISTENCE, context_fname), 'wb')

        for line in context_file:
            if line.strip() == '' or line.strip().find('_' * 10) == 0:
                continue

            if line.strip().find(config.PARENT_CONTENT_FOLDER) == 0:
                current_contentfile_raw = line
                current_contentfile = line.strip()
                host = get_host(current_contentfile_raw)
                continue

            trackers = get_all_trackers(line)

            selected_trackers = [tracker for tracker in trackers if
                                 len(trackertoDays[tracker]) > tracker_days_threshold]
            isPrintReady = (len(selected_trackers) >= 1)
            if isPrintReady:
                fwrite.write('=' * 80 + '\n\n')
                fwrite.write(current_contentfile_raw + '\n')
                if host != None:
                    fwrite.write(host + '\n')
                fwrite.write(line + '\n')
        fwrite.close()

    return


def filterContextFile(context_fpath, context_fname):
    '''
    Filters each context file to remove identifier strings occurring in paths or cookies
    Writes the filtered context to config.CONTEXT_FILTERED folder
    :param context_fpath: path of the context file to be filtered
    :param context_fname: name of the context file to be filtered
    :return: Nothing
    '''
    context_file = open(context_fpath.strip(), 'rb')
    if path.exists(config.CONTEXT_FILTERED) == False:
        system('mkdir ' + config.CONTEXT_FILTERED)
    filtered_context_file = open(path.join(config.CONTEXT_FILTERED, context_fname + '_filtered'), 'wb')

    current_contentfile = ''
    current_contentfile_interesting_bool = False
    for line in context_file:
        if line.strip() == '':
            continue

        # BEFORE MOVING ONTO THE NEXT CONTENT FILE, PRINT THE CONTENTS OF THE PREVIOUS FILE
        # IF THE PREVIOUS FILE HAD SOME LINES THAT ARE INTERESTING
        if line.strip().find(config.PARENT_CONTENT_FOLDER) == 0:
            if current_contentfile_interesting_bool:
                host_name = get_host(current_contentfile)
                filtered_context_file.write(current_contentfile + '\n')
                if host_name != None:
                    filtered_context_file.write(host_name)
                filtered_context_file.write('\n' + '=' * 100 + '\n')
            # current_contentfile = clean_file_path(line).strip()
            current_contentfile = line.strip()
            current_contentfile_interesting_bool = False

        uncolored_line = clean_line(line)

        # REMOVE INTERESTING STRINGS IF OCCURRING ONLY IN PATH
        if uncolored_line.strip()[0:3] == 'GET' or uncolored_line.strip()[0:4] == 'POST':
            interesting_parameter_bool = is_param_interesting(line, filtered_context_file)
            if interesting_parameter_bool:
                current_contentfile_interesting_bool = True
                filtered_context_file.write('\n' + line)
            continue

        if uncolored_line.strip().split(':')[0].lower() == "follow_up":
            continue

        # REMOVE COOKIE LINES
        if uncolored_line.strip().split(':')[0].lower() == 'cookie':
            continue

        current_contentfile_interesting_bool = True
        filtered_context_file.write(line)



def main():
    # filter out identifiers in cookies and paths
    for context_file_name in listdir(config.CONTEXT_FOLDER):
        if re.match(config.CONTEXT_FILENAME_PRE + '_*', context_file_name) != None:
            filterContextFile(config.CONTEXT_FOLDER + context_file_name, context_file_name)
    # filter out identifiers that are not persistent
    tracker_to_days_mapping = get_tracker_persistence_distribution()
    filter_tracker_by_days(tracker_to_days_mapping)



if __name__ == '__main__':
    main()








