'''
For each user, this function pulls all the filtered strings and outputs those strings
    with the context in which they were found. This step merges adjacent
    identifier strings and colors the same in the output files. Host and Referer lines
    filtered out in this step.
'''

import sys
sys.path.append("../")
from collections import namedtuple
from os import path, system, stat
from os.path import join
import global_options
import line_offset
import logging
import config
import json
import csv
import re


content_dict = {}
global host_count
host_count = 0
global ref_count
ref_count = 0
interval = namedtuple('interval', ('start', 'end'))

class line:
    def __init__(self, start, end = None):
        self.interesting = False
        self.boundary = []
        self.start = start
        self.end = end

    def add_and_merge(self, offset):
        # print offset, self.boundary
        # a = raw_input()
        for i, itv in enumerate(self.boundary):
            if ((offset >= itv.start) and (offset <= itv.end)):
                # print "inside"
                if itv.end < (offset + 8):
                        self.boundary[i] = interval(itv.start, offset + 8)
                        # print self.boundary
                        return
        self.boundary.append(interval(offset, offset + 8))
        # print self.boundary

    def final_merge(self):
        self.boundary = sorted(self.boundary, key = lambda x: x.start)
        # print self.boundary
        prev = None
        new_intervals = []
        count = 0
        for i, itv in enumerate(self.boundary):
            if i == 0:
                prev = itv
                new_intervals.append(prev)
                continue
            if itv.start <= prev.end:
                if itv.end > prev.end:
                    new_intervals[count] = interval(prev.start, itv.end)
                prev = new_intervals[count]
            else:
                new_intervals.append(itv)
                count += 1
                prev = new_intervals[count]

        if new_intervals == [] and self.boundary == True:
            logging.error('something wrong, the interval should not be empty')
        # print "new", new_intervals
        # raw_input()
        self.boundary = new_intervals


def get_content_per_connection_per_directory(filepath):
    f = open(filepath, 'r')
    data = json.load(f)
    for str_, cfile_ids in data.items():
        for cfile_id, list_ in cfile_ids.items():
            if content_dict.has_key(cfile_id):
                content_dict[cfile_id][str_] = list_
            else:
                content_dict[cfile_id] = {str_ : list_}


def get_content_per_connection(path, userID):
    for char in xrange(config.DIRECTORY_COUNT):
        filepath = join(join(path, 'data_%s' % str(char)), join(config.FINAL, str(userID)))
        get_content_per_connection_per_directory(filepath)



def sort_identifier_files(userID):
    '''
    Sorts all the files containing identifier strings for user with id: userID
    '''
    for num in config.DIRECTORY_NUM:
        filepath = join( join(config.MASTER_PATH, 'data_%s' % str(num)), config.AFT_PERSISTENCE_FILT + config.AFT_CONNECTION_FILT + str(userID))
        if path.exists(filepath):
            system("sort -t$'\t' -k2,2 -o " + filepath + ' ' + filepath)



def get_escaped_line(l_inp, line_):
    intervals = sorted(line_.boundary, reverse = True, key = lambda x: x.end)
    n_line = False
    if (intervals[0].end > line_.start + len(l_inp)):
        n_line = True
    l_out = ''
    escape_begin = '\x1b[0;31;40m'
    escape_end = '\x1b[0m'
    l_inp = l_inp[:-1]
    line_.end = line_.start + len(l_inp)
    prev = interval(line_.end, line_.end)
    for i, itv in enumerate(intervals):
        l_out = l_inp[itv.end - line_.start : prev.start - line_.start] + l_out
        l_out = escape_begin + l_inp[itv.start - line_.start: itv.end - line_.start] + escape_end + l_out
        prev = itv
    if prev.start != line_.start:
        l_out = l_inp[:prev.start - line_.start] + l_out
    return [n_line, l_out + '\n']

def remove_host(line_):
    # Returns true if Host found

    if re.match('^Host:', line_) == None:
        return False
    global host_count
    host_count += 1
    return True

def remove_referer(line_):
    if re.match('^Referer:', line_) == None:
        return False
    global ref_count
    ref_count += 1
    return True


def analysis_help_filters(line_):
    # returns True if the line needs to be filtered

    return remove_host(line_) or remove_referer(line_)

def print_interesting_lines(fwrite, path, cfile, check):
    # adding a separator before printing contents from each content file

    fwrite.write('\n')
    fwrite.write(path + '/' + cfile + '\n')
    fwrite.write('_'*80 + '\n')

    fout = open(join(path, cfile), 'r')
    count = 0
    curr_line = False
    for line_ in fout:
        if check[count].interesting and (analysis_help_filters(line_) == False):
            next_line, l_out = get_escaped_line(line_, check[count])
            fwrite.write(l_out)
            curr_line = next_line
        else:
            if curr_line == True:
                fwrite.write('follow_up: ' + line_)
                curr_line = False
        count += 1


def get_index(val, lst):
    # get the line number where the particular offset_value called val lies
    prev = 0
    for i, off in enumerate(lst):
        if val >= off:
            prev = i
        else:
            return prev
    return prev

def get_populated_lines(line_offsets): #creates a list of lines: a data structure
    if line_offsets == -1:
        print 'empty line offset. returning'
        return -1
    check = []
    for i, begin in enumerate(line_offsets):
        tmp = line(begin)
        check.append(tmp)
        if i == 0:
            continue
        check[i-1].end = begin
    return check

def get_context(id_2_cfile, userID, content_path, line_offset_data):
    '''
    Prints the interesting lines with colored identifier strings for each user.
    This function also filters to remove Host and Referer lines.
    :param id_2_cfile: mapping from content_file_id to content file name
    :param userID: unique identifier for each user
    :param content_path: path of all content files for the user
    :param line_offset_data: mapping from content file id to list of line offsets
    :return:
    '''
    context_file_path = join(config.CONTEXT_FOLDER, config.CONTEXT_FILENAME_PRE + str(userID))
    fwrite = open(context_file_path, 'w')

    # merge all files containing identifier strings for user : userID
    system("sort -m -t$'\t' -k2,2 -o " + join(config.CONTEXT_FOLDER, 'merged_' + str(userID)) + ' ' + join(config.MASTER_PATH, '*/dfilt_user_' + str(userID)))
    logging.info('done merging')
    if not global_options.debug:
        system('cd %s ; find . | grep /dfilt_user_%s | xargs rm; cd ..' % (config.MASTER_PATH, str(userID)))
    merged_filepath = join(config.CONTEXT_FOLDER, 'merged_%s' % str(userID))
    # system("sort -t$'\t' -k2,2 -o " + merged_filepath + ' ' + merged_filepath)
    if path.exists(merged_filepath) == False or stat(merged_filepath).st_size == 0:
        return
    fread = open(merged_filepath, 'r')

    while True:
        [string, prev_cfile_id, offsets] = fread.readline().split('\t')
        offset_values = line_offset_data[prev_cfile_id]
        if offset_values != -1:
            break
    check = get_populated_lines(offset_values)

    for off in [int(j) for j in offsets.split(',')]:
        i = get_index(off, offset_values)
        check[i].interesting = True
        check[i].add_and_merge(off)

    for line_ in fread:
        [string, cfile_id, offsets] = line_.split('\t')


        if cfile_id == prev_cfile_id:
            if offset_values == -1:
                continue
            for off in [int(j) for j in offsets.split(',')]:
                i = get_index(off, offset_values)
                check[i].interesting = True
                check[i].add_and_merge(off)

        else:
            if offset_values != -1:
                for line_elem in check:
                    line_elem.final_merge()
                dayID = prev_cfile_id.split('_')[1]
                day_path = join(content_path, 'day_%s' % dayID)
                logging.info('printing useful content for cfile ' + prev_cfile_id
                    + ' ' + id_2_cfile[prev_cfile_id])
                print_interesting_lines(fwrite, day_path, id_2_cfile[prev_cfile_id], check)

            offset_values = line_offset_data[cfile_id]
            prev_cfile_id = cfile_id
            if offset_values == -1:
                continue
            check = get_populated_lines(offset_values)
            for off in [int(j) for j in offsets.split(',')]:
                i = get_index(off, offset_values)
                check[i].interesting = True
                check[i].add_and_merge(off)

def read_content_file_is_mapping(fpath):
    '''
    :param fpath: file containing mapping from content_file_id to content_file_name
    :return: dictionary with key on content_file_id and value as content_file_name
    '''
    d = {}
    with open(fpath, 'rb') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter='\t')
        for row in csv_reader:
            d[row[0]] = row[1]
    return d


def main(userID, content_path):
    '''
    For each user, this function pulls all the filtered strings and outputs those strings
    with the context in which they were found. This step also merges adjacent
    identifier strings and colors the same in the output files.
    :param userID: unique identifier of the user
    :param content_path: path of the all the content files of the user
    :return: prints context into files.
    '''

    # computes the list of line offsets for all files of a user
    line_offset_data = line_offset.main(userID, content_path)
    global content_dict
    content_dict = {}
    sort_identifier_files(userID)
    try:
        mapping = read_content_file_is_mapping(join(config.CONTEXT_FOLDER, config.ID2CFILE + str(userID)))
    except:
        logging.error(sys.exc_info()[0])
        logging.error('content_file_id mapping not found for user: ' + str(userID))
        return
    get_context(mapping, userID, content_path, line_offset_data)
    logging.info("removed " + str(host_count) + " lines starting with Host")
    logging.info("removed " + str(ref_count) + " lines starting with Referer")
    return host_count, ref_count






