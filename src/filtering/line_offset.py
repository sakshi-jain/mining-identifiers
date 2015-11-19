import sys
sys.path.append("../")
import config
import logging
from os.path import join

def get_line_offset(filepath):
    '''
    For each file, return list of new line offsets
    :param filepath: path of the file to be read
    :return: list of line offsets
    '''
    try:
        fread = open(filepath, 'r', 1)
    except:
        logging.info(filepath + ' not found while calculating line offsets')
        return -1
    line_offset = []
    offset = 0
    for line in fread:
        line_offset.append(offset)
        offset += len(line)
    return line_offset


def main(userID, content_path):
    '''
    For each user, this returns a dictionary keyed on a content file for the user and
    with value being all new line offsets in that file.
    :param userID: unique identifier for the user
    :param content_path: path to all the content files of the user
    :return: {content_file_id : [line offsets]}
    '''
    line_offset_dict = dict()
    with open(join(config.CONTEXT_FOLDER, config.ID2CFILE + str(userID)), 'r') as fread:
        for line in fread:
            [content_file_id, content_file_name] = line.strip().split('\t')
            dayID = content_file_id.split('_')[1]
            line_offset_dict[content_file_id] = get_line_offset(join(join(content_path, 'day_' + dayID), content_file_name))
    return line_offset_dict

