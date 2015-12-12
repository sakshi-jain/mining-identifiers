'''
This file applies three connection based filters on content of each user:
a. Removes content from unique servers
b. Removes encrypted content
c. Removes server side content

The output strings for each user are written to folders based on the starting
 character of each string.
'''
import sys
sys.path.append("../")
from os import listdir, makedirs, stat, path
from os.path import exists, dirname
import re
import gzip
import config
import logging
import global_options
#!/usr/bin/python

# Filter:
#   - Multicast traffic on port 5353
#   - All other local traffic: ip.addr==192.168.1.0/24 and !(ip.addr==192.168.1.29)

def ensure_dir(f):
    d = dirname(f)
    if not exists(d):
        makedirs(d)

output_dirs = []

##############################################################################
                        #SERVER LEVEL FILTERS#
##############################################################################


def check_unique_server(servers, fname):
    '''
    Return True if the server corresponding to the current file is shared with
    at least one other user.
    '''
    if servers == False:
        return True
    for server in servers:
        if fname.find(str(server) + ":") != -1:
            return False
    return True



def check_host(fname):
    '''
    Return True is originating at the host
    '''
    if fname.find("_orig.dat") != -1:
        return True
    # print fname, 'server orig'
    return False



def check_encryption(fname):
    '''
    Return True if unencrypted
    '''
    list_ssl = [':443_', ':22_']
    for item in list_ssl:
        if fname.find(item) != -1:
            return False
    return True



def check_connection_filters(unique_servers, fname, contents_path):
    encryption_bool = check_encryption(fname)
    host_bool = check_host(fname)
    unique_server_bool = check_unique_server(unique_servers, fname)
    return (encryption_bool and host_bool and unique_server_bool)

##############################################################################

def getDirName(num):
    return "data_" + str(num)

#python_scripts/data/data_A/user_1
def createDirectories(userID):
    for num in config.DIRECTORY_NUM:
        if not exists(config.MASTER_PATH):
            makedirs(config.MASTER_PATH)
        filepath = path.join(path.join(config.MASTER_PATH, getDirName(num)), config.AFT_CONNECTION_FILT + str(userID))
        ensure_dir(filepath)
        if global_options.compress_flag:
            output_dirs.append(gzip.open(filepath, 'ab'))
        else:
            output_dirs.append(open(filepath, 'ab'))


def close_output_files():
    global output_dirs 
    for file in output_dirs:
        file.close()
    output_dirs = []



def get_metadata(fread, file_id):
    '''
    Parses the input file into sliding windows of length 8 and prints them
    along with metadata into bins.

    :param fread: path of the content file to be read and parsed
    :param file_id: unique id for each content file
    :return: None. The output of the form "<string_value>, <file_id>, <byte_offset>"
        is printed to a file depending on the start character of the string
    '''
    byte_offset = 0
    byte = fread.read(8)
    slidingWin = ''
    while True:
        slidingWin += byte
        str_ = str(slidingWin.encode("hex"))
        num = int(str_[0:2], 16)
        output_dirs[num].write(str_ + '\t' + str(file_id) + '\t' + str(byte_offset) + '\n')
        slidingWin = slidingWin[1:]
        byte = fread.read(1)
        byte_offset += 1
        if byte == '':
            return

def checkPrintableAscii(byte_ascii):
    return ((byte_ascii >= 32 and byte_ascii <= 126) or byte_ascii == 7 or byte_ascii == 10 or byte_ascii == 13)


def get_metadata_printable(fread, file_id):
    '''
    Parses the input file into sliding windows of length 8 and prints them
    along with metadata into bins. Non printable characters are not printed in
    this function.

    :param fread: path of the content file to be read and parsed
    :param file_id: unique id for each content file
    :return: None. The output of the form "<string_value>, <file_id>, <byte_offset>"
        is printed to a file depending on the start character of the string
    '''
    slidingWin = ''
    count = -1
    byte_offset = 0
    while True:
        byte = fread.read(1)
        if byte == '':
            return
        count += 1
        if checkPrintableAscii(ord(byte)):
            slidingWin += byte
            if len(slidingWin) == 8:
                str_ = str(slidingWin.encode("hex"))
                num = int(str_[0:2], 16)
                output_dirs[num].write(str_ + '\t' + str(file_id) + '\t' + str(byte_offset) + '\n')
                slidingWin = slidingWin[1:]
                byte_offset += 1
        else:
            byte_offset = count + 1
            slidingWin = ''



def get_content_files(serverList, contents_path):
    logging.info('for the user ' + contents_path)
    returnlist = []
    files_list = listdir(contents_path)
    logging.info("total content files before filtering :" + str(len(files_list)))
    for fname in files_list:
        if (re.match('content*',fname)):
            try:
                if (stat(path.join(contents_path, fname)).st_size != 0):
                    if (check_connection_filters(serverList, fname, contents_path) == True):
                        returnlist.append(fname)
            except:
                logging.error(fname + ' error opening this file')
    logging.info("total content files after filtering : " + str(len(returnlist)))
    return returnlist



def get_id_to_content(userID, dayID, file_list):
    mapping = {str(userID) + '_' + str(dayID) + '_' + str(i) : fname for \
     i, fname in enumerate(file_list)}
    with open(path.join(config.CONTEXT_FOLDER, config.ID2CFILE + str(userID)), 'a') as fwrite:
        for id_, cfile in mapping.items():
            fwrite.write(id_ + '\t' + cfile + '\n')
    return mapping



def main(contents_path = './', userID = 0, dayID = 0, unique_servers = False):
    files_list = get_content_files(unique_servers, contents_path)
    mapping = get_id_to_content(userID, dayID, files_list)

    createDirectories(userID)
    for file_id, fname in mapping.items():
        fread = open(path.join(contents_path, fname))
        if global_options.printable_only:
            get_metadata_printable(fread, file_id)
        else:
            get_metadata(fread, file_id)
        fread.close()
    close_output_files()



if __name__ == '__main__':
    main()




