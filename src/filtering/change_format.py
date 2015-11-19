import sys
sys.path.append("../")
import logging
import config
import global_options
import re
import json
import gzip
from os import listdir, system, stat, path
import sys

def change_format(folder_path, fname):
    fwrite_name = 'ch_' + fname
    fwrite = open(path.join(folder_path, fwrite_name), 'w')
    print fwrite, folder_path
    with open(path.join(folder_path, fname), 'r') as fread:
        for line in fread:
            # print line
            data = json.loads(line.strip())
            for string, value in data.items():
                for cfile_id, offsets in value.items():
                    fwrite.write(string + '\t' + str(cfile_id) + '\t' + str(offsets).lstrip('[').rstrip(']') + '\n')

def change_id_cfile_format():
    for fname in listdir(config.CONTEXT_FOLDER):
        if re.match(config.ID2CFILE + '*', fname) and \
        stat(path.join(config.CONTEXT_FOLDER, fname)).st_size != 0:
            fwrite = open(path.join(config.CONTEXT_FOLDER, 'ch_' + fname), 'w')
            with open(path.join(config.CONTEXT_FOLDER, fname), 'r') as fread:
                for line in fread:
                    data = json.loads(line.strip())
                    for key, value in data.items():
                        fwrite.write(key + '\t' + value + '\n')


def main_sort(file_prefix):
    '''
    Sorts all files in master path with the provided prefix
    :param file_prefix: prefix for all files that need sorting
    :return: sorts file in place
    '''
    count = 0
    for num in config.DIRECTORY_NUM:
        logging.info('starting sorting on directory num: ' + str(count) + '/' + str(config.DIRECTORY_COUNT))
        for fname in listdir(path.join(config.MASTER_PATH, 'data_%s' % str(num))):
            if re.match(file_prefix, fname):
                fpath = path.join(config.MASTER_PATH, path.join('data_%s' % str(num), fname))
                try:
                    if global_options.compress_flag:
                        decomp = open(fpath + '_decomp', 'wb')
                        comp = gzip.open(fpath, 'rb')
                        decomp.writelines(comp)
                        decomp.close()
                        comp.close()
                        system("sort -t$'\t' -k1,1 -k2,2 -o " + fpath + '_decomp' + ' ' + fpath + '_decomp')
                        decomp = open(fpath + '_decomp', 'rb')
                        comp = gzip.open(fpath, 'wb')
                        comp.writelines(decomp)
                        comp.close()
                        decomp.close()
                        system('rm ' + fpath + '_decomp')
                    else:
                        system("sort -t$'\t' -k1,1 -k2,2 -o " + fpath + ' ' + fpath)
                except:
                    logging.error(sys.exc_info()[0])
                    print sys.exc_info()[0:3]
                    continue
                logging.info('done sorting ' + fname + ' in folder ' + str(num))
        count = count + 1

if __name__ == '__main__':
    # main()
    change_id_cfile_format()



