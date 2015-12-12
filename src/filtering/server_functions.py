__author__ = 'Sakshi Jain'

'''
Date: Feb 15 7:48

The various filtering options for analysis after the crude filtering

0.1: Enc


1. strings that appear just once in an individual's network traffic
2. strings sent to a server unique to a user
3. strings sent to just one server by a user
4. strings common to multiple users

'''
from collections import Counter
import re
from os.path import exists, dirname, join
from os import listdir, makedirs, stat, path
import config


''' Reading conn file for each user'''


def get_user_fields(path):
    fields = ['orig_h', 'orig_p', 'resp_h', 'resp_p']  # , 'cookie']
    outputList = []
    connFile = open(join(path, 'conn.log'))
    for line in connFile:
        fieldList = {}
        values = line.split('\t')
        if (line[0] == '#') or (values[9] + values[10] == 0):
            continue
        fieldvalues = [values[2], values[3], values[4], values[5]]  # , values[20]]
        for i, field in enumerate(fields):
            fieldList[field] = fieldvalues[i]
        outputList.append(fieldList)
    return outputList


def unique_elements(lsts):
    c = Counter(num for lst in lsts for num in lst)
    common = set(x for x, v in c.items() if v > 1)
    result = []
    for lst in lsts:
        result.append(lst - common)
    return result


def ensure_dir(f):
    d = dirname(f)
    if not exists(d):
        makedirs(d)


def print_servers_list(pathList, list_):
    for i in xrange(len(pathList)):
        fpath = join(config.CONTEXT_FOLDER, 'uniqueServers_%s' % str(i))
        ensure_dir(fpath)
        f = open(fpath, 'w')
        for serverIP in list_[i]:
            f.write(serverIP + '\n')
        f.close()


def get_unique_servers(user_paths):
    """
    This function prints the list of unique servers for each user in files like uniqueServers_0
    :rtype : None
    """
    users_server_list = []
    for user in user_paths:
        days = listdir(user)
        serverList = []
        for day in days:
            if re.match('day_*', day) == None:
                continue
            path = join(user, day)
            fieldsSet = get_user_fields(path)
            serverSet = set([i['resp_h'] for i in fieldsSet]) - set([i['orig_h'] for i in fieldsSet])
            serverList += serverSet
        serverList = set(serverList)
        users_server_list.append(serverList)
    users_server_list = unique_elements(users_server_list)
    print_servers_list(user_paths, users_server_list)


if __name__ == '__main__':
    pass


