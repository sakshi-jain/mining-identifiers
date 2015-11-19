import sys
sys.path.append("../")
import config
from os import listdir, stat, path, system
import global_options
import re
import gzip
import python_utils
import logging

def remove_non_persistent_strings(fread_path, fwrite_path):
	if global_options.compress_flag:
		fwrite = open(fwrite_path, 'wb')
		fmarker = gzip.open(fread_path, 'rb')
	else:
		fwrite = open(fwrite_path, 'wb')
		fmarker = open(fread_path, 'rb')

	npers_count = 0

	if global_options.compress_flag:
		python_utils.decompress(fread_path, fread_path + '_decomp')
		if stat(fread_path + '_decomp').st_size == 0:
			logging.error("empty file " + fread_path)
			return npers_count
			system('rm ' + fread_path + '_decomp')
	else:
		if stat(fread_path).st_size == 0:
			logging.error("empty file " + fread_path)
			return npers_count

	if global_options.compress_flag:
		fread = gzip.open(fread_path, 'rb')
	else:
		fread = open(fread_path, 'r')

	line = fread.readline()
	[string, cfile_id, offset] = line.strip().split('\t')
	cString = string
	cDay = cfile_id.split('_')[1]
	str_start = 0
	mode = 'un_decided'
	cline_count = 1
	byte_offset = len(line)
	for line in fread:
		[string, cfile_id, offset] = line.strip().split('\t')
		if string != cString:
			if mode == 'selected':
				fmarker.seek(str_start)
				str_ = fmarker.read(byte_offset - str_start)
				fwrite.write(str_)
				mode = 'un_decided'
			cline_count = 1
			cString = string
			cDay = cfile_id.split('_')[1]
			str_start = byte_offset
			npers_count += cline_count
			byte_offset += len(line)

		else:
			byte_offset += len(line)
			cline_count += 1
			if mode == 'selected':
				continue
			chk_day = cfile_id.split('_')[1]
			if chk_day != cDay:
				mode = 'selected'

	if mode == 'selected':
		fmarker.seek(str_start)
		str_ = fmarker.read(byte_offset - str_start)
		fwrite.write(str_)
	else:
		npers_count += cline_count
	
	fwrite.close()
	fmarker.close()
	return npers_count


def openfiles(folder, fwrite_prefix):
	openList = {}
	for fname in listdir(folder):
		if re.match(config.AFT_CONNECTION_FILT, fname) and stat(path.join(folder, fname)).st_size != 0:
			userID = int(fname.split('_')[-1])
			if global_options.compress_flag:
				openList[userID] = [gzip.open(path.join(folder, fname), 'rb'), gzip.open(path.join(folder, fwrite_prefix + fname), 'ab')]
			else:
				openList[userID] = [open(path.join(folder, fname), 'rb'), open(path.join(folder, fwrite_prefix + fname), 'ab')]

	return openList

def remove_common_strings(folder_path, fwrite_prefix):
	common_count = 0
	files = openfiles(folder_path, fwrite_prefix)
	if len(files) == 0:
		return common_count
	cLines = {k:f[0].readline() for k,f in files.items()}
	cStrings = {k:line_.split('\t')[0] for k,line_ in cLines.items()}

	while True:
		if len(cStrings) == 0:
			return
		cMin = min(cStrings.values())
		minIndex = [k for k,v in cStrings.items() if v == cMin]
		if len(minIndex) == 1:
			fwrite = files[minIndex[0]][1]
			fwrite.write(cLines[minIndex[0]])
			file_ = files[minIndex[0]][0]
			while True:
				line = file_.readline()
				if line == '':
					del cLines[minIndex[0]]
					del cStrings[minIndex[0]]
					# print 'deleting', len(cStrings)
					break
				next_str = line.split('\t')[0]
				if next_str != cMin:
					cLines[minIndex[0]] = line
					cStrings[minIndex[0]] = next_str
					break
				else:
					fwrite.write(line)
		else:
			for index in minIndex:
				file_ = files[index][0]
				while True:
					line = file_.readline()
					if line == '':
						del cLines[index]
						del cStrings[index]
						break
					next_str = line.split('\t')[0]
					if next_str != cMin:
						cLines[index] = line
						cStrings[index] = next_str
						break
					else:
						common_count += 1
	return common_count

def merge_two_filters(folder_path):
	for fname in listdir(folder_path):
		if re.match(config.AFT_CONNECTION_FILT + '*', fname):
			cfilt_file = path.join(folder_path, config.AFT_UNIQUENESS_FILT + fname)
			dfilt_file = path.join(folder_path, config.AFT_PERSISTENCE_FILT + fname)
			if path.exists(cfilt_file) and path.exists(cfilt_file):
				system("comm -1 -2 " + cfilt_file + " " + dfilt_file + " > " + 
					path.join(folder_path, config.AFT_UNIQUENESS_FILT + config.AFT_PERSISTENCE_FILT + fname))

def main(bin, command):
	filelist = listdir(bin)
		
	''' uniqueness filtering on user_ files'''
	if command == 'unique':
		fwrite_prefix = config.AFT_UNIQUENESS_FILT
		remove_common_strings(bin, fwrite_prefix)
		logging.info('Completed uniqueness filtering for: '+ bin)
		return


	''' persistence filtering on user_ files '''
	if command == 'persistent':
		npers_count = 0
		for fname in filelist:
			if re.match(config.AFT_UNIQUENESS_FILT, fname):
				userID = fname.split('_')[-1]
				fread_path = path.join(bin, fname)
				fwrite_path = path.join(bin, config.AFT_PERSISTENCE_FILT + config.AFT_CONNECTION_FILT + userID)
				npers_count += remove_non_persistent_strings(fread_path, fwrite_path)
			logging.info('Completed persistence filtering for: ' + path.join(bin, fname))
		return


def test_cmon_filt():
	remove_common_strings(path.join(config.MASTER_PATH, '/data_65/'))

if __name__ == '__main__':
	test_cmon_filt()

		
