from os import system
import gzip

def compress(fread_path, fwrite_path, remove = False):
	orig = open(fread_path, 'rb')
	comp = gzip.open(fwrite_path, 'wb')
	comp.writelines(orig)
	orig.close()
	comp.close()
	if remove:
		system('rm ' + fread_path)

def decompress(fread_path, fwrite_path, remove = False):
	comp = gzip.open(fread_path, 'rb')
	decomp = open(fwrite_path, 'wb')
	decomp.writelines(comp)
	decomp.close()
	comp.close()
	if remove:
		system('rm ' + fread_path)
