import sys
import os
from os import listdir
from os.path import isfile, join

path="/users/aakashsh/cpu_stats"

exists = os.path.isdir(path)

if not exists:
	sys.exit()

files = [join(path,f) for f in listdir(path) if isfile(join(path, f))]

num = 0
cpu = 0
iowait = 0

for f in files:
	with open(f, 'r') as statFile:
		num = num + 1
		line = statFile.readline()
		lst =  line.split(' ')
		cpu = cpu + int(lst[1])
		iowait = iowait + int(lst[4])
	statFile.close()

print("CPU average = " + str(cpu/num))
print("I/O average = " + str(iowait/num))


