import sys
import os
from os import listdir
from os.path import isfile, join

if len(sys.argv) < 2:
	print("Please enter number of reducers as cmd line argument")
	sys.exit()

path="/users/aakashsh/cpu_stats"

exists = os.path.isdir(path)

if not exists:
	sys.exit()

files = [join(path,f) for f in listdir(path) if isfile(join(path, f))]
files.sort()

num = 0
cpu = 0
iowait = 0
cpu_mapper = 0
cpu_reducer = 0
iowait_mapper = 0
iowait_reducer = 0

num_reducer = int(sys.argv[1])

for f in files:
	with open(f, 'r') as statFile:
			
		line = statFile.readline()
		lst =  line.split(' ')
		cpu = cpu + int(lst[1])
		iowait = iowait + int(lst[4])
		
		if num < num_reducer:
			cpu_reducer = cpu_reducer + int(lst[1])
			iowait_reducer = iowait_reducer + int(lst[4])
		else:
			cpu_mapper = cpu_mapper + int(lst[1])
			iowait_mapper = iowait_mapper + int(lst[4])
		
		num = num + 1
	statFile.close()

print("CPU average = " + str(cpu/num))
print("I/O wait average = " + str(iowait/num))
print("Mapper CPU average = " + str(cpu_mapper/(num-num_reducer)))
print("Mapper I/O wait average = " + str(iowait_mapper/(num-num_reducer)))

if num_reducer != 0: 
	print("Reducer CPU average = " + str(cpu_reducer/num_reducer))
	print("Reducer I/O wait average = " + str(iowait_reducer/num_reducer))

