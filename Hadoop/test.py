#!/usr/bin/python

f = open("stopwords.txt",'r')
i = 1 
for line in f:
	if (i % 2 != 0):
		line = line.strip()
		print line
	i = i + 1 
f.close()