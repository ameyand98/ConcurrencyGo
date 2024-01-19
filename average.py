import sys

f = open('out.txt', 'rU')    
lines = f.readlines()    
f.close()

size =  len(lines)
sum=0
for line in lines:
    sum = sum + float(line.rstrip())

avg = sum / float(size)    
print avg,