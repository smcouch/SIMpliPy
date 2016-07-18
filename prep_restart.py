import sys
import os
#this preps and submits a flash restart

prog = sys.argv[1] #e.g. s12
model = sys.argv[2] #e.g. NW,NWEP,GREP
parts = sys.argv[3] # 0 or 1, for particles

print parts, parts=="1"

#file containing restart number
restartnumberfile = prog+"_"+model+"_"+"restartnumber"
file = open(restartnumberfile,'r')
restartnumber = file.readline()
file.close()

#check is last run was successful, look in log file
command = "tail -1 "+prog+"_"+model+".log > mycheck"
os.system(command)
file = open("mycheck")
data = file.readline()
file.close()

correctstring = "LOGFILE_END: FLASH run complete."

if data[len(data)-len(correctstring)-1:len(data)-1] == correctstring:
    #success
    filename = "manualok_"+prog+"_"+model
    file = open(filename,'r')
    data = int(file.readline())
    file.close()
    command = "cat resetrun >"+filename
    os.system(command)
    if data==1:
        filename = prog+"_"+model+"_runok"
        file = open(filename,'w')
        file.write("1")
        file.close()
    else:
        filename = prog+"_"+model+"_runok"
        file = open(filename,'w')
        file.write("0")
        file.close()        
else:
    filename = prog+"_"+model+"_runok"
    file = open(filename,'w')
    file.write("0")
    file.close()
    
    

command = "rm mycheck"
os.system(command)

#prep qsub file
command = "sed 's/restartnumber/"+restartnumber+"/g' submit_"+prog+"_"+model+"_default.qsub > submit_"+prog+"_"+model+".qsub"
os.system(command)

#prep params file
command = "ls -lrt ./out_"+prog+"_"+model+"/*chk* > lsdata"
os.system(command)
command = "tail -1 lsdata > lastchk"
os.system(command)
file = open("lastchk",'r')
checkpointline = file.readline()
checkpointnumber = int(checkpointline[len(checkpointline)-5:len(checkpointline)-1])
file.close()
command = "rm lsdata; rm lastchk"
os.system(command)

command = "ls -lrt ./out_"+prog+"_"+model+"/*"+model+"_hdf5_plt* > lsdata"
os.system(command)
command = "tail -1 lsdata > lastplot"
os.system(command)
file = open("lastplot",'r')
plotfileline = file.readline()
plotfilenumber = int(plotfileline[len(plotfileline)-5:len(plotfileline)-1])+1
file.close()
command = "rm lsdata; rm lastplot"
os.system(command)

if parts == "1":
    command = "ls -lrt ./out_"+prog+"_"+model+"/*part* > lsdata"
    os.system(command)
    command = "tail -1 lsdata > lastpart"
    os.system(command)
    file = open("lastpart",'r')
    partfileline = file.readline()
    partfilenumber = int(partfileline[len(partfileline)-5:len(partfileline)-1])+1
    file.close()
    command = "rm lsdata; rm lastpart"
    os.system(command)
    
command = "sed 's/checkpointnumberhere/"+str(checkpointnumber)+"/g' "+prog+"_"+model+"_default.par > temp.dat"
os.system(command)
command = "sed 's/plotfilenumberhere/"+str(plotfilenumber)+"/g' temp.dat > temp2.dat"
os.system(command)
if parts == "1":
    command = "sed 's/particlefilenumberhere/"+str(partfilenumber)+"/g' temp2.dat > "+prog+"_"+model+".par"
    os.system(command)
else:
    command = "cp temp2.dat ./"+prog+"_"+model+".par"
    os.system(command)
    
command = "rm temp.dat; rm temp2.dat"
os.system(command)

file = open(restartnumberfile,'w')
file.write(str(int(restartnumber)+1))
file.close()
