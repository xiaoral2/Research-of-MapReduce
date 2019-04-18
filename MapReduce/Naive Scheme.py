
import timeit
import sys
import os
import numpy as np
import pandas as pd
#import dispy
import shutil
import re
import time
import threading#, Queue
import random
import math
#from multiprocessing import Process,Lock,Pool
import time,datetime
import multiprocessing

def filecreator():
    ####finish created director
    for i in range(users):
        for j in range(users):
            for k in range(2):
                for l in range(users):
                    if i != j and i<j:
                        filename=str(i+1)+"_"+str(j+1)+"_"+str(k+1)+"_"+str(l+1)+".txt"
                        with open (filename,'w') as filegen:
                            generate = str(random.randint(0,9000))
                        #sys.stdout= open(filename,'w')#create a txt file
                            filegen.write(generate) #store all the numbers which generate earier to the txt  file
#sys.stdout.close()


############### Part 3  ##################
############# Mapping Code  ##############
def Mapping():
    files= os.listdir(folder_path) #get file name under the folder
    results = set() #find folder name
    number_result=set()
    for files_names in files:
        files_name=files_names.split('.')
        file_name=files_name[0].split('_')
        if len(files_names) > 4:
            first = "coded_"+file_name[0]
            third = "coded_"+file_name[1]
            results.add(first)
            results.add(third)
            number_first=file_name[0]
            number_third=file_name[1]
            number_result.add(number_first)
            number_result.add(number_third)
    results=list(results)
    number_result=list(number_result)
    #print(number_result)
    os.chdir(path)#change the director for the folder path
    #print(results)
    for folder_name in results:#delete folders that exists from previous run
        if os.path.exists(path):
            #print(folder_name)
            shutil.rmtree(folder_name)

    for folder_name in results:#create new folders for the number desired
        if os.path.isdir(path):
            os.mkdir(os.path.join(path, folder_name))
            new_path=path+'/'+folder_name
            if os.path.isdir(path):
                os.mkdir(os.path.join(new_path, folder_name))
    counter=0
    
    for files_names in files:
        files_name=files_names.split('.')
        file_name=files_name[0].split('_')
        if len(files_names) > 4:
            first="coded_"+file_name[0]
            third="coded_"+file_name[1]
            shutil.copy('/Users/xiaoran/dropbox/cache_map/coded_master/'+files_names, '/Users/xiaoran/dropbox/cache_map/'+first)
            shutil.copy('/Users/xiaoran/dropbox/cache_map/coded_master/'+files_names, '/Users/xiaoran/dropbox/cache_map/'+third)
                #print(results)
#print(number_result)
#print(type(file_name))
    return (results,number_result)

#second part: shuffe(naive scheme)
def shufgen(val1):
    L1=val1
    L2=L1[:]
    L3=[] #除去两个随机
    L4=[] #
    L5=[]
    L6=[]
    D1={}
    temp=[]
    for i in range(len(L1)):
        L2.pop(i)
        L2=random.sample(L2,len(L2))
        D1[L1[i]]=L2
        L2=L1[:]
    for key,value in D1.items():
        temp=value[:]
        for i in range(len(temp)):
            a=temp.pop(i)
            temp=random.sample(temp,len(temp))
            for j in range(len(temp)):
                if [key,a,temp[j]] not in L4 and [key,temp[j],a] not in L4:
                    L4.append([key,a,temp[j]])
            temp=value[:]
        L4=random.sample(L4,len(L4))
        L5.append([key,L4])
        L4=[]
    return L5

def Encoding(val1):
    t1=time.time()
    counter=0
    jeffdean1=val1
    foldervalue=jeffdean1[0]
    jeffdeanlist=jeffdean1[1]
    path_folder="/Users/xiaoran/dropbox/cache_map/coded_"+jeffdean1[0]
    os.chdir(path_folder)
    folder_naming=[]
    for index, i in enumerate(jeffdeanlist):
        value1=i[1]
        value2=i[2]
        pause_time=-math.log(random.uniform(0, 1))/10 #exponetial distribution
        pause_uni_time=random.uniform(0, 0.2)#uniform distribution
        pratical=0.05+random.uniform(0, 0.1)
        pratical_exp=0.05+(-math.log(random.uniform(0, 1))/10)
        file_trans_location='/Users/xiaoran/dropbox/cache_map/coded_'+value2
        if int(jeffdean1[0]) > int(value1):
            file_named_1=value1+"_"+jeffdean1[0]+"_1_"+value2+".txt"
            file_named_2=value1+"_"+jeffdean1[0]+"_2_"+value2+".txt"
            counter+=2
            time.sleep(2*pause_time)
            shutil.copy('/Users/xiaoran/dropbox/cache_map/coded_'+jeffdean1[0]+'/'+file_named_2,file_trans_location)
            shutil.copy('/Users/xiaoran/dropbox/cache_map/coded_'+value1+'/'+file_named_1,file_trans_location)
        else:
            file_named_1=jeffdean1[0]+"_"+value1+"_1_"+value2+".txt"
            file_named_2=jeffdean1[0]+"_"+value1+"_2_"+value2+".txt"
            counter+=2
            time.sleep(2*pause_time)
            shutil.copy('/Users/xiaoran/dropbox/cache_map/coded_'+jeffdean1[0]+'/'+file_named_1,file_trans_location)
            shutil.copy('/Users/xiaoran/dropbox/cache_map/coded_'+value1+'/'+file_named_2,file_trans_location)
    t2=time.time()
#    print(t2-t1)

######################
if __name__ == "__main__":
    forcounter=0
    total=0
    while forcounter < 100:
        path ="/Users/xiaoran/dropbox/cache_map"
        folder_path="/Users/xiaoran/dropbox/cache_map/coded_master"
        def moveFileto(sourceDir,  targetDir):
            shutil.copy(sourceDir,  targetDir)
        
        if os.path.exists(folder_path): #Delete the folder if the folder exist
            shutil.rmtree(folder_path)
        print("How many user you are trying to have?")
        user=20
#        user=input()
        users=int(user)
        counter=int(user)
        userss=str(user)
        print("you are going to have " +userss + " users for this simulation" )
        files= os.listdir(path) #get file name under the folder
        access_rights = 0o755
        try:
            os.mkdir(folder_path, access_rights)
        except OSError:
            print ("Creation of the directory %s failed" % folder_path)
        else:
            print ("Successfully created the directory %s" % folder_path)
        os.chdir(folder_path)#change the director for the folder path
        filecreator()
        user_name, temp2 = Mapping()
        everything=shufgen(temp2)
        start=time.time()
        threads_encode = []
        for i in everything:
            jeffdean=i[0]
            t = threading.Thread(target=Encoding, args=(i,))
            threads_encode.append(t)
            t.start()
            print("current running server" + jeffdean)
        for x in threads_encode:
            x.join()
        print("Done")
        finish=time.time()
        excute_time=finish-start
        print("For " + userss +" users spent"+str(excute_time))
        forcounter+=1
        total=total+excute_time
    average=total/forcounter
    print("average spent" +str(average))
