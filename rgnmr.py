#For the problem I will connect with spark library to do the MapReduce.

'''
Define names
chapter 1: 1_2_1_character
chapter 2: 1_2_2_character
chapter 3: 1_3_1_character
chapter 4: 1_3_2_character
chapter 5: 1_4_1_character
chapter 6: 1_4_2_character
chapter 7: 2_3_1_character
chapter 8: 2_3_2_character
chapter 9: 2_4_1_character
chapter 10:2_4_2_character
chapter 11:3_4_1_character
chapter 12:3_4_2_character

Define Folder name
maptest: Master folder which contain all the files
1: first node, will find how many As in there
2: second node, will find how many Bss in there
3: third node, will find how many Cs in there
4: fourth node, will find how many Ds in there
'''

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
                        filegen=open(filename,'w')
                        generate = str(random.randint(0,9000))
                        #sys.stdout= open(filename,'w')#create a txt file
                        filegen.write(generate) #store all the numbers which generate earier to the txt  file
                        filegen.close()
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
    print(results)
    print(number_result)
#print(type(file_name))

    return (results,file_name)

#second part: shuffe(naive scheme)

def Encoding(val1,val2,val3):
    results=val1
    file_name=val2
    thread_name =val3
        #for thread_name in results:
    thread_path="/Users/xiaoran/dropbox/cache_map/"+thread_name
    location=os.listdir(thread_path)
    print("Helloworld"+thread_path)
    for file_copy in location:
        pause_time=-math.log(random.uniform(0, 1))
        if len(file_copy) > 7:
            first=file_copy[0]
            third=file_copy[2]
            fifth=file_copy[4]
            seventh=file_copy[6]
            file_location='/Users/xiaoran/dropbox/cache_map/coded_'+seventh
            print(first, third, fifth, seventh)
            if (seventh != first and seventh!=third and (fifth =="1")): # if equal to 1 then copy file from location equal to the value from first
                shutil.copy('/Users/xiaoran/dropbox/cache_map/coded_'+first+'/'+file_copy, file_location)
            elif (seventh != first and seventh!=third and (fifth =="2")): # if equal to 2 then copy file from location equal to the value from second
                shutil.copy('/Users/xiaoran/dropbox/cache_map/coded_'+third+'/'+file_copy, file_location)
    time.sleep(pause_time)
######################
if __name__ == "__main__":
    path ="/Users/xiaoran/dropbox/cache_map"
    folder_path="/Users/xiaoran/dropbox/cache_map/coded_master"
    def moveFileto(sourceDir,  targetDir):
        shutil.copy(sourceDir,  targetDir)
    
    if os.path.exists(folder_path): #Delete the folder if the folder exist
        shutil.rmtree(folder_path)
    print("How many user you are trying to have?")
    user=input()
    users=int(user)
    counter=int(user)
    userss=str(user)
    print("you are going to have " +user + " users for this simulation" )

#filecreating()#creating files and folder

    #def filecreating():
    files= os.listdir(path) #get file name under the folder
    # define the access rights
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
    #Encoding(temp1,temp2)
    pool=multiprocessing.Pool(processes = users)
    start =datetime.datetime.now()
    for i in user_name:
        print("Server current running " + i)
        print(datetime.datetime.now())
        pool.apply_async(Encoding(user_name,temp2,i), args=(i, ))
        #pool.apply_async(Decoding(user_name),args=(i,))
    pool.close()
    pool.join()
    print("Done")
    finish=datetime.datetime.now()
    print("For " + userss +" users spent \{}".format(finish-start))
