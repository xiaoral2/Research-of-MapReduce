import os
import shutil
import math
import random
import time
import threading
from FileProcessor import FileProcessor
from multiprocessing import Queue

times = [0.1, -math.log(random.uniform(0, 1)) / 10, random.uniform(0, 0.2), 0.05 + random.uniform(0, 0.1),
            0.05 + (-math.log(random.uniform(0, 1)) / 20)]

class FileTransfer(object):
    def __init__(self, users, folder_path, path, num_of_links):
        #self.file_dir = file_dir
        self.users = users
        self.folder_path=folder_path
        self.path=path

        self.num_of_links = num_of_links

        self.encoding_time = 0

        self.avg_encoding_time = 0

################ Part 1  #################
############# Mapping Code  ##############
    def Mapping(self):
        '''
        Map function assign files to each user
        '''
        new_path=self.folder_path
        files= os.listdir(new_path) #get file name under the folder
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
        os.chdir(self.path)#change the director for the folder path

        for folder_name in results:#delete folders that exists from previous run
            if os.path.exists(self.path + folder_name):
                shutil.rmtree(self.path + folder_name)
        for folder_name in results:#create new folders for the number desired
            if os.path.isdir(self.path):
                os.mkdir(os.path.join(self.path, folder_name))

        for files_names in files:
            files_name=files_names.split('.')
            file_name=files_name[0].split('_')
            if len(files_names) > 4:
                first="coded_" + file_name[0] + "/Map/"
                third="coded_" + file_name[1] + "/Map/"
                if not os.path.isdir(self.path+first):
                    os.mkdir(self.path + first)
                shutil.copy(new_path+files_names, self.path+first)

                if not os.path.isdir(self.path + third):
                    os.mkdir(self.path + third)
                shutil.copy(new_path+files_names, self.path+third)

        return results,number_result
#['coded_1', 'coded_3', 'coded_2', 'coded_4'] ['3', '1', '4', '2']

    def Call_File_Rename(self, everything):
        threads_encode = []
        for i in everything:
            t = threading.Thread(target=self.modify_files, args=(i,))
            threads_encode.append(t)
            t.start()

            # Wait until all workers are done with their job
        for x in threads_encode:
            x.join()
        return

    def modify_files(self, i):
        worker_id = i[0]
        path_folder = self.path + "coded_" + str(worker_id) + '/'
        # print("worker", worker_id, "starts")
        fileprocess = FileProcessor(path_folder + "Map/", self.users, self.num_of_links, self.path)
        fileprocess.create_pair_files('', worker_id)
        fileprocess.collect_file_data()
        return

        ############### Part 51 ##################
        ##### Shuffling Code for generate ########
    def shufgen(self):
        L1 = []
        user = int(self.users)
        for i in range(user):
            i = i + 1
            L1.append(i)
        # L1=number_result
        L2 = L1[:]
        L3 = []  # 除去两个随机
        L4 = []  #
        L5 = []
        L6 = []
        D1 = {}
        temp = []
        # print(L1)
        for i in range(len(L1)):
            L2.pop(i)
            L2 = random.sample(L2, len(L2))
            D1[L1[i]] = L2
            L2 = L1[:]
        for key, value in D1.items():
            temp = value[:]
            for i in range(len(temp)):
                a = temp.pop(i)
                temp = random.sample(temp, len(temp))
                for j in range(len(temp)):
                    if [key, a, temp[j]] not in L4 and [key, temp[j], a] not in L4:
                        L4.append([key, a, temp[j]])
                temp = value[:]
            L4 = random.sample(L4, len(L4))
            L5.append([key, L4])
            L4 = []
        return L5

    ################ Part 3  #################
############ Call Function  ##############  naive
    def Call_naive_function(self, everything):
        threads_encode = []
        enout_que = Queue()
        enc_start = time.time()
        for i in everything:
            # print("Server current encoding coded" + str(i[0]))
            jeffdean = i[0]
            t = threading.Thread(target=self.Naive_Encoding, args=(i, enout_que,))
            threads_encode.append(t)
            t.start()

            # Wait until all workers are done with their job
        for x in threads_encode:
            x.join()

        enc_finish = time.time()

        self.encoding_time = enc_finish - enc_start

        en_out = 0
        while not enout_que.empty():
            dout = enout_que.get()
            en_out += dout
        enout_que.close()
        self.avg_encoding_time = en_out / self.users
        # self.avg_encoding_time =

################ Part 4  #################
############# Naive Code  ################
    def Naive_Encoding(self,everything, que):
        start = time.time()
        counter=0
        jeffdean1=everything
        foldervalue=jeffdean1[0]
        jeffdeanlist=jeffdean1[1]
        #for thread_name in results:
        thread_path = self.path + "coded_"
        # os.chdir(thread_path)
        folder_naming=[]


        for index, i in enumerate(jeffdeanlist):
            value1 = str(i[1])
            value2 = str(i[2])


            pause_time = -math.log(random.uniform(0, 1)) / 10  # exponetial distribution
            pause_uni_time = random.uniform(0, 0.2)  # uniform distribution
            pratical = 0.05 + random.uniform(0, 0.1)
            pratical_exp = 0.05 + (-math.log(random.uniform(0, 1)) / 20)

            file_trans_location = self.path + "coded_"  + value2

            if jeffdean1[0] > int(value1):
                file_named_1=value1+"_"+ str(jeffdean1[0]) + "_1_" + value2+".txt"
                file_named_2=value1+"_"+str(jeffdean1[0])  + "_2_" + value2+".txt"
                counter+=2
                time.sleep(2 * pause_time)
                shutil.copy(thread_path + str(jeffdean1[0]) + '/' + file_named_2, file_trans_location)
                shutil.copy(thread_path + value1 + '/' + file_named_1, file_trans_location)
            else:
                file_named_1 = str(jeffdean1[0]) + "_" + value1 + "_1_" + value2 + ".txt"
                file_named_2 = str(jeffdean1[0]) + "_" + value1 + "_2_" + value2 + ".txt"
                counter+=2
                time.sleep(2 * pause_time)
                shutil.copy(thread_path + str(jeffdean1[0]) + '/'+file_named_1,file_trans_location)
                shutil.copy(thread_path + value1 + '/' + file_named_2, file_trans_location)
        end = time.time()
        que.put(end - start)
        time.sleep(0.1)

'''
        print("Helloworld"+thread_path)
        for file_copy in location:
            gau_pause_time=-math.log(random.uniform(0, 1))#gaussian distribution
            result_file_copy=file_copy.split('_')
            if len(result_file_copy) >3:
                print(file_copy)
                first=result_file_copy[0]
                third=result_file_copy[1]
                fifth=result_file_copy[2]
                seventh=result_file_copy[3]
                file_location=self.path+'coded_'+seventh
                #print(first, third, fifth, seventh)
                if (seventh != first and seventh!=third and (fifth =="1")): # if equal to 1 then copy file from location equal to the value from first
                    shutil.copy(self.path+'coded_'+first+'/'+file_copy, file_location)
                elif (seventh != first and seventh!=third and (fifth =="2")): # if equal to 2 then copy file from location equal to the value from second
                    shutil.copy(self.path+ 'coded_'+third+'/'+file_copy, file_location)
'''
