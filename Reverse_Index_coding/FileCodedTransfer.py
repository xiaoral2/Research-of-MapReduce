import os
import shutil
import multiprocessing
import math
import random
import time,datetime
import multiprocessing
from BinaryTool import BinaryTool
import threading
from FileProcessor import FileProcessor
from multiprocessing import Queue
import math

times = [0.1, -math.log(random.uniform(0, 1)) / 10, random.uniform(0, 0.2), 0.05 + random.uniform(0, 0.1),
            0.05 + (-math.log(random.uniform(0, 1)) / 20)]


class FileCodedTransfer(object):
    def __init__(self, users, folder_path, path, num_of_links):
        #self.file_dir = file_dir
        self.users = users
        self.folder_path=folder_path
        self.path=path
        self.num_of_links = num_of_links

        self.encoding_time = 0
        self.decoding_time = 0

        self.avg_encoding_time = 0
        self.avg_decoding_time = 0

        self.pause_factor = 0.02
        self.file_trans_num = 0
    ################ Part 1  #################
    ############# Mapping Code  ##############
    def Mapping(self):
        '''
        Map function assign files to each user
        '''
        new_path = self.folder_path
        files = os.listdir(new_path)  # get file name under the folder
        results = set()  # find folder name
        number_result = set()
        for files_names in files:
            files_name = files_names.split('.')
            file_name = files_name[0].split('_')
            if len(files_names) > 4:
                first = "coded_" + file_name[0]
                third = "coded_" + file_name[1]
                results.add(first)
                results.add(third)
                number_first = file_name[0]
                number_third = file_name[1]
                number_result.add(number_first)
                number_result.add(number_third)
        results = list(results)
        number_result = list(number_result)
        os.chdir(self.path)  # change the director for the folder path

        for folder_name in results:  # delete folders that exists from previous run
            if os.path.exists(self.path + folder_name):
                shutil.rmtree(self.path + folder_name)
        for folder_name in results:  # create new folders for the number desired
            if os.path.isdir(self.path):
                os.mkdir(os.path.join(self.path, folder_name))

        for files_names in files:
            files_name = files_names.split('.')
            file_name = files_name[0].split('_')
            if len(files_names) > 4:
                first = "coded_" + file_name[0] + "/Map/"
                third = "coded_" + file_name[1] + "/Map/"
                if not os.path.isdir(self.path + first):
                    os.mkdir(self.path + first)
                shutil.copy(new_path + files_names, self.path + first)

                if not os.path.isdir(self.path + third):
                    os.mkdir(self.path + third)
                shutil.copy(new_path + files_names, self.path + third)

        return results, number_result


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
        fileprocess.create_pair_files('pair_dir/', worker_id)
        fileprocess.collect_file_data()
        return



################ Part 3  #################
############ Call Function  ##############  coded
    def Call_coded_function(self, everything):
        enc_start = time.time()
        threads_encode = []
        enout_que = Queue()

        deout_que = Queue()

        #Create a single thread for each worker
        for i in everything:
            # print("Server current encoding coded" + str(i[0]))
            jeffdean = i[0]
            t = threading.Thread(target=self.coded_Encoding, args=(i, enout_que,))
            threads_encode.append(t)
            t.start()

        #Wait until all workers are done with their job
        for x in threads_encode:
            x.join()

        enc_finish = time.time()

        self.encoding_time = enc_finish - enc_start

        en_out = 0
        de_out = 0
        file_trans_num = 0


        dec_start = time.time()

        for i in everything:
            # print("Server current encoding coded" + str(i[0]))
            jeffdean = i[0]
            t = threading.Thread(target=self.Decoding, args=(i, deout_que,))
            threads_encode.append(t)
            t.start()

        #Wait until all workers are done with their job
        for x in threads_encode:
            x.join()

        dec_end = time.time()

        while not enout_que.empty():
            dout = enout_que.get()
            en_out += dout[1]
            file_trans_num += dout[3]

        while not deout_que.empty():
            out = deout_que.get()
            de_out += out[1]

        enout_que.close()
        deout_que.close()

        self.decoding_time = dec_end - dec_start
        self.avg_encoding_time = en_out / self.users
        self.avg_decoding_time = de_out / self.users
        self.file_trans_num = file_trans_num

################ Part 5  #################
############# Coded Code  ################

############### Part 51 ##################
##### Shuffling Code for generate ########
    def shufgen(self):
        L1=[]
        user=int(self.users)
        for i in range(user):
            i=i+1
            L1.append(i)
        #L1=number_result
        L2=L1[:]
        L3=[] #除去两个随机
        L4=[] #
        L5=[]
        L6=[]
        D1={}
        temp=[]
        #print(L1)
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


############### Part 52  ##################
############ Shuffling Code ##############
#print (results)

    def coded_Encoding(self,i, out_que):
        t1 = time.time()
        jeffdean1=i
        worker_id = jeffdean1[0]
        jeffdeanlist=jeffdean1[1]
        file_trans_num = 0

        #for new_folder_name in results:#create new file in different folder
        path_folder=self.path + "coded_" + str(jeffdean1[0]) + '/'
        os.chdir(path_folder)
        bi_tool = BinaryTool(path_folder)

        pair_files = os.listdir(path_folder + "pair_dir/")

        for pair_file in pair_files:
            bi_content = bi_tool.encrypt("pair_dir/" + pair_file)
            try:
                with open(path_folder + pair_file, 'w') as wc:
                    wc.write(bi_content)
            except IOError as io:
                print("IO error:", io)

        for index, i in enumerate(jeffdeanlist):
            beta = i[1]#value 1
            theta = i[2]#value 2
            beta_str = str(beta)
            theta_str = str(theta)
            sub_name_list = []
            worker_id_str = str(worker_id)#jeffdean1[0]
            try:
                if int(beta) > int(worker_id):
                    file1=worker_id_str+"_"+beta_str+"_1_"+theta_str
                    with open(path_folder + worker_id_str+"_"+beta_str+"_1_"+theta_str+".txt",'r') as f1:
                        sub_name_list.append(file1)
                        bin_a = f1.read()
                else:
                    file1=beta_str+"_"+worker_id_str+"_2_"+ theta_str
                    with open(path_folder + beta_str+"_"+worker_id_str+"_2_"+theta_str+".txt",'r') as f1:
                        sub_name_list.append(file1)
                        bin_a = f1.read()

                if int(theta) > int(worker_id):
                    file2=worker_id_str+"_"+ theta_str +"_1_"+beta_str
                    with open(path_folder + worker_id_str + "_" + theta_str + "_1_"+beta_str + ".txt",'r') as f2:
                        sub_name_list.append(file2)
                        bin_b = f2.read()
                else:
                    file2=theta_str + "_" + worker_id_str + "_2_"+ beta_str
                    with open(path_folder + theta_str + "_"+worker_id_str+"_2_"+beta_str+".txt",'r') as f2:
                        sub_name_list.append(file2)
                        bin_b = f2.read()
            except IOError as io:
                print("IO error:", io)
            # print("Xor,", index, i)
            total= bi_tool.Xor(bin_a, bin_b)

            #The xor output file of file A and file B is named as fileA__fileB
            if int(beta) > int(theta):
                transfer2=str(sub_name_list[1]+'__'+sub_name_list[0])
            else:
                transfer2=str(sub_name_list[0]+'__'+sub_name_list[1])


            xor_dir = path_folder + '/'
            if not os.path.isdir(xor_dir):
                os.mkdir(xor_dir)

            if transfer2 not in sub_name_list:
                try:
                    with open(xor_dir + transfer2+".txt",'w') as file_transfer:
                        file_transfer.write(total)
                except IOError as io:
                    print("IO error:", io)
        src_dir = path_folder + '/'

        #worker_num_str is the string format of current worker number
        worker_num_str = str(jeffdean1[0])
        #
        # #After the encoding is done, all the xor files should be inside of the "/xored_files/" of each workers, the the each worker starts sending files
        for src_file in os.listdir(src_dir):

            pause_time = -math.log(random.uniform(0, 1)) / 10  # exponetial distribution
            pause_uni_time = random.uniform(0, 0.2)  # uniform distribution
            pratical = 0.05 + random.uniform(0, 0.1)
            pratical_exp = 0.05 + (-math.log(random.uniform(0, 1)) / 20)

            file_name = src_file.split('.')
            file_name_list=file_name[0].split('_')
            if len(src_file) > 17:
                #Compare if the alpha or beta of the file A is equal to the current worker number
                if file_name_list[0]== worker_num_str or file_name_list[1] == worker_num_str:
                    # Compare if the alpha or beta of the file B is equal to the current worker number
                    if file_name_list[5] == worker_num_str or file_name_list[6] == worker_num_str:

                        #Creating destination for the file to be sent based on the value of last digit of file A and file B
                        dest_1 = self.path + "coded_"+file_name_list[3] + '/'
                        dest_2 = self.path + "coded_"+file_name_list[8] + '/'
                        # print(dest_1, dest_2, src_file)
                        # print(str(jeffdean1[0]), "dest1: " + dest_1, "dest2:" + dest_2)

                        #Here we use file copy to implement the file sending
                        shutil.copy(src_dir + src_file, dest_1)
                        shutil.copy(src_dir + src_file, dest_2)
                        time.sleep(pratical_exp)
                        file_trans_num += 1

        # print(worker_id, "has", file_trans_num, "files transfered")
        t2 = time.time()

        enc_time = t2 - t1

        time.sleep(0.2)

        dec_start = time.time()
        # self.Decoding(jeffdean1)
        dec_end = time.time()

        out_que.put((jeffdean1[0], enc_time, dec_end - dec_start, file_trans_num))


        time.sleep(0.1)

    def pause_time(self, file):
        return self.pause_factor * os.path.getsize(file)


    def Decoding(self, val1, out_que = None):

        start = time.time()
        jeffdean1=val1
        # foldervalue=str(jeffdean1[0])
        jeffdeanlist=jeffdean1[1]
        path_folder=self.path + "coded_" + str(jeffdean1[0])
        os.chdir(path_folder)
        jeffdean1[0] = str(jeffdean1[0])

        # print("current Decoding user" + jeffdean1[0])
        binary_tool = BinaryTool("")

        def write_file(filename, content):
            with open(filename + ".txt", 'w') as f:
                f.write(content)

        for index, i in enumerate(jeffdeanlist):
            value1=str(i[1])
            value2=str(i[2])

            # try:
            if int(value1) > int(value2):
                if int(jeffdean1[0])>int(value1) and int(jeffdean1[0])>int(value2):
                #if coming from value1. The value exist in the file is  V1_num[1]_1_v2
                    file1=value1+"_"+jeffdean1[0]+"_1_"+value2
                    #if coming from value2 The value exist in the file is  V1_num[1]_1_v2
                    file2=value2+"_"+jeffdean1[0]+"_1_"+value1
                    file3=value2+"_"+value1+"_2_"+jeffdean1[0]+"__"+file1
                    file4=value2+"_"+value1+"_1_"+jeffdean1[0]+"__"+file2

                    file1 = path_folder+"/"+ file1 + ".txt"
                    file2 = path_folder+"/"+ file2 + ".txt"
                    file3 = path_folder+"/"+ file3 + ".txt"
                    file4 = path_folder+"/"+ file4 + ".txt"

                    substrate=binary_tool.xorTowFiles(file3, file1, encrypt= False)
                    substrates=binary_tool.xorTowFiles(file4, file2, encrypt= False)

                    decodefile1=value2+"_"+value1+"_2_"+jeffdean1[0]
                    decodefile2=value2+"_"+value1+"_1_"+jeffdean1[0]
                    write_file(path_folder + "/" + decodefile1, substrate)
                    write_file(path_folder + "/" + decodefile2, substrates)

                elif int(jeffdean1[0])>int(value2) and int(jeffdean1[0])<int(value1):
                    #if coming from value1. The value exist in the file is  V1_num[1]_1_v2
                    file1=jeffdean1[0]+"_"+value1+"_2_"+value2
                    #if coming from value2 The value exist in the file is  V1_num[1]_1_v2
                    file2=value2+"_"+jeffdean1[0]+"_1_"+value1
                    file3=value2+"_"+value1+"_2_"+jeffdean1[0]+"__"+file1
                    file4=file2+"__"+value2+"_"+value1+"_1_"+jeffdean1[0]

                    file1 = path_folder+"/"+ file1 + ".txt"
                    file2 = path_folder+"/"+ file2 + ".txt"
                    file3 = path_folder+"/"+ file3 + ".txt"
                    file4 = path_folder+"/"+ file4 + ".txt"
                    substrate = binary_tool.xorTowFiles(file3, file1, encrypt= False)
                    substrates = binary_tool.xorTowFiles(file4, file2, encrypt= False)
                    decodefile1=value2+"_"+value1+"_2_"+jeffdean1[0]
                    decodefile2=value2+"_"+value1+"_1_"+jeffdean1[0]

                    # print("fuck: ", jeffdean1[0], decodefile1, decodefile2)
                    write_file(path_folder+"/" + decodefile1, substrate)
                    write_file(path_folder+"/" + decodefile2, substrates)
                elif int(jeffdean1[0])<int(value1) and int(jeffdean1[0])<int(value2):
                #if coming from value1. The value exist in the file is  V1_num[1]_1_v2
                    file1=jeffdean1[0]+"_"+value1+"_2_"+value2
                    #if coming from value2 The value exist in the file is  V1_num[1]_1_v2
                    file2=jeffdean1[0]+"_"+value2+"_2_"+value1
                    file3=file1+"__"+value2+"_"+value1+"_2_"+jeffdean1[0]
                    file4=file2+"__"+value2+"_"+value1+"_1_"+jeffdean1[0]

                    # substrate=str(c-a)
                    # substrates=str(d-b)
                    file1 = path_folder+"/"+ file1 + ".txt"
                    file2 = path_folder+"/"+ file2 + ".txt"
                    file3 = path_folder+"/"+ file3 + ".txt"
                    file4 = path_folder+"/"+ file4 + ".txt"
                    substrate = binary_tool.xorTowFiles(file3, file1, encrypt= False)
                    substrates = binary_tool.xorTowFiles(file4, file2, encrypt= False)
                    decodefile1=value2+"_"+value1+"_2_"+jeffdean1[0]
                    decodefile2=value2+"_"+value1+"_1_"+jeffdean1[0]
                    write_file(path_folder + "/" + decodefile1, substrate)
                    write_file(path_folder + "/" + decodefile2, substrates)
            else:
                if int(jeffdean1[0])>int(value1) and int(jeffdean1[0])>int(value2):
                    #if coming from value1. The value exist in the file is  V1_num[1]_1_v2
                    file1=value1+"_"+jeffdean1[0]+"_1_"+value2
                    #if coming from value2 The value exist in the file is  V1_num[1]_1_v2
                    file2=value2+"_"+jeffdean1[0]+"_1_"+value1
                    file3=value1+"_"+value2+"_1_"+jeffdean1[0]+"__"+file1
                    file4=value1+"_"+value2+"_2_"+jeffdean1[0]+"__"+file2
                    # substrate=str(c-a)
                    # substrates=str(d-b)
                    file1 = path_folder+"/"+ file1 + ".txt"
                    file2 = path_folder+"/"+ file2 + ".txt"
                    file3 = path_folder+"/"+ file3 + ".txt"
                    file4 = path_folder+"/"+ file4 + ".txt"
                    substrate = binary_tool.xorTowFiles(file3, file1, encrypt= False)
                    substrates = binary_tool.xorTowFiles(file4, file2, encrypt= False)
                    decodefile1=value1+"_"+value2+"_1_"+jeffdean1[0]
                    decodefile2=value1+"_"+value2+"_2_"+jeffdean1[0]
                    write_file(path_folder + "/" + decodefile1, substrate)
                    write_file(path_folder + "/" + decodefile2, substrates)
                elif int(jeffdean1[0])>int(value1) and int(jeffdean1[0])<int(value2):
                    #if coming from value1. The value exist in the file is  V1_num[1]_1_v2
                    file1=value1+"_"+jeffdean1[0]+"_1_"+value2
                    #if coming from value2 The value exist in the file is  V1_num[1]_1_v2
                    file2=jeffdean1[0]+"_"+value2+"_2_"+value1
                    file3=file1+"__"+value1+"_"+value2+"_1_"+jeffdean1[0]
                    file4=value1+"_"+value2+"_2_"+jeffdean1[0]+"__"+file2

                    file1 = path_folder+"/"+ file1 + ".txt"
                    file2 = path_folder+"/"+ file2 + ".txt"
                    file3 = path_folder+"/"+ file3 + ".txt"
                    file4 = path_folder+"/"+ file4 + ".txt"
                    substrate = binary_tool.xorTowFiles(file3, file1, encrypt= False)
                    substrates = binary_tool.xorTowFiles(file4, file2, encrypt= False)
                    decodefile1=value1+"_"+value2+"_1_"+jeffdean1[0]
                    decodefile2=value1+"_"+value2+"_2_"+jeffdean1[0]
                    write_file(path_folder + "/" + decodefile1, substrate)
                    write_file(path_folder + "/" + decodefile2, substrates)

                elif int(jeffdean1[0])<int(value1) and int(jeffdean1[0])<int(value2):
                    #if coming from value1. The value exist in the file is  V1_num[1]_1_v2
                    file1=jeffdean1[0]+"_"+value1+"_2_"+value2
                    #if coming from value2 The value exist in the file is  V1_num[1]_1_v2
                    file2=jeffdean1[0]+"_"+value2+"_2_"+value1
                    file3=file1+"__"+value1+"_"+value2+"_1_"+jeffdean1[0]
                    file4=file2+"__"+value1+"_"+value2+"_2_"+jeffdean1[0]


                    file1 = path_folder+"/"+ file1 + ".txt"
                    file2 = path_folder+"/"+ file2 + ".txt"
                    file3 = path_folder+"/"+ file3 + ".txt"
                    file4 = path_folder+"/"+ file4 + ".txt"
                    # substrate=str(c-a)
                    # substrates=str(d-b)

                    substrate = binary_tool.xorTowFiles(file3, file1, encrypt= False)
                    substrates = binary_tool.xorTowFiles(file4, file2, encrypt= False)

                    decodefile1=value1+"_"+value2+"_1_"+jeffdean1[0]
                    decodefile2=value1+"_"+value2+"_2_"+jeffdean1[0]
                    write_file(path_folder + "/" + decodefile1, substrate)
                    write_file(path_folder + "/" + decodefile2, substrates)
            # except IOError as e:
            #     print("Node error:", "from coded " + jeffdean1[0], e)
        end = time.time()

        out_que.put((jeffdean1, end - start))
        time.sleep(0.1)