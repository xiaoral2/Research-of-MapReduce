import shutil
import os
from Crawler import Crawler
from FileProcessor import FileProcessor
from FileTransfer import FileTransfer
from FileCodedTransfer import FileCodedTransfer
from FileReduce import FileReduce
import time
import sys

dir = os.path.dirname(os.path.realpath(__file__))
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def do_naive_job(loop_num = 1):
    forcounter = 0
    num_of_links = 1155
    enc_time, dec_time = 0, 0
    per_enc_time, per_dec_time = 0, 0

    while forcounter < loop_num:
        # print("round", forcounter)
        file_transfer = FileTransfer(user, folder_path, path, num_of_links)
        everything = file_transfer.shufgen()
        file_transfer.Call_naive_function(everything)
        forcounter += 1
        enc_time += file_transfer.encoding_time
        per_enc_time += file_transfer.avg_encoding_time

    print("Average encoding time:", enc_time / loop_num, "Average per encoding time:",
          per_enc_time / loop_num)
    return


def do_coded_job(loop_num = 1):
    forcounter = 0
    num_of_links = 1155
    #        print("you are going to have " + users + " users for this simulation")

    enc_time, dec_time = 0, 0
    per_enc_time, per_dec_time = 0, 0

    while forcounter < loop_num:
        # print("round", forcounter)

        try:
            file_coded_transfer = FileCodedTransfer(user, folder_path, path, num_of_links)
            everything = file_coded_transfer.shufgen()
            file_coded_transfer.Call_coded_function(everything)
        except IOError as io:
            print("IO error:", io)
        enc_time += file_coded_transfer.encoding_time
        dec_time += file_coded_transfer.decoding_time
        per_enc_time += file_coded_transfer.avg_encoding_time
        per_dec_time += file_coded_transfer.avg_decoding_time

        forcounter += 1

    avg_enc_time = enc_time / loop_num
    avg_dec_time = dec_time / loop_num
    avg_per_enc_time = per_enc_time / loop_num
    avg_per_dec_time = per_dec_time / loop_num

    print("Average encoding time:", avg_enc_time, "Average per encoding time:", avg_per_enc_time)
    print("Average decoding time:", avg_dec_time + avg_enc_time, "Average per decoding time:", avg_per_dec_time + avg_per_enc_time)
    print("File transferred:", file_coded_transfer.file_trans_num)
    return


if __name__ == '__main__':
    path = ROOT_DIR+'/'
    # path="/Users/xiaoran/Dropbox/Reverse_Index/"
    # folder_path="/Users/xiaoran/Dropbox/Reverse_Index/coded_master"

    folder_path = path + 'coded_master/'
    def moveFileto(sourceDir,  targetDir):
        shutil.copy(sourceDir,  targetDir)
    print("How many user you are trying to have?")
    users=input()
    user = int(users)
    print("you are going to have " +users + " users for this simulation" )

    print("Choose 1 or 2: 1. Naive Scheme; 2. Coded Scheme")
    mode = int(input())

    # print("How many times:")
    # loopnum = int(input())


    # user = int(sys.argv[1])
    # mode = int(sys.argv[2])
    # loopnum = int(sys.argv[3])

    reinit = True
    recrawl = True
    reprocess = True
    remapping = True
    nodeJob = True

    rename_files = True

    if reinit:
        if os.path.exists(folder_path):  # Delete the folder if the folder exist
            shutil.rmtree(folder_path)
        # def filecreating():
        files = os.listdir(path)  # get file name under the folder
        # define the access rights
        access_rights = 0o755
        try:
            os.mkdir(folder_path, access_rights)
        except OSError:
            print("Creation of the directory %s failed" % folder_path)
        # else:
        #     print("Successfully created the directory %s" % folder_path)
        for folder_nameint in range(user):  # delete folders that exists from previous run
            folder_name = str(folder_nameint + 1)
            if os.path.exists(os.path.join(path, "coded_" + folder_name)):
                shutil.rmtree(os.path.join(path, "coded_" + folder_name))

        for folder_nameint in range(user):  # create new folders for the number desired
            folder_name = str(folder_nameint + 1)
            if os.path.isdir(path):
                os.mkdir(os.path.join(path, "coded_" + folder_name))

    linkdict = {}
    reverse_linkdict = {}

    uci_linkdict = {}

    with open(dir + "/res.txt", "r") as f:
        links = f.readlines()
        num_of_links = len(links)
        linkdict = {value + 1: links[value].strip() for value in range(num_of_links)}
        uci_linkdict = linkdict
        reverse_linkdict = {links[value].strip(): value + 1 for value in range(num_of_links)}

    if recrawl:
        # print(len(reverse_linkdict.keys()))
        os.chdir(folder_path)  # change the director for the folder path
        print("Start crawling")
        # Created an instance of crawler and pass user number and links file into
        crawler = Crawler(user, "res.txt", reverse_linkdict, linkdict)
        # #Call crawl_and_createfile method to get all target links and create file for each source link
        crawler.crawl_and_createfile(False, False);

    if reprocess:
        if not reinit:
            with open(dir + "/res.txt", "r") as f:
                num_of_links = len(f.readlines())

        fileprocess = FileProcessor(folder_path, user, num_of_links, path)
        fileprocess.file_filling()
        fileprocess.index_value()
        fileprocess.rename()


    if remapping:
        if mode == 1:
            file_transfer = FileTransfer(user, folder_path, path, num_of_links)
            file_coded_transfer = FileCodedTransfer(user, folder_path, path, num_of_links)
            result, number_result = file_transfer.Mapping()
        else:
            # file_transfer = (user, folder_path, path, num_of_links)
            file_coded_transfer = FileCodedTransfer(user, folder_path, path, num_of_links)
            result, number_result = file_coded_transfer.Mapping()

    if rename_files:
        if mode == 1:
            file_transfer = FileTransfer(user, folder_path, path, num_of_links)
            everything = file_transfer.shufgen()
            file_transfer.Call_File_Rename(everything)
        else:
            file_transfer = FileCodedTransfer(user, folder_path, path, num_of_links)
            everything = file_transfer.shufgen()
            file_transfer.Call_File_Rename(everything)

    # if mode == 1:
    #     do_naive_job(loopnum)
    # else:
    #     do_coded_job(loopnum)

    # fp = FileProcessor(folder_path ,4, num_of_links, path)
    #
    # fp.create_pair_files("pair_dir" ,"master")
    # fp.write_bin_files()

    # fp.collect_file_data(force_dir=True)

    if nodeJob:
        everything=file_coded_transfer.shufgen()
        print("everything", everything)
        file_coded_transfer.Call_File_Rename(everything)
        # file_transfer.Call_naive_function(everything)
        file_coded_transfer.Call_coded_function(everything)
        fr = FileReduce(user, folder_path, path, uci_linkdict, linkdict)
        top_lists = []
        for val in everything:
            top_lists += fr.reduce(val)

        top_write_dir = folder_path + "total_rank.txt"
        rank_data = "link, number of referred times\n"

        top_lists.sort(key = lambda x: len(x[1]), reverse = True)

        for i in range(10):
            rank_data += (linkdict[top_lists[i][0]] + ', ' + str(len(top_lists[i][1])) + '\n')

        with open(top_write_dir, "w") as rank_f:
            rank_f.write(rank_data)

