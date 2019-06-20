import os
import shutil
import multiprocessing
import math
import random
import time,datetime
import multiprocessing
from BinaryTool import  BinaryTool
from collections import defaultdict
import operator
from GraphGenerator import GraphGenerator

class FileReduce(object):
    def __init__(self, users, folder_path,path, link_dict_uci, link_dict_all):
        #self.file_dir = file_dir
        self.users = users
        self.folder_path=folder_path
        self.path=path
        self.link_dict_uci = link_dict_uci
        self.link_dict_all = link_dict_all

    def reduce(self, value, draw = False):
        def get_pair(a, b):
            return str(a) + '_' + str(b) if a < b else str(b) + '_' + str(a)

        reducer_id = value[0]
        combinations = value[1]
        binary_tool = BinaryTool("")
        read_path = self.path + "coded_" + str(reducer_id) + '/'

        # print("Reducer id:", reducer_id)
        files_to_read = set()
        for theta in ['1', '2']:
            for comb in combinations:
                alpha, beta = comb[1], comb[2]
                new_comb_1 = get_pair(alpha, beta)
                new_comb_2 = get_pair(int(reducer_id), alpha)
                new_comb_3 = get_pair(int(reducer_id), beta)
                files_to_read.add(new_comb_1 + '_' + theta + '_' + str(reducer_id))
                files_to_read.add(new_comb_2 + '_' + theta + '_' + str(reducer_id))
                files_to_read.add(new_comb_3 + '_' + theta + '_' + str(reducer_id))

        # print(list(files_to_read))
        # print("\n")
        base_dir = self.path + "coded_" + str(reducer_id) + '/';
        file_to_write = base_dir + "output.txt"
        rank_write_dir = base_dir + "rank.txt"
        mem = {}

        freq_mem_all = defaultdict(list)

        for file in list(files_to_read):
            with open(read_path + file + ".txt", 'r') as f:
                content = binary_tool.decrypt(f.read())
                content = content.split('\n')
                written_data = ""
                source = -1
                target = -1
                for line in content:
                    line = line[:line.find('\x01')]
                    pair = line[1 : -1].split(',')
                    if len(pair) == 2:
                        if pair[0]:
                            target = int(pair[0])
                        if pair[1]:
                            source = int(pair[1])

                    if source >= 0 and target >= 0:
                        freq_mem_all[source].append(target)
                    new_form = (source, target)
                    # print(new_form)
                    if new_form in mem:
                        mem[new_form] += 1
                    else:
                        mem[new_form] = 1

        pair_list = []
        for key in mem.keys():
            if(key[0] < 0 and key[1] < 0):
                continue
            pair_list.append((key[0], key[1], mem[key]))

        #sort the list
        pair_list = sorted(sorted(sorted(pair_list, key = lambda x: x[2]), key = lambda x: x[1]), key = lambda x: x[0])
        # print("Write to", file_to_write)
        for pair in pair_list:
            op = 'a'
            if not os.path.isfile(file_to_write):
                op = 'w'
            written_data = '(' + str(pair[0]) + ',' + str(pair[1]) + ')' + str(pair[2]) + '\n'
            with open(file_to_write, op) as wf:
                wf.write(written_data)

        #Pick the most referred links
        rank_data = "links, referred times\n"
        top_ten = sorted(freq_mem_all.items(), key=lambda x : len(x[1]), reverse = True)[:10]


        print(reducer_id, top_ten[0][0])

        t_tens = ""
        for item in top_ten:
            t_tens += str(item[0]) + ","
            rank_data += (self.link_dict_all[item[0]] + ", " + str(len(item[1])) + "\n")
        print(t_tens)

        with open(rank_write_dir, "w") as rank_f:
            rank_f.write(rank_data)

        if draw:
            # print("Start drawing graph")
            gg = GraphGenerator()
            gg.draw_graph(freq_mem_all, reducer_id, self.link_dict_uci)

                # with open(file_to_write, op) as write_file:
                #     write_file.write(orig_content)

        return top_ten

# if __name__ == "__main__":
#     fr = FileReduce(4, )