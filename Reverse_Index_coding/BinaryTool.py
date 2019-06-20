import os
import struct
import time

class BinaryTool(object):
    def __init__(self, base_dir):
        self.base_dir = base_dir
    def encrypt(self, filename):
        # Parameter is the file name to be converted to binary
        with open(self.base_dir + filename, 'r') as f:
            content = f.read()
            # print("Original content:", content)
            # for c in content:
            #     print(c)
            bin_form = []
            for x in content:
                o = str(format(ord(str(x)), 'b'))
                if (len(o) < 7):
                    o = '0' * (7 - len(o)) + o
                bin_form.append(o)
            binary = ' '.join(bin_form)
        return binary

    def decrypt(self, bin_text):
        # Parameter is the binary string to convert back to text
        text = ''
        for ch in bin_text.split(' '):
            ascii_code = int(ch, 2)
            if (ascii_code > 0):
                text += chr(ascii_code)
        return text

    def readFile(self, file):
        read = ""
        with open(file) as f:
            read = f.read().strip();
        return read

    def xorTowFiles(self,fileA, fileB, encrypt = True):
        if encrypt:
            ec_fileA, ec_fileB = self.encrypt(fileA), self.encrypt(fileB)
        else:
            ec_fileA, ec_fileB = self.readFile(fileA), self.readFile(fileB)
        return self.Xor(ec_fileA, ec_fileB)

    def Xor(self, ecA, ecB):
        output = []
        ec_fileA, ec_fileB = ecA.split(' '), ecB.split(' ')
        l_a, l_b = len(ec_fileA), len(ec_fileB)
        if (l_a > l_b):
            output = ec_fileA[:l_a - l_b]
            ec_fileA = ec_fileA[l_a - l_b:]
        elif (l_a < l_b):
            output = ec_fileB[:l_b - l_a]
            ec_fileB = ec_fileB[l_b - l_a:]

        for c, d in zip(ec_fileA, ec_fileB):
            if not c or not d:
                continue
            c_bin, d_bin = int(c, 2), int(d, 2)
            cd_xor = c_bin ^ d_bin
            bin_xor = bin(cd_xor)
            output.append(str(bin_xor)[2:].zfill(7))
        return ' '.join(output)


if __name__ == '__main__':
    # print(bin(8))
    filename1 = '1_2_96.txt'
    filename2 = '1_2_12.txt'
    binary_tool = BinaryTool('coded_master/')
    # binary_tool.readBin('1_2_1_1.txt')

    binary = binary_tool.encrypt(filename1)
    binary2 = binary_tool.encrypt(filename2)
#    print("Encrypted binary 1:", binary)
#    print("Encrypted binary 2:", binary2)
#
#    print("Decrypted content:",binary_tool.decrypt(binary))

#    print("Xor two files: ", filename1, filename2)
    res = binary_tool.xorTowFiles(filename1, filename2)
#    print("Xor value:", res)

#    print("De-xor file 2")
    dcp = binary_tool.Xor(binary, res)

#    print("Original Content")
#    print(binary_tool.decrypt(dcp))
