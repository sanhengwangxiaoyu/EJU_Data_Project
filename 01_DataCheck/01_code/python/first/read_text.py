import codecs

input_txt = 'C:\\Users\\Damon\\Desktop\\data\\新房交易原始文件\\baoji.txt'
start_row = 0
start_col = 0

f = open(input_txt, encoding = 'utf-8')

row_excel = start_row
for line in f:
    line = line.strip('\n')
    line = line.split('\t')
    
#    print(line)


f.close
