import xlwt
import codecs

input_txt = 'C:\\Users\\Damon\\Desktop\\data\\新房交易原始文件\\hangzhou.txt'
output_excel = 'C:\\Users\\Damon\\Desktop\\data.xls'
sheetName = 'Sheet1'
start_row = 0
start_col = 0

wb = xlwt.Workbook(encoding = 'utf-8')
ws = wb.add_sheet(sheetName)

f = open(input_txt, encoding = 'utf-8')

row_excel = start_row

for line in f:
    line = line.strip('\n')
    line = line.split('\t')
    
   #print(line)

    col_excel = start_col
    len_line = len(line)
    for j in range(len_line):
        print (line[j])
        ws.write(row_excel,col_excel,line[j])
        col_excel += 1
        wb.save(output_excel)

    row_excel += 1

f.close
