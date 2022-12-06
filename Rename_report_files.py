from module import file_operations, date_operations
from pprint import pprint
import os, shutil
import glob
import openpyxl



dir_path = os.getcwd() 
file = dir_path + '/' + 'reports' + '/' + '*' + 'accounts_report_' + '*'
files = glob.glob(file)
pprint(files)

for file in files[0:10]:
    
    
    ss=openpyxl.load_workbook(filename = file)
    
    
    pprint(ss)
    
    # #printing the sheet names
    # ss_sheet = ss['Sheet']
    # ss_sheet.title = 'Fruit'
    # ss.save("file.xlsx")