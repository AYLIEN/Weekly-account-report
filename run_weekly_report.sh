#!/bin/sh
cd "$(dirname "$0")";
CWD="$(pwd)"
echo $CWD
pip install os
pip install json
pip install time
pip install requests
pip install glob
pip install xmltodict
pip install copy
pip install pprint
pip install tqdm
pip install datetime
pip install sys
pip install os, shutil
pip install pandas
pip install numpy
pip install decimal
pip install xlsxwriter
pip install math
/home/angelo/miniconda3/bin/python3 /home/angelo/Weekly-account-report/main.py >> /home/angelo/Weekly-account-report/log.txt 
