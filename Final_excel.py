import pandas as pd
import numpy as np
import ftplib
from re import search
import warnings
import subprocess
import getpass
from os import remove
from openpyxl import Workbook
from openpyxl.styles import Font, Color, colors, PatternFill
from openpyxl.styles.borders import Border, Side
from openpyxl.styles import Alignment
warnings.filterwarnings("ignore")


UID = input("Enter your User ID: ").upper().strip()
# PWD = input("Password: ").upper().strip()
PWD = getpass.getpass()
prev = input("Enter your Previous month: ").upper().strip()
curr = input("Enter your Current month: ").upper().strip()

#-------------------------------------------------------
#filenames for each month for class 0&1
Custom1    = 'DUL.DBM.ULNTHJ.'+curr+'.CMP.AU01'
Net1       = 'DMK.ULNTHJ.F'+curr+'.NET.COMP01'
Drops1     = 'DUL.DBM.ULNTHJ.A'+prev+'.UNMAT01.AUD'
Adds1      = 'DUL.DBM.ULNTHJ.'+curr+'.UNMAT01.AUD'
#---------------------------------------------------------
#-------------------------------------------------------
#filenames for each month for class 2
Custom2    = 'DUL.DBM.ULNTHJ.'+curr+'.CMP.AU2'
Net2       = 'DMK.ULNTHJ.F'+curr+'.NET.COMP2'
Drops2     = 'DUL.DBM.ULNTHJ.A'+prev+'.UNMAT2.AUD'
Adds2      = 'DUL.DBM.ULNTHJ.'+curr+'.UNMAT2.AUD'
#---------------------------------------------------------



def test_hostname_alive(host):
    "ping a host"
    # $TODO : test under "windows"
    ping = subprocess.Popen("ping -q -c2 -W 2 " + host, shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    exitcode = ping.wait()
    response = ping.stdout.read()
    errors   = ping.stderr.read()
    if exitcode in (0, 1):
        life = search(r"(\d) received", response)
        if life and life.group(1) == '0':  # 1 packets transmitted, 0 received,
            raise ZftpError("Unknown hostname %s (if ICMP ping is blocked"
                            ", try this: Zftp(..,noping=1) )"%host)
    else:
        if 'unknown host' in errors:
            raise ZftpError("Unknown hostname %s (%s)"%(host, errors))
        else:
            raise ZftpError("Ping hostname %s error (%s)"%(host, errors))


def sanitize_mvsname(name):
    " sanitize mvs dataset name "
    if name:
        return "'" + name.strip().replace("'","").replace('"','') + "'"
    else:
        return name


class ZftpError( Exception ):
    """ZosFtp error."""
    def __init__(self, value):
        super(ZftpError, self).__init__(value)
        self.value = value
    def __str__(self):
        return repr(self.value)


class Zftp(ftplib.FTP):
    def __init__(self, host='', user='', passwd='', acct='', timeout=600.0,
                 sbdataconn='', **kwargs):

        self.__ping = kwargs.get('ping', False)
        if self.__ping:                 # caution: a host can be configured
            test_hostname_alive(host)   # to block icmp pings

        self.__kwargs = kwargs
        try:
            ftplib.FTP.__init__(self, host, user, passwd, acct, timeout)
        except TypeError: # timeout not supported ?
            ftplib.FTP.__init__(self, host, user, passwd, acct)
            self.timeout = None
        syst = self.sendcmd('SYST')
        if not 'z/OS' in syst:
            raise ZftpError("host %s is not a MVS or z/OS platform: %s"
                            %(host, syst))
        if sbdataconn:
            self.sendcmd('SITE sbdataconn=' + sbdataconn)
        self.sbdataconn = sbdataconn
        self.stats  = self.sendcmd('STAT')
        self.stats  = self.sendcmd('STAT')
        pos_ftyp    = self.stats.find('211-FileType') + 12
        pos_jesint  = self.stats.find('211-JESINTERFACELEVEL') + 25
        self.filetype = self.stats[pos_ftyp :pos_ftyp + 3]
        self.__jesinterfacelevel = self.stats[pos_jesint :pos_jesint + 1]
        self.__offsets = None
        self.__processed_members = 0
        self.__jobid = None

    def login(self, user='', passwd='', acct=''):
        self.user = user
        self.passwd = passwd
        self.acct = acct
        ftplib.FTP.login(self, user, passwd, acct)

    def _setfiletype(self, filetype='SEQ'):
        """Switch z/OS FTP filetype parameter : SEQ, JES, DB2
        """
        if not self.filetype == filetype:
            self.sendcmd('SITE filetype=' + filetype)
        self.filetype = filetype

    def getresp(self):
        """
        ftplib.getresp :
        parse JOBNAME in 250/125 z/OS FTP response
        """
        resp = self.getmultiline()
        if self.debugging:
            print('*resp*', self.sanitize(resp))
        self.lastresp = resp[:3]
        c = resp[:1]
        if c in ('1', '2', '3'):
            if resp[:3] in('250','125'):                   #|Zftp spec
                sx0 = search(r"\s+(JOB\d{5})\s+", resp) #|
                if sx0:                                    #|
                    self.__jobid = sx0.group(1)            #|
            return resp
        if c == '4':
            raise ftplib.error_temp(resp)
        if c == '5':
            raise ftplib.error_perm(resp)
        raise ftplib.error_proto(resp)

    def download_text(self, mvsname, localpath):
        " download one file by FTP in text mode "
        self._setfiletype('SEQ')
        localfile = open(localpath, 'w',encoding='utf-8')
        mvsname = sanitize_mvsname(mvsname)
        def callback(line):
            localfile.write(line + '\n')
        self.retrlines('RETR ' + mvsname, callback)
        localfile.close()

def compare_aud(file_cmp):
    with open(file_cmp, 'r', encoding='utf-8') as file:
        f = file.readlines()
    variable = []
    previous = []
    current= []
    percentage = []

    for line in f:
        if "PERCENTAGE " not in line  and "PREVIOUS RUN        CURRENT RUN" not in line and "ERCENTAG" not in line and line[30:].strip() != '':
            variable.append(line[30:52].strip())
            previous.append(line[61:71].strip())
            current.append(line[80:91].strip())
            percentage.append(line[95:103].strip())
        
        
    dict={'VARIABLE': variable,'PREVIOUS':previous,'CURRENT':current,'PERCENTAGE': percentage}
    df = pd.DataFrame(dict)
    #print(df.head(10))
    #print(df.dtypes)
    def difference(row):
        
        if len(row['PREVIOUS'].strip()) != 0 and len(row['CURRENT'].strip()) == 0:
            return ("{:,}".format(0-int(row['PREVIOUS'].strip().replace(',',''))))
        

        elif len(row['PREVIOUS'].strip()) == 0 and len(row['CURRENT'].strip()) != 0:
            return ("{:,}".format(int(row['CURRENT'].strip().replace(',',''))))

        elif len(row['PREVIOUS'].strip()) == 0 and len(row['CURRENT'].strip()) == 0:
            return (' ')
               
        else:
            return ("{:,}".format((int(row['CURRENT'].strip().replace(',',''))) - (int(row['PREVIOUS'].strip().replace(',','')))))

    df['DIFFERENCE'] = df.apply(difference, axis=1)

    return df

def Audit(file_aud):
    with open(file_aud, 'r', encoding='utf-8') as file:
        f = file.readlines()
    variable = []
    previous = []
    current= []
    percentage = []

    for line in f:
        if "PGM=AHS003C " not in line  and "PRESENT         ABSENT" not in line and "ULINE DATA" not in line and line[30:].strip() != '':
            variable.append(line[2:24].strip())
            current.append(line[48:58].strip())
            
        
        
    dict={'VARIABLE': variable,'CURRENT':current}
    df = pd.DataFrame(dict)
    return df
    
            
           
Myzftp = Zftp("159.137.156.61", UID, PWD, timeout=2000.0, sbdataconn='(IBM-1047,ISO8859-1)')

#************************************************ CLASS 0&1 **************************************
custom_out1 = Custom1 +'.txt'
net_out1 = Net1 +'.txt'
drop_out1 = Drops1 +'.txt'
add_out1 = Adds1 +'.txt'

Myzftp.download_text(Custom1, custom_out1)
Myzftp.download_text(Net1, net_out1)
Myzftp.download_text(Drops1, drop_out1)
Myzftp.download_text(Adds1, add_out1)

df1 = compare_aud(custom_out1)
df2 = compare_aud(net_out1)
df3 = Audit(drop_out1)
df4 = Audit(add_out1)

output = pd.ExcelWriter('Custom_NET_adds_deletes_0&1_'+curr+'.xlsx', engine='xlsxwriter')

df1.to_excel(output,sheet_name='custom audit 0&1',index=False)

workbook  = output.book
worksheet = output.sheets['custom audit 0&1']
header_format = workbook.add_format({'bold':True,'fg_color':'#FFFF00','border': 1})
for i, col in enumerate(df1):                                   # loop through all columns
    worksheet.write(0, i, col, header_format)
    series = df1[col]
    max_len = max((
    series.astype(str).map(len).max(),                 # len of largest item   
    len(str(series.name))                                 # len of column name/header
    )) + 10                                                # adding a little extra space
    worksheet.set_column(i, i, max_len)                           # set column width

df2.to_excel(output,sheet_name='Net Audit',index=False)

worksheet = output.sheets['Net Audit']
header_format = workbook.add_format({'bold':True,'fg_color':'#FFFF00','border': 1})
for i, col in enumerate(df2):                                   # loop through all columns
    worksheet.write(0, i, col, header_format)
    series = df2[col]
    max_len = max((
    series.astype(str).map(len).max(),                 # len of largest item   
    len(str(series.name))                                 # len of column name/header
    )) + 10                                                # adding a little extra space
    worksheet.set_column(i, i, max_len)                           # set column width

df3.to_excel(output,sheet_name='Drops',index=False)

worksheet = output.sheets['Drops']
header_format = workbook.add_format({'bold':True,'fg_color':'#FFFF00','border': 1})
for i, col in enumerate(df3):                                   # loop through all columns
    worksheet.write(0, i, col, header_format)
    series = df3[col]
    max_len = max((
    series.astype(str).map(len).max(),                 # len of largest item   
    len(str(series.name))                                 # len of column name/header
    )) + 10                                                # adding a little extra space
    worksheet.set_column(i, i, max_len)                           # set column width

df4.to_excel(output,sheet_name='Adds',index=False)

worksheet = output.sheets['Adds']
header_format = workbook.add_format({'bold':True,'fg_color':'#FFFF00','border': 1})
for i, col in enumerate(df4):                                   # loop through all columns
    worksheet.write(0, i, col, header_format)
    series = df4[col]
    max_len = max((
    series.astype(str).map(len).max(),                 # len of largest item   
    len(str(series.name))                                 # len of column name/header
    )) + 10                                                # adding a little extra space
    worksheet.set_column(i, i, max_len)                           # set column width

output.save()

remove(custom_out1)
remove(net_out1)
remove(drop_out1)
remove(add_out1)


#***********************************************************CLASS 2******************************************
custom_out2 = Custom2 +'.txt'
net_out2 = Net2 +'.txt'
drop_out2 = Drops2 +'.txt'
add_out2 = Adds2 +'.txt'


Myzftp.download_text(Custom2, custom_out2)
Myzftp.download_text(Net2, net_out2)
Myzftp.download_text(Drops2, drop_out2)
Myzftp.download_text(Adds2, add_out2)

df5 = compare_aud(custom_out2)
df6 = compare_aud(net_out2)
df7 = Audit(drop_out2)
df8 = Audit(add_out2)


output = pd.ExcelWriter('Custom_NET_adds_deletes_2_'+curr+'.xlsx', engine='xlsxwriter')

df5.to_excel(output,sheet_name='custom audit 2',index=False)

workbook  = output.book
worksheet = output.sheets['custom audit 2']
header_format = workbook.add_format({'bold':True,'fg_color':'#FFFF00','border': 1})
for i, col in enumerate(df5):                                   # loop through all columns
    worksheet.write(0, i, col, header_format)
    series = df5[col]
    max_len = max((
    series.astype(str).map(len).max(),                 # len of largest item   
    len(str(series.name))                                 # len of column name/header
    )) + 10                                                # adding a little extra space
    worksheet.set_column(i, i, max_len)                           # set column width

df6.to_excel(output,sheet_name='Net Audit',index=False)

worksheet = output.sheets['Net Audit']
header_format = workbook.add_format({'bold':True,'fg_color':'#FFFF00','border': 1})
for i, col in enumerate(df6):                                   # loop through all columns
    worksheet.write(0, i, col, header_format)
    series = df6[col]
    max_len = max((
    series.astype(str).map(len).max(),                 # len of largest item   
    len(str(series.name))                                 # len of column name/header
    )) + 10                                                # adding a little extra space
    worksheet.set_column(i, i, max_len)                           # set column width

df7.to_excel(output,sheet_name='Drops',index=False)

worksheet = output.sheets['Drops']
header_format = workbook.add_format({'bold':True,'fg_color':'#FFFF00','border': 1})
for i, col in enumerate(df7):                                   # loop through all columns
    worksheet.write(0, i, col, header_format)
    series = df7[col]
    max_len = max((
    series.astype(str).map(len).max(),                 # len of largest item   
    len(str(series.name))                                 # len of column name/header
    )) + 10                                                # adding a little extra space
    worksheet.set_column(i, i, max_len)                           # set column width

df8.to_excel(output,sheet_name='Adds',index=False)

worksheet = output.sheets['Adds']
header_format = workbook.add_format({'bold':True,'fg_color':'#FFFF00','border': 1})
for i, col in enumerate(df8):                                   # loop through all columns
    worksheet.write(0, i, col, header_format)
    series = df8[col]
    max_len = max((
    series.astype(str).map(len).max(),                 # len of largest item   
    len(str(series.name))                                 # len of column name/header
    )) + 10                                                # adding a little extra space
    worksheet.set_column(i, i, max_len)                           # set column width

#extra_sheet = output.add_sheet('FREQUENCY GRID')

output.save()

remove(custom_out2)
remove(net_out2)
remove(drop_out2)
remove(add_out2)
#***************************************************************************


print('Done creating your Excels')





    
   



