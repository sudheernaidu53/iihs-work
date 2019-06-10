import numpy as np
import pandas as pd
nic_path = 'C:/Users/sudheera/Downloads/nic_mess/'
nic_class = pd.read_csv(nic_path +'nic_class.csv')
nic_class.drop(list(nic_class.columns)[0],axis = 1,inplace=True)
nic_group = pd.read_csv(nic_path +'nic_group.csv')
nic_group.drop(list(nic_group.columns)[0],axis = 1,inplace=True)
nic_subclass = pd.read_csv(nic_path +'nic_subclass.csv')
nic_subclass.drop(list(nic_subclass.columns)[0],axis = 1,inplace=True)
nic_division = pd.read_csv(nic_path+'nic_division.csv')
nic_division.drop(list(nic_division.columns)[0],axis = 1,inplace=True)
nic_section = pd.read_csv(nic_path+'nic_section.csv')
nic_section.drop(list(nic_section.columns)[0],axis = 1,inplace=True)
div_to_sec_dict = pd.read_csv(nic_path+'div_to_sec_dict.csv')
div_to_sec_dict.set_index('division',inplace=True)
div_to_sec_dict = div_to_sec_dict.to_dict('index')
def loadutils():
    nic_path = 'C:/Users/sudheera/Downloads/nic_mess/'
    nic_class = pd.read_csv(nic_path +'nic_class.csv')
    nic_class.drop(list(nic_class.columns)[0],axis = 1,inplace=True)
    nic_group = pd.read_csv(nic_path +'nic_group.csv')
    nic_group.drop(list(nic_group.columns)[0],axis = 1,inplace=True)
    nic_subclass = pd.read_csv(nic_path +'nic_subclass.csv')
    nic_subclass.drop(list(nic_subclass.columns)[0],axis = 1,inplace=True)
    nic_division = pd.read_csv(nic_path+'nic_division.csv')
    nic_division.drop(list(nic_division.columns)[0],axis = 1,inplace=True)
    nic_section = pd.read_csv(nic_path+'nic_section.csv')
    nic_section.drop(list(nic_section.columns)[0],axis = 1,inplace=True)
    div_to_sec_dict = pd.read_csv(nic_path+'div_to_sec_dict.csv')
    div_to_sec_dict.set_index('division',inplace=True)
    div_to_sec_dict = div_to_sec_dict.to_dict('index')
    return (nic_section,nic_division,nic_group,nic_class,nic_subclass,div_to_sec_dict)
def toint(string):
    return(int(string))
def setthem(array):
    return list(set(array))
def divideinto(number):
    if len(str(number))==5: #if it's already of length 5, don't need to do anything
        return str(number)
    elif (nic_subclass['subclass'] == number).sum(): # it's very probable that the nic_code is of 5 digits or 4 digits,
#                                                         we encounter codes with 3 or 2 or 1 digits very rarely
        return str(number).zfill(5)
    elif (nic_class['class'] == number).sum():
        return str(number).zfill(4)
    elif (nic_group['group'] == number).sum():
        return str(number).zfill(3)
    elif (nic_division['division'] == number).sum():
        return str(number).zfill(2)
    else:
        return number
def stripto4(a):
    if len(str(a))==5:
        return(str(a)[:4])
    else:
        return str(a)
def stripto3(a):
    if len(str(a))>3:
        return(str(a)[:3])
    else:
        return str(a)
def stripto2(a):
    if len(str(a))>2:
        return(str(a)[:2])
    else:
        return str(a)
def striptoalpha(a):
    if len(str(a))>=1:
        try:
            return div_to_sec_dict[int(str(a)[:2])]['section']
        except:
            return np.nan
def makeitstring(arr,sep = ' '):
    return sep.join(arr)
def lenone(integer):
    if (len(str(integer))) >= 1:
        return 1
    return 0
def lentwo(integer):
    if (len(str(integer))) >= 2:
        return 1
    return 0
def lenthree(integer):
    if (len(str(integer))) >= 3:
        return 1
    return 0
def lenfour(integer):
    if (len(str(integer))) >= 4:
        return 1
    return 0
def lenfive(integer):
    if (len(str(integer))) >= 5:
        return 1
    return 0
# someof the methods are hell slow, it takes minutes to run the whole program if input csv has lakhs of entries.
# if you are not patient enough, uncomment the print commands, so that you can know the progress
def masterdf(address = 0,nic_type = 'group',extra = None,mergeon = 'company_name',groupon = 'company_name',df = pd.DataFrame(),*args,**kwargs):
    if (df.shape == (0,0)):
#         print(':(')
        return None
#     print(1)
    file = df
    if len(args):
        for arg in args:
            file = pd.merge(file,arg,how='outer',on = mergeon)
    colstokeep = ['company_name','nic_prod_code','nic_name']
#     print(2)
    if extra: 
        colstokeep = colstokeep + extra #extra is a list of column names
    if address:
        colstokeep = colstokeep+['address']
    file = file[colstokeep]    
#     print(file.columns)
    file['correctlength'] = file['nic_prod_code'].apply(divideinto)
#     print(3)
    if nic_type == 'subclass':
        file['filter'] = file['correctlength'].apply(lenfive)
    elif nic_type == 'class':
        file['filter'] = file['correctlength'].apply(lenfour)
#         file = file[colstokeep][file['filter']==1]
        file['nic_prod_code'] = file['correctlength'].apply(stripto4)
    elif nic_type == 'group':
        file['filter'] = file['correctlength'].apply(lenthree)
        file['nic_prod_code'] = file['correctlength'].apply(stripto3)
    elif nic_type == 'division':
        file['filter'] = file['correctlength'].apply(lentwo)
        file['nic_prod_code'] = file['correctlength'].apply(stripto2)
    elif nic_type == 'section':
        file['filter'] = file['correctlength'].apply(lentwo)
#         file['nic_prod_code'] = pd.Series(map(lambda x: ))
        file['nic_prod_code'] = file['correctlength'].apply(striptoalpha)
        
#     print(4)
#     file = file[colstokeep][file['filter']]
    file = file[colstokeep][file['filter']==1]
    file = file.groupby(groupon).agg(lambda x:list(x))
    tempcols = colstokeep
    tempcols.remove(groupon)
#     print(5)
    for i in tempcols:
        file[i] = file[i].apply(setthem)
    return file
