#settlesments to insure pacakges are installed correctly
import subprocess
import sys

def install(package):
    subprocess.check_call([sys.executable, "-m","pip", "install", package])



#list of library imports
import os
from datetime import datetime
from traceback import print_tb
from queries import get_FolioTitlesQuery, get_materialTypeQuery
from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from collections import Counter
import tkinter as tk
from tkinter import filedialog
import re

#if getting code from github uncomment the line below
#from databaseConnecttemplate import get_connectionString 

#if getting code from github comment out the line below
from dbConnect import get_connectionString 
#end of imports

#output class to build the output and directory.  may need to modify where the target directory goes
def outputfiles(folioDF,EbscoDF,mergeDF):
    outputDir = '.\FY202XStats\output' #define output folder, should it not exist it will be created 
    #logic check to see if output directory exists
    isExist = os.path.exists(outputDir)
    print(isExist)
    if not isExist:
        # Create a new directory because it does not exist 
        os.makedirs(outputDir)
        print("The new directory is created!")
    
    #save dataframe outputs to output directory with a date, outputs all dataframes entered as both csv or excel files
    date = datetime.now()
    dt = date.strftime("%d%m%Y")
    folioDF.to_csv(f'{outputDir}/Folio_Title_{dt}.csv', index=False)
    folioDF.to_excel(f'{outputDir}/Folio_Title_{dt}.xlsx', index=False) 
    EbscoDF.to_csv(f'{outputDir}/Ebsco_Title_{dt}.csv', index=False)
    EbscoDF.to_excel(f'{outputDir}/Ebsco_Title_{dt}.xlsx', index=False)
    mergeDF.to_csv(f'{outputDir}/merge_Title_{dt}.csv', index=False)
    mergeDF.to_excel(f'{outputDir}/merge_Title_{dt}.xlsx', index=False) 

##Folio stuff
def foiloTitles():
    try:
        # GET THE CONNECTION OBJECT (ENGINE) FOR THE DATABASE
        engine = create_engine(
            url=get_connectionString()) #pull connection string from dbConnect.py so that connection isn't hard coded in main file
        print(
            f"Connection created successfully.")
    except Exception as ex:
        print("Connection could not be made due to the following error: \n", ex)

#create connection and execute query from quries.py
    with engine.connect() as conn: 
            #Note call the text() from sqlachemy to turn the text string result from get_inventoryQuery() into an executable SQL
        #pull out the material type list from FOLIO
        df_MatrerialType = pd.DataFrame(conn.execute(text(get_materialTypeQuery())))
        #pulls the records from Folio
        df_UM_Title = pd.DataFrame(conn.execute(text(get_FolioTitlesQuery())))

    #microforms records had migrated with the incorrect material type in some cases 
    #the below logic addresses this and makes the data consistant to our needs 

    #microform extractions of the material type Microforms, pulls the material type 'Microform' from the large dataset of titles into its own DF, it will be recombine later on
    #mt = material type
    microfrom_remove_filter = df_UM_Title['material_type'] == 'Microform'
    df_um_mt_microform = pd.DataFrame(df_UM_Title.loc[microfrom_remove_filter])
    condition = df_UM_Title['title'].isin(df_um_mt_microform['title'])
    df_UM_Title.drop(df_UM_Title[condition].index,inplace=True)

    #this extracts and assigns the records from df_um_titles where the title contains 'microform' and assigns them to a new Dataframe
    #title = microform marker [microform] contained in the title info
    DF_um_title_microform = pd.DataFrame(df_UM_Title.loc[df_UM_Title['title'].str.contains("microform")==True])
    DF_um_title_microform['material_type'].value_counts()
    #pulls both microform Dataframes and combines them into one DF to be joined back with the records 
    df_Microform = pd.concat([df_um_mt_microform,DF_um_title_microform])
    #assign the proper datatype for all microform records
    df_Microform = df_Microform.assign(material_type='Microform')
    condition = df_UM_Title['title'].isin(DF_um_title_microform['title'])
    df_UM_Title.drop(df_UM_Title[condition].index,inplace=True)

    #pull the full Microform dataframes and attaches it back to the total Title dataframes with the corrected material type assigned 
    df_UM_Title = pd.concat([df_UM_Title,df_Microform])


    #to start the count by material type using for loops sorta the long form is a bit more cumbersom but this allows a more direct build of the title/volume array by material type
    for i in df_MatrerialType['name']:
        #print(i)
        #x = str(i)
        
        #build the filter used 
        filter = df_UM_Title['material_type'] == i
        
        #this will create local dataframes, use name has no dashes or spaces 
        #df_um_titles below will be changed to df_um_tiles_sans_Microforms to account for moving microform to its own
        
        #CondencedMatType = df_um_titles[filter]
        
        #creating the variable Dataframe
        locals()["df_"+i] = df_UM_Title.loc[filter]
        push = df_UM_Title.loc[filter]
        #this will give a little print out for the builds as this runs
        #volume = df record count 
        #title = unique Instance record count. we need to use the pd.unique value because our df is built from ITEM records first meaning that the 1:many ratio is in reverse.
        print(f'Material Type | {i}')
        uniquetitle = pd.unique(push['holdingid'])
        print(f'Unique holdings for {i} material type | {len(uniquetitle)}')
        uniqueInstance = pd.unique(push['instanceid'])
        print(f'Unique Instances for {i} material type | {len(uniqueInstance)}')
        #df_list.append("df_"+str(CondencedMatType))
        #casting dataframe to Numpy for tile count resolve
        #locals()["np_"+i] = locals()["df_"+i].title.to_numpy()
        #use counter object to allow for title count
        #locals()["cobj_"+i] = Counter(locals()["np_"+i])
        #locals()["keys_"+i] = locals()["cobj_"+i].keys()
        #locals()["tcount_"+i] = len(locals()["keys_"+i])
        #pulls the # of unique instances into the local variables  
        locals()["tcount_"+i] = len(uniqueInstance)


    #builds out consolidation volume/title count df for Folio related amounts
    header = ['Material_Type', 'Volume_Count', 'Title_Count']
    df = pd.DataFrame() #columns=header
    for i in df_MatrerialType['name']:
        data = pd.DataFrame([[i, len(locals()['df_'+i]), locals()['tcount_'+i]]],columns=header)
    #df = df.append(data)
        df = pd.concat([df,data])

    df.keys()
    filterNull = df.Volume_Count != 0
    df.index
    FT = df[filterNull]
    return FT

#be aware of 'mark for delete record', surpression flag in item and holdings.  "discovery_suppress"(may need to look more into this) "item status"(only on item record)

# for ACRL marks for use of totaling 
# T0 = EXCLUDED
# T1 = MONOGRAPHS
# T2 = E-BOOKS
# T3 = DATABASE
# T4 = PHYSICAL MEDIA
# T5 = DIGITAL MEDIA
# T6 = SERIALS
# T7 = E-SERIALS
# T8 = include only in total physical title count
# T9 = inlcude only in digital title count

# for the total physical counts 
#    Physical volume counts should include T1, T4, T6
#    Physical title count should include T1, T4, T6, T8
# for the total digital count (only need title count)
#    digital title count should include T2, T3, T5, T7, T9

#this marks the the material type for later summations based on the questions asked and how we interprate what material types need to be reported for what 
def FolioForm(folio):
    folioReportCountMark = []
    acrlMark = []
    arlMarkQ1 = []
    arlMarkQ2 = []
    arlMarkQ4 = []

    for i in folio['Material_Type']:
        match i:
            case 'E-Book Package':
                folioReportCountMark.append('E-Book')
                acrlMark.append('T2')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('Y')
                arlMarkQ4.append('Y')
            case 'Map':
                folioReportCountMark.append('Physical Media') #
                acrlMark.append('T4')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Equipment':
                folioReportCountMark.append('Exclude') #Equipment
                acrlMark.append('T0')
                arlMarkQ1.append('Y') 
                arlMarkQ2.append('') 
                arlMarkQ4.append('')
            case 'E-Journal Package':
                folioReportCountMark.append('E-Serial')
                acrlMark.append('T7')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Book':
                folioReportCountMark.append('Book')
                acrlMark.append('T1')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('Y')
                arlMarkQ4.append('')
            case 'Streaming Video':
                folioReportCountMark.append('Digital Media') #Streaming Video
                acrlMark.append('T5')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Artifact/Object':
                folioReportCountMark.append('Exculded') # Artifact/Oject
                acrlMark.append('T0')
                arlMarkQ1.append('Y') 
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'E-Newspaper':
                folioReportCountMark.append('E-Serial')
                acrlMark.append('T7')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Admin':
                folioReportCountMark.append('Admin')
                acrlMark.append('T0')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Database':
                folioReportCountMark.append('Database')
                acrlMark.append('T3')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Data File':
                folioReportCountMark.append('Data File')
                acrlMark.append('T5')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Supplement':
                folioReportCountMark.append('Supplement')
                acrlMark.append('T0')
                arlMarkQ1.append('Y') #?
                arlMarkQ2.append('Y') #RUN WITH AND WITHOUT TO SEE QUESTION 2 NUMBER DIFFERENCE 
                arlMarkQ4.append('')
            case 'E-Journal':
                folioReportCountMark.append('E-Serial')
                acrlMark.append('T7')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'CD-ROM':
                folioReportCountMark.append('Physical Media')
                acrlMark.append('T4')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Film':
                folioReportCountMark.append('Physical Media')
                acrlMark.append('T4')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Audio CD':
                folioReportCountMark.append('Physical Media')
                acrlMark.append('T4')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Newspaper':
                folioReportCountMark.append('Serial')
                acrlMark.append('T6')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('Y') #RUN WITH AND WITH OUT TO SEE IF Q2
                arlMarkQ4.append('')
            case 'LP Phonorecord':
                folioReportCountMark.append('Physical Media')
                acrlMark.append('T4')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Video Game':
                folioReportCountMark.append('Physical Media')
                acrlMark.append('T4')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'E-Score':
                folioReportCountMark.append('E-Book')
                acrlMark.append('T2')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('Y')#?
                arlMarkQ4.append('Y')
            case 'Analog Game':
                folioReportCountMark.append('Exclude')
                acrlMark.append('T0')
                arlMarkQ1.append('Y') #?
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Journal':
                folioReportCountMark.append('Serial')
                acrlMark.append('T6')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('Y') #RUN WITH AND WITHOUT CHECK NUMBER QUESTION 4
                arlMarkQ4.append('')
            case 'Archival material':
                folioReportCountMark.append('Archival')
                acrlMark.append('T4')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('Y') #ADDED BECAUSE WE CATALOG ALOT OF BOOK MATERIAL FOR SCUA
                arlMarkQ4.append('')
            case 'Microform':
                folioReportCountMark.append('Microform')
                acrlMark.append('T4')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'E-Thesis/Dissertation':
                folioReportCountMark.append('E-Book')
                acrlMark.append('T2')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('Y')
                arlMarkQ4.append('Y')
            case 'Government Publication':
                folioReportCountMark.append('Title/volume')
                acrlMark.append('T1')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('Y')
                arlMarkQ4.append('')
            case 'Score':
                folioReportCountMark.append('title/volume')
                acrlMark.append('T1')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('Y')
                arlMarkQ4.append('')
            case 'Serial':
                folioReportCountMark.append('Serial')
                acrlMark.append('T6')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('Y') #RUN WITH AND WITHOUT CHECK NUMBERS 
                arlMarkQ4.append('')
            case 'Audiocassette':
                folioReportCountMark.append('Physical Media')
                acrlMark.append('T4')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'E-Book':
                folioReportCountMark.append('E-Book')
                acrlMark.append('T2')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('Y')
                arlMarkQ4.append('Y')
            case 'unspecified':
                folioReportCountMark.append('exclude')
                acrlMark.append('T0')
                arlMarkQ1.append('')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Musical Instrument':
                folioReportCountMark.append('exclude')
                acrlMark.append('T0')
                arlMarkQ1.append('Y') #?
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Image':
                folioReportCountMark.append('Image')
                acrlMark.append('T4')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Videocassette':
                folioReportCountMark.append('Physical Media')
                acrlMark.append('T4')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Thesis/Dissertation':
                folioReportCountMark.append('Book')
                acrlMark.append('T1')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('Y')
                arlMarkQ4.append('')
            case 'DVD/Blu-ray':
                folioReportCountMark.append('Physical Media')
                acrlMark.append('T4')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Streaming Audio':
                folioReportCountMark.append('Digital Media')
                acrlMark.append('T5')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
    folio['report_count'] = folioReportCountMark
    folio['ACRL_Marks'] = acrlMark
    folio['ARLQ1'] = arlMarkQ1
    folio['ARLQ2'] = arlMarkQ2
    folio['ARLQ4'] = arlMarkQ4
    #return folioReportCountMark


##Ebsco stuff
def EbscoTitles():
    root = tk.Tk()
    root.withdraw()
    #needs to load file from ebsco HLM reporting, if not using ebsco as your discovery layer code may(will) require some modifications
    file_path = filedialog.askopenfilename()
    #if this take a long time miniize the window its likely that the dialog didnt take focus

    #assign file to dataframe
    Ebsco = pd.DataFrame(pd.read_csv(file_path, index_col=False))

    #for whatever reason all january downloads of the hlm report from ebsco is not counting 'KBID' number column as its own column so all values are shifted to the left by 1 column
    #trim dataframe to just what i need to use (kbid = ebsco's 'propriatry' unique idenifier, title = article/object title, ResoruceType = what ebsco defines the material type is)
    cutdf = Ebsco[['KBID','Title','ResourceType']]
#    cutdf = Ebsco[['KBID','Title','Subject']]
    #print(cutdf)
    resource = cutdf['ResourceType'].value_counts()
#    resource = cutdf['Subject'].value_counts()
    resource.to_numpy()

    #ebsco uses weird material types as such we need to regex some of it it before we can process the unique Kbid value
    for i in resource.keys():
        x = str(i)
        condencedMatType = re.sub(r'[^A-Za-z0-9]+','',x)
        print(x)
        filter = cutdf['ResourceType'] == i
    #    filter = cutdf['Subject'] == i
        Ufill = cutdf.loc[filter]
        unique = pd.unique(Ufill['KBID'])
        
        locals()["df_"+i] = cutdf.loc[filter]
        
        
        #casting dataframe to Numpy for tile count resolve
        
        #locals()["np_"+i] = locals()["df_"+i].Title.to_numpy()
        
        #use counter object to allow for title count
        
        #locals()["cobj_"+i] = Counter(locals()["np_"+i])
        #locals()["keys_"+i] = locals()["cobj_"+i].keys()
        #locals()["tcount_"+i] = len(locals()["keys_"+i])
        locals()["tcount_"+i] = len(unique)

    header = ['Material_Type','Volume_Count','Title_Count']
    df = pd.DataFrame() #columns=header

    for i in resource.keys():
        #print(i)
        data = pd.DataFrame([[i, len(locals()['df_'+i]), locals()['tcount_'+i]]], columns=header)
        #print(data)
        df = pd.concat([df,data])
    return df

def EbscoContentType():
    root = tk.Tk()
    root.withdraw()

    file_path = filedialog.askopenfilename()
#if this take a long time miniize the window its likely that the dialog didnt take focus

#assign file to dataframe
    Ebsco = pd.DataFrame(pd.read_csv(file_path))

    cutdf = Ebsco[['Title','PackageContentType']]
    resource = cutdf['PackageContentType'].value_counts()
    resource.to_numpy()

    for i in resource.keys():
        x = str(i)
        condencedMatType = re.sub(r'[^A-Za-z0-9]+','',x)
        filter = cutdf['PackageContentType'] == i
        locals()["df_"+i] = cutdf.loc[filter]
        #df_list.append("df_"+str(CondencedMatType))
        #casting dataframe to Numpy for tile count resolve
        locals()["np_"+i] = locals()["df_"+i].Title.to_numpy()
        #use counter object to allow for title count
        locals()["cobj_"+i] = Counter(locals()["np_"+i])
        locals()["keys_"+i] = locals()["cobj_"+i].keys()
        locals()["tcount_"+i] = len(locals()["keys_"+i])

    header = ['Material_Type','Volume_Count','Title_Count']
    dfone = pd.DataFrame() #columns=header

    for i in resource.keys():
        #print(i)
        data = pd.DataFrame([[i, len(locals()['df_'+i]), locals()['tcount_'+i]]], columns=header)
        #print(data)
        df = pd.concat([df,data])
    return dfone

# for ACRL marks for use of totaling 
# T0 = EXCLUDED
# T1 = MONOGRAPHS
# T2 = E-BOOKS
# T3 = DATABASE
# T4 = PHYSICAL MEDIA
# T5 = DIGITAL MEDIA
# T6 = SERIALS
# T7 = E-SERIALS
# T8 = include only in total physical title count
# T9 = inlcude only in digital title count

# for the total physical counts 
#    Physical volume counts should include T1, T4, T6
#    Physical title count should include T1, T4, T6, T8
# for the total digital count (only need title count)
#    digital title count should include T2, T3, T5, T7, T9

def ebscoForm(ebsco):
    ebscoReportCountMark = []
    acrlMarks = []
    arlMarkQ1 = []
    arlMarkQ2 = []
    arlMarkQ4 = []

    for i in ebsco['Material_Type']:
        match i:
            case 'Book':
                ebscoReportCountMark.append('E-Book')
                acrlMarks.append('T2')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('Y')
                arlMarkQ4.append('Y')
            case 'Journal':
                ebscoReportCountMark.append('E-Journal')
                acrlMarks.append('T7')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Report':
                ebscoReportCountMark.append('E-Report')
                acrlMarks.append('T9')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('Y') #RUN BOTH AND SEE
                arlMarkQ4.append('') #Y removed 2023 per SF
            case 'Proceedings':
                ebscoReportCountMark.append('E-Proceedings')
                acrlMarks.append('T9')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('Y') #RUN BOTH AND SEE
                arlMarkQ4.append('') #Y removed 2023 per SF
            case 'Book Series':
                ebscoReportCountMark.append('E-Book Series')
                acrlMarks.append('T2')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('Y')
                arlMarkQ4.append('Y')
            case 'Newspaper':
                ebscoReportCountMark.append('E-Newspaper')
                acrlMarks.append('T7')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Newsletter':
                ebscoReportCountMark.append('E-Newsletter')
                acrlMarks.append('T7')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Web site':
                ebscoReportCountMark.append('E-WebSite')
                acrlMarks.append('T9')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Streaming Video':
                ebscoReportCountMark.append('Streaming Video')
                acrlMarks.append('T5')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Database':
                ebscoReportCountMark.append('Database')
                acrlMarks.append('T3')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Streaming Audio':
                ebscoReportCountMark.append('Streaming Audio')
                acrlMarks.append('T5')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Audio Book':
                ebscoReportCountMark.append('Audio Book')
                acrlMarks.append('T5')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
            case 'Thesis/Dissertation':
                ebscoReportCountMark.append('E-Thesis/Dissertation')
                acrlMarks.append('T2')
                arlMarkQ1.append('Y')
                arlMarkQ2.append('Y')
                arlMarkQ4.append('Y')
            case 'Unspecified':
                ebscoReportCountMark.append('Unspecified')
                acrlMarks.append('T0')
                arlMarkQ1.append('')
                arlMarkQ2.append('')
                arlMarkQ4.append('')
    
    ebsco['report_count'] = ebscoReportCountMark
    ebsco['ACRL_Marks'] = acrlMarks
    ebsco['ARLQ1'] = arlMarkQ1
    ebsco['ARLQ2'] = arlMarkQ2
    ebsco['ARLQ4'] = arlMarkQ4
    #return ebscoReportCountMark




def main():
    #check/install required packages using pip as a subprocess 
    install('numpy')
    install('pandas')
    install('psycopg2')
    install('psycopg2_binary')
    install('SQLAlchemy')


    #run folio titles
    FT = foiloTitles()
    print('Folio Finished')

    FT.sort_values(by=['Material_Type'])
    FolioForm(FT)

    print("select file to continue")
    Eb = EbscoTitles()
    ebscoForm(Eb)


    #analysis code starts
    compound = pd.concat([FT, Eb])
    build_totals = {
    'Monographs': [{
        'Volume' : compound.Volume_Count.loc[compound['ACRL_Marks']=='T1'].sum(),
        'Title' : compound.Title_Count.loc[compound['ACRL_Marks']=='T1'].sum()
    }],
    'E-Books': [{
        'Volume' : compound.Volume_Count.loc[compound['ACRL_Marks']=='T2'].sum(),
        'Title' : compound.Title_Count.loc[compound['ACRL_Marks']=='T2'].sum()
    }],
    'Databases': [{
        'Volume' : compound.Volume_Count.loc[compound['ACRL_Marks']=='T3'].sum(),
        'Title' : compound.Title_Count.loc[compound['ACRL_Marks']=='T3'].sum()
    }],
    'Physical Media': [{
        'Volume' : compound.Volume_Count.loc[compound['ACRL_Marks']=='T4'].sum(),
        'Title' : compound.Title_Count.loc[compound['ACRL_Marks']=='T4'].sum()
    }],
    'Digital Media': [{
        'Volume' : compound.Volume_Count.loc[compound['ACRL_Marks']=='T5'].sum(),
        'Title' : compound.Title_Count.loc[compound['ACRL_Marks']=='T5'].sum()
    }],
    'Serials': [{
        'Volume' : compound.Volume_Count.loc[compound['ACRL_Marks']=='T6'].sum(),
        'Title' : compound.Title_Count.loc[compound['ACRL_Marks']=='T6'].sum()
    }],
    'E-Serials': [{
        'Volume' : compound.Volume_Count.loc[compound['ACRL_Marks']=='T7'].sum(),
        'Title' : compound.Title_Count.loc[compound['ACRL_Marks']=='T7'].sum()
    }],
    'Total Physical Collection': [{
        'Volume' : compound.Volume_Count.loc[compound['ACRL_Marks']=='T1'].sum()+compound.Volume_Count.loc[compound['ACRL_Marks']=='T4'].sum()+compound.Volume_Count.loc[compound['ACRL_Marks']=='T6'].sum()+compound.Volume_Count.loc[compound['ACRL_Marks']=='T8'].sum(),
        'Title' : compound.Title_Count.loc[compound['ACRL_Marks']=='T1'].sum()+compound.Title_Count.loc[compound['ACRL_Marks']=='T4'].sum()+compound.Title_Count.loc[compound['ACRL_Marks']=='T6'].sum()+compound.Title_Count.loc[compound['ACRL_Marks']=='T8'].sum()
    }],
    'Total Digital Collection': [{
        #'Volume' : compound.Volume_Count.loc[compound['ACRL_Marks']=='T1'].sum(),
        'Title' : compound.Title_Count.loc[compound['ACRL_Marks']=='T2'].sum()+compound.Title_Count.loc[compound['ACRL_Marks']=='T3'].sum()+compound.Title_Count.loc[compound['ACRL_Marks']=='T5'].sum()+compound.Title_Count.loc[compound['ACRL_Marks']=='T7'].sum()+compound.Title_Count.loc[compound['ACRL_Marks']=='T9'].sum()
    }],
    'ARL_Question_1': [{
        'Volume' : compound.Volume_Count.loc[compound['ARLQ1']=='Y'].sum(),
        'Title' : compound.Title_Count.loc[compound['ARLQ1']=='Y'].sum()
    }],
    'ARL_Question_2': [{
        'Volume' : compound.Volume_Count.loc[compound['ARLQ2']=='Y'].sum(),
        'Title' : compound.Title_Count.loc[compound['ARLQ2']=='Y'].sum()
    }],
    'ARL_Question_4': [{
        'Volume' : compound.Volume_Count.loc[compound['ARLQ4']=='Y'].sum(),
        'Title' : compound.Title_Count.loc[compound['ARLQ4']=='Y'].sum()
    }],
}
    vantage = pd.DataFrame(build_totals)

    #output block
    outputfiles(FT,Eb,vantage)
    print('Finished')

if __name__ == '__main__':
    main()