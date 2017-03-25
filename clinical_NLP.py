'''
Created on Mar 22, 2016

@author: Binghuang Cai
'''

import csv
import subprocess

from aligner import align_terms
from datetime import datetime

import mysql.connector
import sqlite3

import os
os.sys.path.append('/data/users/bcai/Home/site-packages/')
import nltk
import nltk.data

import time

termID=0

# sentence chunker
def sentenceChuncker(text):
    sent_detector = nltk.data.load('tokenizers/punkt/english.pickle')
    return sent_detector.tokenize(text.strip())


# convert the output of metamap to a table in a csv file
def RunMetamap(dirname,filename):
    
    p = subprocess.Popen(['./bin/metamap','-I','-z','--silent',dirname+filename,dirname+filename+'.out'], cwd='/data/users/xiyao111/public_mm/')
    p.wait()
     
    return

# convert indices of characters in a sentence into word indices
def cha2wordInd(sentence,cStart, cEnd):
    return (len(sentence[0:cStart].split(' ')),len(sentence[0:cEnd].split(' ')))

    
# Version 2: for new location generation in single sentence process by metamap; convert the output of metamap to a table in a csv file
def MetamapOut2TableOS(dirname,sentencefilename,noteID,tableFileName,cur,tablename,psLoc,cProc,sentence):
        
    curTermDict={}
    paragraphInd=0
    mapFoundNum=0
    charProcessed=1 # count of characters already processed including current phrase
    charPreProcessed=0 # count of characters already processed before current phrase
    phrase_start=1 # start index of current phrase in the whole note
    
    termBegInd=1 #term beginning character position in the whole note, counting from 1
    termEndInd=0 # term ending character position in the whole note, counting from 1
    
    global termID
    with open(dirname+sentencefilename+'.out') as noteFile:
        with open(dirname+tableFileName, 'ab') as csvfile:
            pdWriter = csv.writer(csvfile, delimiter='\t')
            #pdWriter.writerow(['Term ID']+['Note ID']+['Location in Note']+['Term CUI']+['UMLS String']+['Concept Preferred Name']+['Concept Semantic Type']+['Phrase'])
            for line in noteFile:
                #print line
                                
                # process sentence title line, get term location
                if line.split(' ')[0]=='Processing':
                    sentenceInd=int(line.split(' ')[1].split('.')[2][0:-1])
                    if sentenceInd==1: 
                        paragraphInd=paragraphInd+1
                        charProcessed=charProcessed-1 # reduce one for non-space after period at the end of the paragraph

                    #curTermDict['Location in Note']='P'+str(paragraphInd)+'.S'+str(sentenceInd)
                    curTermDict['Location in Note']=psLoc
                    mapFoundNum=0; # reset mapping count for new phrase 
                
                # process phrase line, get original phrase for term
                if line.split(' ')[0]=='Phrase:':
                    curTermDict['Phrase']=line.split(': ',1)[1][:-1]  
                    mapFoundNum=0; # reset mapping count for new phrase 
                    charPreProcessed=charProcessed
                    charProcessed=charProcessed+len(curTermDict['Phrase']) # add 1 for space after period
                    #print curTermDict['Phrase']
                    #print len(curTermDict['Phrase'])
                    
                # process meta mapping title lines
                if line.split(' ')[0]=='Meta' and line.split(' ')[1]=='Mapping':                    
                    mapFoundNum=mapFoundNum+1 # count mapping sets
                
                # process meta mapping term lines
                if len(line) - len(line.lstrip(' '))==3 or len(line) - len(line.lstrip(' '))==4: 
                    if mapFoundNum==1: # process first meta mapping currently                        
                        termID=termID+1
                                                
                        curTermDict['Term CUI']=line.split(':')[0][9:17]
                        curTermDict['UMLS String']=line.partition(':')[-1].partition('(')[0][0:-1]
                        if line.partition(':')[-1].rpartition('(')[0]=='': 
                            curTermDict['UMLS String']=line.partition(':')[-1].rpartition('[')[0][0:-1]
                        curTermDict['Concept Preferred Name']=line.partition('(')[-1].rpartition(')')[0]
                        curTermDict['Concept Semantic Type']=line.rpartition('[')[-1].rpartition(']')[0]
                        curTermDict['Term ID']=str(termID)
                        
                        # get the character range of the term in the whole note 
                        try:
                            termBegInd=charPreProcessed+curTermDict['Phrase'].lower().index(curTermDict['UMLS String'].lower())+1 # count starting from 1
                            termEndInd=termBegInd+len(curTermDict['UMLS String'])-1
                            curTermDict['Character Range']='['+str(cProc+termBegInd)+':'+str(cProc+termEndInd)+']'
                        except ValueError:                                                                                    
                            phrase_start=charPreProcessed+1 # beginning index of phrase
                            term_start=termEndInd-charPreProcessed+1 # beginning index to search unmatched term
                            if term_start<1: term_start=1 # get beginning index to search unmatched term for new phrase                        
                            termInd=align_terms(curTermDict['Phrase'],phrase_start,curTermDict['UMLS String'],term_start)
                            termBegInd=termInd[0]
                            termEndInd=termInd[1]
                            curTermDict['Character Range']='['+str(cProc+termBegInd)+':'+str(cProc+termEndInd)+']'
                            if termBegInd==-1:
                                print 'Alignment Error!'
                                print 'Input Parameters for Aligner:'
                                print curTermDict['Phrase']
                                print phrase_start
                                print curTermDict['UMLS String']
                                print term_start 
                        
                        # get the word indices of the term in sentence
                        termWordInd=cha2wordInd(sentence,termBegInd,termEndInd)
                        curTermDict['Word Range']='['+str(termWordInd[0])+':'+str(termWordInd[1])+']'
                        
                        # save to vcf file        
                        pdWriter.writerow([curTermDict['Term ID']]+[noteID]+[curTermDict['Location in Note']]+[curTermDict['Character Range']]+[curTermDict['Word Range']]+[curTermDict['Term CUI']]+[curTermDict['UMLS String']]+[curTermDict['Concept Preferred Name']]+[curTermDict['Concept Semantic Type']]+[curTermDict['Phrase']]+[sentence])
                        # pass to mysql
                        sql='insert into '+tablename+' values (?,?,?,?,?,?,?,?,?,?,?)' # for sqlite
                        #sql='insert into '+tablename+' values (%s,%s,%s,%s,%s,%s,%s,%s,%s)' # for mysql
                        args=(curTermDict['Term ID'],noteID,curTermDict['Location in Note'],curTermDict['Character Range'],curTermDict['Word Range'],curTermDict['Term CUI'],curTermDict['UMLS String'],curTermDict['Concept Preferred Name'],curTermDict['Concept Semantic Type'],curTermDict['Phrase'],sentence)
                        cur.execute(sql,args)

                        #cur.execute('insert into '+tablename+' values ("'+curTermDict['Term ID']+'","'+noteID+'","'+curTermDict['Location in Note']+'","'+curTermDict['Character Range']+'","'+curTermDict['Term CUI']+'","'+curTermDict['UMLS String']+'","'+curTermDict['Concept Preferred Name']+'","'+curTermDict['Concept Semantic Type']+'","'+curTermDict['Phrase']+'")')

    return


if __name__ == '__main__':
    pass

start_time = time.time()

##########################
#run on server
dataPath='/data/users/bcai/Home/Projects/ClinicalNotes/Data/notesSample/'
#dataPath='/data/users/bcai/Home/Projects/ClinicalNotes/Data/mtsamples/txt/'
outPath='/data/users/bcai/Home/Projects/ClinicalNotes/Program/'

#########################

#########################
#local test
#dataPath='/Users/admin/Documents/BCai_Doc/Project/ClinicalNotes/Data/notesSample/'
#dataPath='/data/users/bcai/Home/Projects/ClinicalNotes/Data/mtsamples/txt/'
#outPath=''
########################

# timestr to generate unique mysql table name and unique csv table file name
timestr = datetime.now().strftime('%Y%m%d%H%M%S%f') 
tablename='NoteTable_'+timestr

'''
#####mysql###########
#initial mysql dataset tables
cnx = mysql.connector.connect(user='root', password='iFfuOa_5*:0o',
                              host='127.0.0.1',
                              database='CNP_Tables')
cur=cnx.cursor()
cur.execute('create table '+tablename+' (TermID char(20),NoteID char(20),Location char(30),Range char(30),TermCUI char(10),UMLSString varchar(300),ConceptPreferredName varchar(300),ConceptSemanticType char(200),Phrase varchar(1000)),Sentence varchar(1000)')
#####################
'''

#####sqlite3 test for server#########
#initial sqlite3 dataset tables
cnx = sqlite3.connect('CNP_Tables1.db')
cnx.text_factory = str
cur=cnx.cursor()
cur.execute('create table '+tablename+' (TermID char(20),NoteID char(20),Location char(30),Range char(30),WordRange char(30),TermCUI char(10),UMLSString varchar(300),ConceptPreferredName varchar(300),ConceptSemanticType char(200),Phrase varchar(1000),Sentence varchar(1000))')
#####################################


# initial output csv file
# delete the old table csv file and create new csv for the note table
try:
    os.remove(outPath+'NoteTable.csv')
except OSError:
    pass


tableFileName='NoteTable_'+timestr+'.txt'
with open(outPath+tableFileName, 'ab') as csvfile:
    pdWriter = csv.writer(csvfile, delimiter='\t')
    pdWriter.writerow(['Term ID']+['Note ID']+['Location in Note']+['Character Range in Note']+['Word Range in Sentence']+['Term CUI']+['UMLS String']+['Concept Preferred Name']+['Concept Semantic Type']+['Phrase']+['Sentence'])


# traves a folder with note txt files.
#for file in os.listdir(dataPath):
pInd=0 # paragraph index in the whole note
sInd=0 # sentence index in a paragraph
cProc=0 # the number of characters already processed

for dirname, subdirlist, filelist in os.walk(dataPath):
    for filename in filelist:
        if filename.endswith(".txt"):
            noteID=filename[0:-4]
            pInd=0
            with open(dirname+'/'+filename) as noteFile:
                text=''
                cProc=0 
                for line in noteFile:
                    
                    if line=='\n':
                        pInd=pInd+1
                        sInd=0
                        sentenceList=sentenceChuncker(text)
                        
                        for sentence in sentenceList:
                            sInd=sInd+1
                            sentencefilename='sentence.txt'
                            text_file = open(outPath+'sentence.txt', 'w')
                            text_file.write(sentence+'\n')
                            text_file.close()
                            
                            RunMetamap(outPath,sentencefilename)
                            psLoc='P'+str(pInd)+'.S'+str(sInd)
                            MetamapOut2TableOS(outPath,sentencefilename,noteID,tableFileName,cur,tablename,psLoc,cProc,sentence)
                            os.remove(outPath+sentencefilename+'.out')
                            cProc=cProc+len(sentence)
                        text=''
                    else:
                        text=text+'\n'+line
                        
'''  
##### for local testing ##### 
# run list of the notes file with their name in a list         
for noteID in noteIDList:
    #RunMetamap(noteID)
    MetamapOut2Table(noteID,tableFileName,cur,tablename)
    os.remove(outPath+noteID+'.txt.out')
###########################
'''
        
cnx.commit()
cur.close()
cnx.close()

with open(outPath+tableFileName+'RunningTime', 'ab') as csvfile:
    pdWriter = csv.writer(csvfile, delimiter='\t')
    pdWriter.writerow([str(time.time() - start_time)+' Seconds'])
