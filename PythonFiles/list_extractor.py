import pandas as pd 
from googletrans import Translator
import os 
os.chdir(r"C:\Users\Dell\Dropbox\Il mio PC (DESKTOP-6UGDNEK)\Desktop\research_professional\corporate-governance-codes\PythonFiles")
translator = Translator()
df=pd.read_excel("GreenAccounts2010.xlsx")
df = df.fillna('')
def comma(word):
    if word=="nan": 
        word = ""
    output=str(word)
    return output 

df['strange_list']=df['measure_list'].apply(comma)

string=''.join(df.strange_list)
li=string.split(" !!! ")
se=set(li)
n=0
for i in se: 
       n=n+1
       if n>256: 
          result=translator.translate(i, src='da')
          print(" "+str(n)+" "+result.text+" - "+result.origin) 

