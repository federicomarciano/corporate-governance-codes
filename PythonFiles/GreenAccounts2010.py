###############################################################################
#GreenAccounts.py 11/08/2021 ##################################################
###############################################################################
#
# AIM: collect data on green accounts from EPA's website
#
# WARNING: for cvr  26693985 - p  1009209707 - year 2008 and for 
# cvr  31875560 - p 1015165223 - year 2008 (observations 3495 and 4473) there 
# two missing values that I consider as 0.
#
###############################################################################





#####PRELIMINARIES#############################################################


#libraries
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd


#define the dataset
df = pd.DataFrame(columns=['company_name', 'cvr_firm', 'p_number','year', 'air', 'GHGs','strange_air', 'water_rec', 'strange_water_rec',
                           'water_sew', 'strange_water_sew', 'strange_list'])


#dictionary for air emission toxicity weights
dict_air={"1,1,1-trichlorethan":0.7, 
          "1,1,2,2-tetrachlorethan":2600, 
          "1,2,3,4,5,6-hexachlorcyclohexan(HCH)":6400000,
          "1,2-dichlorethan (EDC)":93000, 
          "Aldrin":18000000, 
          "Ammoniak (NH3)":7, 
          "Anthracen":3.3, 
          "Arsen og arsenforbindelser (som As)":15000000, 
          "Asbest":170000000, 
          "Bly og blyforbindelser (som Pb)":23000,
          "Cadmium og cadmiumforbindelser (som Cd)":6400000,
          "Chlordan":360000,
          "Chrom og chromforbindelser (som Cr)":43000000,
          "Di-(2-ethylhexyl)phthalat (DEHP)":8600, 
          "Dichlormethan (DCM)":36,
          "Ethylenoxid":11000000,
          "Fluor og uorganiske fluorforbindelser (som HF)":17,
          "Heptachlor":4600000,
          "Hexachlorbenzen (HCB)":1600000, 
          "Hydrogencyanid (HCN)":4400, 
          "Kobber og kobberforbindelser (som Cu)":1500,  
          "Kviksølv og kviksølvforbindelser (som Hg)":12000,
          "Lindan":110000,  
          "Naphthalen":12000, 
          "Nikkel og nikkelforbindelser (som Ni)":930000, 
          "PCDD + PCDF (dioxiner + furaner) (som Teq)":1400000000, 
          "Pentachlorbenzen":1300, 
          "Pentachlorphenol (PCP)":18000,  
          "Polychlorerede biphenyler (PCB)":360000, 
          "Polycykliske aromatiske kulbrinter (PAH)":390000,
          "Tetrachlorethylen (PER)":930, 
          "Tetrachlormethan (TCM)":21000, 
          "Toxaphen":1100000, 
          "Trichlorbenzener (TCB) (alle isomere)":18, 
          "Trichlorethylen":15000, 
          "Trichlormethan":82000, 
          "Vinylchlorid":31000, 
          "Zink og zinkforbindelser (som Zn)":100,
          "Benzen":28000, 
          "Flour og uorganiske flourforbindelser":17, 
          "Bly":23000, 
          "Kviksølv":12000, 
          "Dioxin":1400000000, 
          "Cr":43000000, 
          "Cu":1500, 
          "Pb":23000, 
          "As":15000000, 
          "Ammoniak/ammonium":7,
          "Dioxiner og furaner":1400000000, 
          "Sum af tungmetallerne As":15000000, 
          "Cd":6400000, 
          "Hg":12000, 
          "Nikkel":930000, 
          "Zink":100, 
          "Chrom":43000000, 
          "Kobber":1500, 
          "Zinkstøv":100, 
          "Cadmium":6400000, 
          "Støv fra zinkgryde":100
          }

dict_air_nP={"hch":180,  
          "Calcium":4400, 
          "K (Kalium)":500000,
          "Nitratforbindelser":0.63, 
          "Thalium":14000, 
          "Saltsyre":180, 
          "cobalt":17000000, 
          "selen":180, 
          "Molybdæn":15000, 
          "Mn":12000,
          "Styren":3.5, 
          "Vanadium":140, 
          "Be":8600000, 
          "HCl":180, 
          "cobolt":17000000,
          "Sølv (Ag)":200,
          "Formaldehyd":46000, 
          "HF":250, 
          "Sb":12000, 
          "Antimon":12000, 
          "Kobolt (som Co)":17000000, 
          "V":140}


#dictionary for water emission toxicity weights
dict_water={"1,2-dichlorethan (EDC)":91000, 
            "Alachlor":80000, 
            "Aldrin":17000000,
            "Anthracen":3.3, 
            "Arsen og arsenforbindelser (som As)":1500000,
            "Asbest":170000000, 
            "Atrazin":56, 
            "Benzo(g,h,i)perylen":20000, 
            "Bly og blyforbindelser (som Pb)":18000,
            "Cadmium og cadmiumforbindelser (som Cd)":2000,
            "Chlordan":350000,  
            "Chlorpyrifos":1000, 
            "Chrom og chromforbindelser (som Cr)":500000,
            "Cyanider (som total CN)":200,  
            "Di-(2-ethylhexyl)phthalat (DEHP)":14000, 
            "Dichlormethan (DCM)":2000,
            "Diuron":19000, 
            "Ethylbenzen":1100, 
            "Ethylenoxid":220000, 
            "Heptachlor":4500000, 
            "Hexachlorbenzen (HCB)":1600000, 
            "Hexachlorbutadien (HCBD)":7800, 
            "Kobber og kobberforbindelser (som Cu)":1500, 
            "Kviksølv og kviksølvforbindelser (som Hg)":10000, 
            "Lindan":110000, 
            "Naphthalen":50, 
            "Nikkel og nikkelforbindelser (som Ni)":91, 
            "PCDD + PCDF (dioxiner + furaner) (som Teq)":1400000000, 
            "Pentachlorbenzen":1300, 
            "Pentachlorphenol (PCP)":400000, 
            "Phenoler (som total C)":3.3, 
            "Polychlorerede biphenyler (PCB)":2000000, 
            "Polycykliske aromatiske kulbrinter (PAH)":180000,
            "Simazin":12000, 
            "Tetrachlorethylen (PER)":2100,
            "Tetrachlormethan (TCM)":70000, 
            "Toluen":13, 
            "Total fosfor":50000,  
            "Toxaphen":1100000, 
            "Trichlorbenzener (TCB) (alle isomere)":100,
            "Trichlorethylen":4600, 
            "Trichlormethan":6100, 
            "Trifluralin":770, 
            "Triphenyltin og triphenyltinforbindelser":18000000, 
            "Vinylchlorid":1500000, 
            "Xylener":5, 
            "Zink og zinkforbindelser (som Zn)":3.3, 
            "Benzen":55000, 
            "hch":180, 
            "Total fosfor  (tot-P)":50000, 
            "Bly":18000, 
            "Toluol":13, 
            "Kviksølv":10000, 
            "Dioxin":1400000000, 
            "Total fosfor":50000, 
            "Cr":500000, 
            "Cu":1500, 
            "Pb":18000, 
            "As":1500000, 
            "Dioxiner og furaner":1400000000, 
            "Sum af tungmetallerne As":1500000, 
            "Cd":2000, 
            "Hg":10000, 
            "Total fosfor (tot-P)":50000, 
            "phenol":3.3, 
            "Nikkel":91,
            "Xilen":5,
            "Zink":3.3,
            "Toluene":13,
            "Chrom":500000, 
            "Phenol":3.3,
            "Kobber":1500, 
            "Total P":50000, 
            "Zinkstøv":3.3, 
            "Cadmium":2000,
            "Støv fra zinkgryde":3.3
            }

dict_water_nP= {"Calcium":1000, 
            "K (Kalium)":500000, 
            "Nitratforbindelser":0.63, 
            "Thalium":14000, 
            "Saltsyre":180,
            "selen":200, 
            "Molybdæn":380, 
            "Mn":7.1, 
            "Styren":5, 
            "Vanadium":140, 
            "Be":500, 
            "HCl":180, 
            "Sølv (Ag)":200, 
            "Formaldehyd":5, 
            "HF":25, 
            "Sb":2500, 
            "Antimon":2500, 
            "V":140}

#dictionary for GHG 
dict_GHG={"Chlorfluorcarboner (CFC)":4750,
          "Dinitrogenoxid (N2O)":298, 
          "Hydrochlorfluorcarboner (HCFC)":1810,
          "Hydrofluorcarboner (HFC)":1430,
          "Kuldioxid (CO2)":1, 
          "Metan (CH4)":25,
          "Perfluorcarboner (PFC)":7390,
          "Svovlhexafluorid (SF6)":22800, 
          "Methan (CH4)":25, 
          "Co2":1, 
          "CO2":1, 
          "kuddioxid":1, 
          "Kuldioxid":1, 
          "Kuldioid (CO2)":1}




#####routine###################################################################


#initialize  
base="https://miljoeoplysninger.mst.dk/PrtrPublicering"
url=base+"/Search?ignoreResultSizeLimit=&Virksomhedsnavn=&Aar=Alle&Vejnavn=&By=&Postnr=&Kommune=&CvrNr=&PNr=&MedtagListepunktISoegning=false&ListepunktKategori=&MedtagStofISoegning=false&Stof=&UdledningTilLuft=true&UdledningTilLuft=false&UdledningTilRecipient=true&UdledningTilRecipient=false&UdledningTilVandViaKloak=true&UdledningTilVandViaKloak=false&MedtagAffaldISoegning=false&IkkeFarligtAffald=true&IkkeFarligtAffald=false&BortskafftetIkkefarligt=true&BortskafftetIkkefarligt=false&NyttegoerelseIkkefarligt=true&NyttegoerelseIkkefarligt=false&FarligtAffald=true&FarligtAffald=false&BortskafftetFarligt=true&BortskafftetFarligt=false&NyttegoerelseFarligt=true&NyttegoerelseFarligt=false&FarligtAffaldEksporteret=true&FarligtAffaldEksporteret=false&BortskafftetEskporteret=true&BortskafftetEskporteret=false&NyttegoerelseEskporteret=true&NyttegoerelseEskporteret=false"
html=requests.get(url).text 
soup=bs(html,features='html.parser')
rows=soup.find("tbody").findAll("tr")


#loop
for row in rows: 


#initial page 
 num=row.find("td").text
 company_name= row.find("td").findNext("td").find("a").text
 year=row.find("td").findNext("td").findNext("td").text
 identifier = row.find("td").findNext("td").find("a")["href"]
 identifier = identifier.replace("PrtrPublicering/Virksomhed/Detaljer/","")
 url1=base+"/Virksomhed/Detaljer/"+identifier 
 url2=base+"/Virksomhed/UdledningOgAffald/"+identifier 
 url3=base+"/Virksomhed/UdledningOgAffald/"+identifier 


#company details 
 html=requests.get(url1).text
 soup=bs(html,features='html.parser') 
 div=soup.find("div", {"class":"VirksomhedsDetaljerStamdata"}) 
 cvr_firm=div.find("label").findNext("label").next_sibling 
 p_number=div.find("label").findNext("label").findNext("label").next_sibling 


#emissions
 html=requests.get(url2).text
 soup=bs(html,features='html.parser')
 h4=soup.find("div", id="Body").findAll("h4") 
 strange_list=""

#air
 if "Virksomheden har ikke oplyst, at den har udledninger til luft for det pågældende regnskabsår." in html: 
     air=0
     strange_air=0
     GHGs=0
 else:    
     substances=h4[0].findNext("tbody").findAll("tr")
     air=0 
     strange_air=0
     GHGs=0
     for substance in substances: 
         name=substance.findAll("td")[0].findNext("a").text 
         if name=="": 
             name=substance.findAll("td")[0].text.strip() 
         value=substance.findAll("td")[2].text.replace(",",".")
         if name.strip() in list(dict_air.keys()): 
             if value=='': 
                 value=0
                 print('!!!!MISSING!!!!')
             else: 
                 value=float(value)
             position= list(dict_air.keys()).index(name)
             value=value*list(dict_air.values())[position]
             air=air + value 
         elif name.strip() in list(dict_GHG.keys()): 
             if value=='': 
                 value=0
                 print('!!!!MISSING!!!!')
             else: 
                 value=float(value)
             position= list(dict_GHG.keys()).index(name)
             value=value*list(dict_GHG.values())[position]
             GHGs=GHGs + value 
         else: 
             strange_list=strange_list+name +", "
             strange_air=strange_air+1
            


#water recipient  
 if "Virksomheden har ikke oplyst, at den har udledninger til vand (til recipient) for det pågældende regnskabsår." in html: 
     water_rec=0
     strange_water_rec=0
 else:    
     substances=h4[1].findNext("tbody").findAll("tr")
     water_rec=0 
     strange_water_rec=0
     for substance in substances: 
         name=substance.findAll("td")[0].findNext("a").text 
         if name=="": 
             name=substance.findAll("td")[0].text.strip() 
         value=substance.findAll("td")[2].text.replace(",",".").strip()
         if name.strip() in list(dict_water.keys()): 
             if value=='': 
                 value=0
                 print('!!!!MISSING!!!!')
             else: 
                 value=float(value)
             position= list(dict_water.keys()).index(name)
             value=value*list(dict_water.values())[position]
             water_rec=water_rec + value
         else: 
             strange_list=strange_list+name+", "
             strange_water_rec=strange_water_rec+1 

#water_sewer
 if "Virksomheden har ikke oplyst, at den har udledninger til vand (via kloak) for det pågældende regnskabsår." in html: 
     water_sew=0
     strange_water_sew=0
 else:    
     substances=h4[2].findNext("tbody").findAll("tr")
     water_sew=0 
     strange_water_sew=0
     for substance in substances: 
         name=substance.findAll("td")[0].findNext("a").text 
         if name=="": 
             name=substance.findAll("td")[0].text.strip() 
         value=substance.findAll("td")[2].text.replace(",",".").strip() 
         if name.strip() in list(dict_water.keys()): 
             if value=='': 
                 value=0
                 print('!!!!MISSING!!!!')
             else:
                 value=float(value)
             position= list(dict_water.keys()).index(name)
             value=value*list(dict_water.values())[position]
             water_sew=water_rec + value
         else:
             strange_list=strange_list+name+", "
             strange_water_sew=strange_water_sew+1 




#add a row 
 values_to_add={'company_name':company_name, 'cvr_firm':cvr_firm, 'p_number':p_number, 
                'year':year, 'air':air, 'GHGs':GHGs, 'strange_air':strange_air,
                'water_rec':water_rec, 'strange_water_rec':strange_water_rec, 
                'water_sew':water_sew, 'strange_water_sew':strange_water_sew, 
                'strange_list':strange_list}
 row_to_add=pd.Series(values_to_add)
 df=df.append(row_to_add, ignore_index=True)
 print(num)
 
 
#save
writer=pd.ExcelWriter('GreenAccounts2010.xlsx')
df.to_excel(writer, index=False)
writer.save()
