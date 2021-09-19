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
df = pd.DataFrame(columns=['company_name', 'cvr_firm', 'p_number','year', 'GHGs','air_tox', 'water_rec', 'water_sew'])


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
          "Kobber og kobberforbindelser (som Cu)":1/100, 
          "Kuldioxid (CO2)":1/100000000, 
          "Kulmonoxid (CO)":1/500000, 
          "Kviksølv og kviksølvforbindelser (som Hg)":1/10,
          "Kvælstofoxider (NOx/NO2)":1/100000, 
          "Lindan":1, 
          "Metan (CH4)":1/100000, 
          "Mirex":1,
          "Naphthalen":1/100, 
          "Nikkel og nikkelforbindelser (som Ni)":1/50, 
          "Partikler (PM10)":1/50000, 
          "PCDD + PCDF (dioxiner + furaner) (som Teq)":1/0.0001, 
          "Pentachlorbenzen":1, 
          "Pentachlorphenol (PCP)":1/10, 
          "Perfluorcarboner (PFC)":1/100, 
          "Polychlorerede biphenyler (PCB)":1/0.1, 
          "Polycykliske aromatiske kulbrinter (PAH)":1/50,
          "Svovlhexafluorid (SF6)":1/50, 
          "Svovloxider (SOx/SO2)":1/150000, 
          "Tetrachlorethylen (PER)":1/2000, 
          "Tetrachlormethan (TCM)":1/100, 
          "Toxaphen":1, 
          "Trichlorbenzener (TCB) (alle isomere)":1/10, 
          "Trichlorethylen":1/2000, 
          "Trichlormethan":1/500, 
          "Vinylchlorid":1/1000, 
          "Zink og zinkforbindelser (som Zn)":1/200}


#dictionary for water emission toxicity weights
dict_water={"1,2-dichlorethan (EDC)":91000, 
            "Alachlor":1, 
            "Aldrin":17000000,
            "Anthracen":3.3, 
            "Arsen og arsenforbindelser (som As)":1500000,
            "Asbest":170000000, 
            "Atrazin":1,"Benzen":200, 
            "Benzo(g,h,i)perylen":1, 
            "Bly og blyforbindelser (som Pb)":18000,
            "Bromerede diphenylethere (PBDE)":1, 
            "Cadmium og cadmiumforbindelser (som Cd)":2000,
            "Chloralkaner, C10-C13":1, 
            "Chlordan":350000,  
            "Chlorfenvinfos":1,
            "Chlorider (som total Cl)":1/2000000, 
            "Chlorpyrifos":1, 
            "Chrom og chromforbindelser (som Cr)":500000,
            "Cyanider (som total CN)":1/50, 
            "DDT":1, 
            "Di-(2-ethylhexyl)phthalat (DEHP)":14000, 
            "Dichlormethan (DCM)":2000,
            "Diuron":1, 
            "Endosulfan":1, 
            "Ethylbenzen":1/200, 
            "Ethylenoxid":220000,
            "Fluoranthen":1, 
            "Fluorider (som total F)":1/2000, 
            "Halogenerede organiske forbindelser (som AOX)":1/1000,
            "Heptachlor":4500000, 
            "Hexachlorbenzen (HCB)":1600000, 
            "Hexachlorbutadien (HCBD)":7800,
            "Isodrin":1, "Isoproturon":1, 
            "Kobber og kobberforbindelser (som Cu)":1/50, 
            "Kviksølv og kviksølvforbindelser (som Hg)":1, 
            "Lindan":1, 
            "Mirex":1, 
            "Naphthalen":1/10, 
            "Nikkel og nikkelforbindelser (som Ni)":1/20, 
            "Nonylphenol og nonylphenolethoxylater (NP/NPE)":1, 
            "Octylphenoler og octylphenolethoxylater":1, 
            "Organiske tinforbindelser(som total Sn)":1/50, 
            "PCDD + PCDF (dioxiner + furaner) (som Teq)":1/0.0001, 
            "Pentachlorbenzen":1, 
            "Pentachlorphenol (PCP)":1, 
            "Phenoler (som total C)":1/20, 
            "Polychlorerede biphenyler (PCB)":1/0.1, 
            "Polycykliske aromatiske kulbrinter (PAH)":1/5,
            "Simazin":1, 
            "Tetrachlorethylen (PER)":1/10,
            "Tetrachlormethan (TCM)":1, 
            "Toluen":1/200, 
            "Total fosfor":1/5000, 
            "Total kvælstof":1/50000, 
            "Totalmængde organisk kulstof (TOC) (som total C eller COD/3)":1/50000, 
            "Toxaphen":1, 
            "Tributyltin og tributyltinforbindelser":1, 
            "Trichlorbenzener (TCB) (alle isomere)":1,
            "Trichlorethylen":1/10, 
            "Trichlormethan":1/10, 
            "Trifluralin":1, 
            "Triphenyltin og triphenyltinforbindelser":1, 
            "Vinylchlorid":1/10, 
            "Xylener":1/200, 
            "Zink og zinkforbindelser (som Zn)":1/100}

#dictionary for GHG 
dict_GHG={"Chlorfluorcarboner (CFC)":4750,"Dinitrogenoxid (N2O)":298, "Hydrochlorfluorcarboner (HCFC)":1810,
                    "Hydrofluorcarboner (HFC)":1430, }




#####routine###################################################################


#initialize  
base="https://miljoeoplysninger.mst.dk/PrtrPublicering"
url=base+"/Search?ignoreResultSizeLimit=&Virksomhedsnavn=&Aar=Alle&Vejnavn=&By=&Postnr=&Kommune=&CvrNr=&PNr=&MedtagListepunktISoegning=false&ListepunktKategori=&MedtagStofISoegning=false&Stof=&UdledningTilLuft=true&UdledningTilLuft=false&UdledningTilRecipient=true&UdledningTilRecipient=false&UdledningTilVandViaKloak=true&UdledningTilVandViaKloak=false&MedtagAffaldISoegning=false&IkkeFarligtAffald=true&IkkeFarligtAffald=false&BortskafftetIkkefarligt=true&BortskafftetIkkefarligt=false&NyttegoerelseIkkefarligt=true&NyttegoerelseIkkefarligt=false&FarligtAffald=true&FarligtAffald=false&BortskafftetFarligt=true&BortskafftetFarligt=false&NyttegoerelseFarligt=true&NyttegoerelseFarligt=false&FarligtAffaldEksporteret=true&FarligtAffaldEksporteret=false&BortskafftetEskporteret=true&BortskafftetEskporteret=false&NyttegoerelseEskporteret=true&NyttegoerelseEskporteret=false"
html=requests.get(url).text 
soup=bs(html,features='html.parser')
rows=soup.find("tbody").findAll("tr")


#loop
for row in rows[4473:]: 


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
 

#air
 if "Virksomheden har ikke oplyst, at den har udledninger til luft for det pågældende regnskabsår." in html: 
     air=0
 else:    
     substances=h4[0].findNext("tbody").findAll("tr")
     air=0 
     for substance in substances: 
         name=substance.findAll("td")[0].findNext("a").text 
         if name=="": 
             name=substance.findAll("td")[0].text.strip() 
         value=substance.findAll("td")[2].text.replace(",",".")
         if name in list(dict_air.keys()): 
             if value=='': 
                 value=0
                 print('!!!!MISSING!!!!')
             else: 
                 value=float(value)
             position= list(dict_air.keys()).index(name)
             value=value*list(dict_air.values())[position]
             air=air + value 


#water recipient  
 if "Virksomheden har ikke oplyst, at den har udledninger til vand (til recipient) for det pågældende regnskabsår." in html: 
     water_rec=0
 else:    
     substances=h4[1].findNext("tbody").findAll("tr")
     water_rec=0 
     for substance in substances: 
         name=substance.findAll("td")[0].findNext("a").text 
         if name=="": 
             name=substance.findAll("td")[0].text.strip() 
         value=substance.findAll("td")[2].text.replace(",",".").strip()
         if name in list(dict_water.keys()): 
             if value=='': 
                 value=0
                 print('!!!!MISSING!!!!')
             else: 
                 value=float(value)
             position= list(dict_water.keys()).index(name)
             value=value*list(dict_water.values())[position]
             water_rec=water_rec + value
             

#water_sewer
 if "Virksomheden har ikke oplyst, at den har udledninger til vand (via kloak) for det pågældende regnskabsår." in html: 
     water_sew=0
 else:    
     substances=h4[2].findNext("tbody").findAll("tr")
     water_sew=0 
     for substance in substances: 
         name=substance.findAll("td")[0].findNext("a").text 
         if name=="": 
             name=substance.findAll("td")[0].text.strip() 
         value=substance.findAll("td")[2].text.replace(",",".").strip() 
         if name in list(dict_water.keys()): 
             if value=='': 
                 value=0
                 print('!!!!MISSING!!!!')
             else:
                 value=float(value)
             position= list(dict_water.keys()).index(name)
             value=value*list(dict_water.values())[position]
             water_sew=water_rec + value


#non hazardous waste 
 if "Virksomheden har ikke oplyst, om den har ikke-farligt affald til bortskaffelse eller nyttiggørelse for det pågældende regnskabsår." in html: 
     nhaz_waste_rec=0
     nhaz_waste_disp=0
 else:    
     substances=h4[3].findNext("tbody").findAll("tr")
     nhaz_waste_rec=0
     nhaz_waste_disp=0    
     for substance in substances: 
         name=substance.findAll("td")[0].text 
         value=substance.findAll("td")[1].text.replace(",",".").strip()
         if value!='': 
             value=float(value)
         else: 
             value=0 
             print('!!!!MISSING!!!!')
         if name=="Nyttiggørelse": 
             nhaz_waste_rec=nhaz_waste_rec + value 
         else: 
             nhaz_waste_disp=nhaz_waste_disp + value


#hazardous waste 
 if "Virksomheden har ikke oplyst, om den har farligt affald til bortskaffelse eller nyttiggørelse for det pågældende regnskabsår." in html: 
     haz_waste_rec=0
     haz_waste_disp=0
 else:    
     substances=h4[4].findNext("tbody").findAll("tr")
     haz_waste_rec=0
     haz_waste_disp=0    
     for substance in substances: 
         name=substance.findAll("td")[0].text 
         value=substance.findAll("td")[1].text.replace(",",".").strip()
         if value!='': 
             value=float(value)
         else: 
             value=0  
             print('!!!!MISSING!!!!')
         if name=="Nyttiggørelse": 
             haz_waste_rec=haz_waste_rec + value 
         else: 
             haz_waste_disp=haz_waste_disp + value

#add a row 
 values_to_add={'company_name':company_name, 'cvr_firm':cvr_firm, 'p_number':p_number, 'year':year,
                'air':air, 'water_rec':water_rec, 'water_sew':water_sew,
                'nhaz_waste_rec':nhaz_waste_rec, 'nhaz_waste_disp':nhaz_waste_disp,
                'haz_waste_rec':haz_waste_rec, 'haz_waste_disp':haz_waste_disp}
 row_to_add=pd.Series(values_to_add)
 df=df.append(row_to_add, ignore_index=True)
 print(num)
 
 
#save
writer=pd.ExcelWriter('GreenAccounts2008.xlsx')
df.to_excel(writer, index=False)
writer.save()