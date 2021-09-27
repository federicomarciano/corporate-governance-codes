#libraries
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd


#define the dataset
df = pd.DataFrame(columns=['company_name', 'cvr_firm', 'p_number','year', 'water'])



dict_water={"1,2-dichlorethan (EDC)":91000, 
            "1,2,3,4,5,6-hexachlorcyclohexan(HCH)":6300000,
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
            "Benzen":55000
            }
import statistics 
med=statistics.median(dict_water.values())

#initialize  
base="https://miljoeoplysninger.mst.dk/PrtrPublicering"
url=base+"/Search?ignoreResultSizeLimit=&Virksomhedsnavn=&Aar=Alle&Vejnavn=&By=&Postnr=&Kommune=&CvrNr=&PNr=&MedtagListepunktISoegning=false&ListepunktKategori=&MedtagStofISoegning=false&Stof=&UdledningTilLuft=true&UdledningTilLuft=false&UdledningTilRecipient=true&UdledningTilRecipient=false&UdledningTilVandViaKloak=true&UdledningTilVandViaKloak=false&MedtagAffaldISoegning=false&IkkeFarligtAffald=true&IkkeFarligtAffald=false&BortskafftetIkkefarligt=true&BortskafftetIkkefarligt=false&NyttegoerelseIkkefarligt=true&NyttegoerelseIkkefarligt=false&FarligtAffald=true&FarligtAffald=false&BortskafftetFarligt=true&BortskafftetFarligt=false&NyttegoerelseFarligt=true&NyttegoerelseFarligt=false&FarligtAffaldEksporteret=true&FarligtAffaldEksporteret=false&BortskafftetEskporteret=true&BortskafftetEskporteret=false&NyttegoerelseEskporteret=true&NyttegoerelseEskporteret=false"
html=requests.get(url).text 
soup=bs(html,features='html.parser')
rows=soup.find("tbody").findAll("tr")




#4555
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

#waste water
 html=requests.get(url2).text
 soup=bs(html,features='html.parser')
 water=0
#water recipient 
 if "Virksomheden har ikke oplyst, at den har udledninger til vand (til recipient) for det pågældende regnskabsår." in html: 
     water=water+0
 else: 
     body=soup.find("table", {"id":"UdledningTilVandRecipientTabel"}).find("tbody")
     substances=body.findAll("tr")
     
     
     for substance in substances: 
         name=substance.findNext("td").text.strip() 
         
         if "vand" in name.lower(): 
             value=substance.findAll("td")[2].text.replace(",",".")
             measure=substance.findAll("td")[3].text.strip().lower()
             #see if the value is 0 
             if value.strip()=='': 
                 value=0
             else: 
                 value=float(value)
             if measure.startswith("l"): 
                 value=value 
             elif measure.startswith("t"): 
                 value=value*1000 
             elif measure.startswith("m"): 
                 value=value*1000 
             elif measure.startswith("kg"):
                 value=value 
             water=water+value
                 

#water sewer 
 if "Virksomheden har ikke oplyst, at den har udledninger til vand (via kloak) for det pågældende regnskabsår." in html: 
     water=water
 else: 
     body=soup.find("table", {"id":"UdledningTilVandKloakTabel"}).find("tbody")
     substances=body.findAll("tr")
     
     
     for substance in substances: 
         name=substance.findNext("td").text.strip() 
         value=substance.findAll("td")[2].text.replace(",",".")
         measure=substance.findAll("td")[3].text.strip().lower()
         
         if "vand" in name.lower(): 
             value=substance.findAll("td")[2].text.replace(",",".")
             measure=substance.findAll("td")[3].text.strip().lower()
             #see if the value is 0 
             if value.strip()=='': 
                 value=0
             else: 
                 value=float(value)
             if measure.startswith("l"): 
                 value=value 
             elif measure.startswith("t"): 
                 value=value*1000 
             elif measure.startswith("m"): 
                 value=value*1000 
             elif measure.startswith("kg"):
                 value=value 
             water=water+value 



#add a row 
 water=water*med
 values_to_add={'company_name':company_name, 'cvr_firm':cvr_firm, 'p_number':p_number, 
                'year':year, 'water':water}
 row_to_add=pd.Series(values_to_add)
 df=df.append(row_to_add, ignore_index=True)
 print(num)
 

#save
writer=pd.ExcelWriter('GreenAccountsWater.xlsx')
df.to_excel(writer, index=False)
writer.save()
