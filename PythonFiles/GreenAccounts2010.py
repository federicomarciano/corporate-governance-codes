###############################################################################
#GreenAccounts.py 09/20/2021 ##################################################
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
                           'water_sew', 'strange_water_sew', 'strange_list', 'measure_list', 'problems'])


#dictionary for air emission toxicity weights
dict_air={"1,2,3,4,5,6-hexachlorcyclohexan(HCH)":0.0000366095266066354,
          "hch":0.0000366095266066354,
          
          "1,1,2,2-tetrachlorethan":2.24779603704264*0.000001,
          
          "1,1,1-trichlorethan":3.61446311352547*0.00000001,
    
    
          "1,2-dichlorethan (EDC)":1.07158410599566*0.000001,
     
          "Aldrin":0.0000206732426546541,
          
          "Anthracen":4.96240490662358*0.000001,
    
          "Andre flygtige organiske forbindelser end methan (NMVOC)":3.18259331840221*0.0000001,
          "Andre flygtige organiske forbindelser end meth":3.18259331840221*0.0000001,
          
          "Arsen og arsenforbindelser (som As)":0.422796038951198, 
          "As":0.422796038951198, 
          "Sum af tungmetallerne As":0.422796038951198,
          
          "Benzen":3.18259331840221*0.0000001,
          
          "Benzo(g,h,i)perylen":6.17414072683301*0.000001,
          
          "Bly og blyforbindelser (som Pb)":0.0040824456327159,
          "Bly":0.0040824456327159, 
          "Pb":0.0040824456327159,
          
          
          "Cadmium og cadmiumforbindelser (som Cd)":0.0678636211928546,
          "Cadmium":0.0678636211928546,
          "Cd":0.0678636211928546, 
          
          "Chlordan":0.000736958948216843,
          
          "Chrom og chromforbindelser (som Cr)":1.11467615385284, 
          "Cr":1.11467615385284, 
          "Chrom":1.11467615385284, 
          
          "DDT":0.0000755472734313923,
          
          "Dichlormethan (DCM)":9.90562818568672*0.0000001,
          
          "Di-(2-ethylhexyl)phthalat (DEHP)":1.04033663993061*0.000001,
          
          "Dieldrin":0.000469832578068859,
          
          "Endrin":0.0000495014078789241,
          
          "Ethylenoxid":5.42952090092797*0.0000001,
          
          "Heptachlor":7.69419438990404*0.000001, 
          
          "Hexachlorbenzen (HCB)":0.000540638436420577,
          
          "Kobber og kobberforbindelser (som Cu)":8.40947585956069*0.000001,
          "Cu":8.40947585956069*0.000001, 
          "Kobber":8.40947585956069*0.000001,
          "kobber":8.40947585956069*0.000001,
          "Kobber og kobberforbindelser":8.40947585956069*0.000001,
          
          "Kviksølv og kviksølvforbindelser (som Hg)":0.401902575137441, 
          "Kviksølv":0.401902575137441, 
          "Hg":0.401902575137441, 
          
          "Lindan":0.0000338310574898758,
          
          "Mirex":0.0137796546686404,
          
          "Naphthalen":3.10493492042204*0.00000001,
          
          "Nikkel og nikkelforbindelser (som Ni)":0.0043038411975314,
          "Nikkel":0.0043038411975314,
          
          "PCDD + PCDF (dioxiner + furaner) (som Teq)":75.0553379397237, 
          "Dioxiner og furaner":75.0553379397237,
          "Dioxin":75.0553379397237,
          
          "Pentachlorbenzen":0.000041184145170549,
          
          "Pentachlorphenol (PCP)":5.62121366524698*0.000001, 
          
          "Polychlorerede biphenyler (PCB)":0.000313265883480163,
          
          "Polycykliske aromatiske kulbrinter (PAH)":0.000853017256328671, 
          
          "Tetrachlorethylen (PER)":2.95513417841283*0.000001,
          
          "Tetrachlormethan (TCM)":0.0000334520732776412,
          
          "Toxaphen":0.00145083752359959,
          
          "Trichlorethylen":1.55110783379822*0.00000001,
          
          "Trichlormethan":2.55939714848341*0.000001,
          
          "Vinylchlorid":3.78973220840766*0.0000001,
          
          "Zink og zinkforbindelser (som Zn)":0.0038082097461898, 
          "Zink og zinkforbindelser (som ZN)":0.0038082097461898, 
          "Zink":0.0038082097461898,  
          "Zinkstøv":0.0038082097461898,  
          "Støv fra zinkgryde":0.0038082097461898,
          "Tungmetaller (hovedsagelig zink)":0.0038082097461898, 
          }



dict_water={"1,2,3,4,5,6-hexachlorcyclohexan(HCH)":0.0000192105853935005,
            "hch":0.0000192105853935005,
    
            "1,2-dichlorethan (EDC)":9.51389585137521*0.0000001,
            
            "Aldrin":0.0000851824336171736,
            
            "Anthracen":8.86806522137371*0.000001,
    
            "Arsen og arsenforbindelser (som As)":0.374925215174424, 
            "As":0.374925215174424,
            "Sum af tungmetallerne As":0.374925215174424,
            
            "Atrazin":6.5362190864358*0.00000001, 
            
            "Benzen":2.83139803726972*0.0000001,
            
            "Benzo(g,h,i)perylen":7.71268648726522*0.000001,
            
            "Bly og blyforbindelser (som Pb)":4.48051383847829*0.000001,
            "Bly":4.48051383847829*0.000001, 
            "Pb":4.48051383847829*0.000001,
            
            "Cadmium og cadmiumforbindelser (som Cd)":0.0490069084422254, 
            "Cd":0.0490069084422254,
            "Cadmium":0.0490069084422254,
            
            "Chlordan":0.000789887390778273, 
            
            "Chrom og chromforbindelser (som Cr)":1.11612168360908, 
            "Cr":1.11612168360908, 
            "Chrom":1.11612168360908, 
            
            "Chlorpyrifos":2.55848553766639*0.000001,
            
            "DDT":0.0000403810974358998, 
            
            "Dichlormethan (DCM)":8.72788669106573*0.0000001,
            
            "Di-(2-ethylhexyl)phthalat (DEHP)":6.70721969814947*0.000000001, 
            
            "Dieldrin":0.000590362020290507, 
            
            "Diuron":1.73399290088565*0.00000001, 
            
            "Endosulfan":5.11027812984048*0.0000001, 
            
            "Endrin":0.0000665529831129206,
            
            "Ethylbenzen":3.09026883913325*0.00000001,
            
            "Ethylenoxid":3.89388081541384*0.0000001,
            
            "Fluoranthen":4.39676771665795*0.000001,
            
            "Halogenerede organiske forbindelser (som AOX)":5.5958206770491*0.0000001,
            
            "Heptachlor":0.0000340223240056288,
            
            "Hexachlorbenzen (HCB)":0.000522210508198463,
            
            "Hexachlorbutadien (HCBD)":0.0000175245595280676, 
            
            "Kobber og kobberforbindelser (som Cu)":1.39349770499053*0.00000001, 
            "Cu":1.39349770499053*0.00000001, 
            "Kobber":1.39349770499053*0.00000001,
            "kobber":1.39349770499053*0.00000001,
            "Kobber og kobberforbindelser":1.39349770499053*0.00000001,
            
            "Kviksølv og kviksølvforbindelser (som Hg)":0.0149114724843161, 
            "Kviksølv":0.0149114724843161, 
            "Hg":0.0149114724843161, 
             
            "Lindan":0.0000187350021495162,
            
            "Mirex":0.00813827460835499,
            
            "Naphthalen":1.65649576922607*0.00000001,
            
            "Nikkel og nikkelforbindelser (som Ni)":0.00455669002477504,
            "Nikkel":0.00455669002477504,
            
            "PCDD + PCDF (dioxiner + furaner) (som Teq)":30.1350014076867, 
            "Dioxiner og furaner":30.1350014076867,
            "Dioxin":30.1350014076867,
            
            "Pentachlorbenzen":0.00003953078010687, 
            
            "Pentachlorphenol (PCP)":1.41683151524376*0.000001, 
            
            "Phenoler (som total C)":3.27810444781117*0.0000000001,
            "phenol":3.27810444781117*0.0000000001,
            "Phenol":3.27810444781117*0.0000000001,
            "Fenol":3.27810444781117*0.0000000001,
            
            "Polychlorerede biphenyler (PCB)":0.00029613434521411,
            
            "Polycykliske aromatiske kulbrinter (PAH)":0.000204708133260099,
            
            "Simazin":8.57169347484953*0.00000001,
            
            "Tetrachlorethylen (PER)":2.69497025955807*0.000001,
            
            "Tetrachlormethan (TCM)":0.0000305208964727396,
            
            "Toluen":1.42537978105269*0.000000001,
            "Toluol":1.42537978105269*0.000000001,
            "Toluene":1.42537978105269*0.000000001,
            
            "Toxaphen":0.00117606341398786,
            
            "Trichlorethylen":1.39314357791123*0.00000001,
            
            "Trichlormethan":2.23354715780281*0.000001,
            
            "Trifluralin":2.90338350754896*0.0000001,
            
            "Vinylchlorid":3.2293907914479*0.0000001, 
            
            "Xylener":1.06366534836094*0.000000001,
            "Xilen":1.06366534836094*0.000000001,
            
            "Zink og zinkforbindelser (som Zn)":0.00199172288350793, 
            "Zink og zinkforbindelser (som ZN)":0.00199172288350793, 
            "Zink":0.00199172288350793,  
            "Zinkstøv":0.00199172288350793, 
            "Støv fra zinkgryde":0.00199172288350793,  
            "Tungmetaller (hovedsagelig zink)":0.00199172288350793, 
            }
"""
dict_air={
          "Ammoniak (NH3)":7,     
          "Flour og uorganiske flourforbindelser":17,  
          "Ammoniak/ammonium":7,
          "Antimon+arsen+bly+krom+koblot+kobber+mangan+nikkel+vanadium":43000000, 
          "Sum (pb, cr, cu mv.)":43000000,
          "Sum af tungmetallerne As, Co, Cr, Cu, Mn, Ni, Pb, Sb, V":43000000, 
          "Sb, As, Pb, Cr,Co, Cu, Mn, Ni, V, Hg":43000000, 
          "As+Co+Cr+Cu+Mn+Ni+Pb+Sb+V+Sn": 43000000, 
          "Pb+Cr+Cu+Mn":43000000, 
          "Sb,As,Pb,Cr,Co,Cu,Mn,Ni,V,Hg":43000000,
          "Sb+As+Pb+Cr+Co+Cu+Mn+Ni+V":43000000,
          "Sum af tungmetallerne CD og Tl":6400000, 
          "Ammoniak/ammonium":7, 
          "Sum af tungmetallerne Cd og Ti":6400000, 
          "pb+Cr+Cu+Mn+Ni+As+Sb+Co+V":43000000, 
          "Sb+As+Pb+Cr+Cu+Mn+Ni+V":43000000, 
          "Cd+As+Ni+Cr":43000000, 
          "Sum (Cd, TI)":6400000, 
          "Cd+Tl":6400000, 
          "Sum (pb, cr, cu mv.)":43000000, 
          "Cadmium og thallium":6400000, 
          "Hg+Cd":6400000, 
          "Cd+Hg":6400000, 
          "As+Co+Cr+Cu+Mn+Ni+Pb+Sb+V+Sn":43000000, 
          "Antimon+arsen+bly+krom+koblot+kobber+mangan+nikkel+vanadium":43000000,
          "Pb+Cr+Cu+Mn":43000000, 
          "Sb+As+Pb+Cr+Co+Cu+Mn+Ni+V+Sn":43000000, 
          "Sb+As+Pb+Cr+Co+Cu+Mn+Ni+V":43000000, 
          "Ni+As":15000000, 
          "Sum af tungmetallerne As, Co, Cr, Cu, Mn, Ni, Pb, Sb, V":43000000, 
          "Sb, As, Pb, Cr,Co, Cu, Mn, Ni, V, Hg":43000000, 
          "pb+Cr+Cu+Mn+Ni+As+Sb+Co+V":43000000, 
          "Sum af tungmetallerne As, Co, Cr, Cu, Mn, Ni, Pb, Sb og V":43000000, 
          "Sb+As+Pb+Cr+Cu+Mn+Ni+V":43000000, 
          "Fluor og uorganiske fluorforbindelser (som HF":17, 
          "As+Co+Cr+Cu+Mn+Ni+Pb+Sb+V": 43000000, 
          "Nikkel+Arsen+Cadmium+Kviksølv+Bly+Chrom+Kobber+Mangan":43000000, 
          "Hydrogenflourid":17,
          "Cd,Ti":6400000, 
          "cd + Tl":6400000,
          "Pb+Cr+Cu+Mn+Ni+As+Sb+Co+V":43000000, 
          "Pb+Cr+Cu":43000000, 
          "Cd, Ti":6400000, 
          "Sølv+Crom+Kobber":43000000, 
          }



#dictionary for water emission toxicity weights
dict_water=  
            "Total fosfor":50000,    
            "Total fosfor  (tot-P)":50000, 
            "Total fosfor":50000,  
            "Total fosfor (tot-P)":50000, 


            "Total P":50000,
            "Antimon+arsen+bly+krom+koblot+kobber+mangan+nikkel+vanadium":1500000, 
            "Sum (pb, cr, cu mv.)": 500000,
            "Pb+Cr+Cu+Mn":500000, 
            "Sb,As,Pb,Cr,Co,Cu,Mn,Ni,V,Hg": 1500000, 
            "Sb+As+Pb+Cr+Co+Cu+Mn+Ni+V":1500000, 
            "Sum af tungmetallerne CD og Tl":2000, 
            "Sum af tungmetallerne Cd og Ti":2000, 
            "pb+Cr+Cu+Mn+Ni+As+Sb+Co+V":1500000, 
            "Sb+As+Pb+Cr+Cu+Mn+Ni+V":1500000, 
            "Cd+As+Ni+Cr":1500000, 
            "Sum (Cd, TI)":2000, 
            "Cd+Tl":2000, 
            "Sum (pb, cr, cu mv.)":500000, 
            "Cadmium og thallium":2000, 
            "Hg+Cd":10000, 
            "Cd+Hg":10000, 
            "As+Co+Cr+Cu+Mn+Ni+Pb+Sb+V+Sn":1500000, 
            "Antimon+arsen+bly+krom+koblot+kobber+mangan+nikkel+vanadium":1500000, 
            "Pb+Cr+Cu+Mn":500000, 
            "Sb+As+Pb+Cr+Co+Cu+Mn+Ni+V+Sn":1500000, 
            "Sb+As+Pb+Cr+Co+Cu+Mn+Ni+V":1500000, 
            "Ni+As":1500000, 
            "Sum af tungmetallerne As, Co, Cr, Cu, Mn, Ni, Pb, Sb, V":1500000, 
            "Sb, As, Pb, Cr,Co, Cu, Mn, Ni, V, Hg":1500000, 
            "pb+Cr+Cu+Mn+Ni+As+Sb+Co+V":1500000, 
            "Sum af tungmetallerne As, Co, Cr, Cu, Mn, Ni, Pb, Sb og V":1500000, 
            "Sb+As+Pb+Cr+Cu+Mn+Ni+V":1500000, 
            "As+Co+Cr+Cu+Mn+Ni+Pb+Sb+V":1500000, 
            "Nikkel+Arsen+Cadmium+Kviksølv+Bly+Chrom+Kobber+Mangan":1500000, 
            "Cd,Ti":2000, 
            "cd + Tl":2000, 
            "Pb+Cr+Cu+Mn+Ni+As+Sb+Co+V":1500000, 
            "Pb+Cr+Cu":500000, 
            "Cd, Ti":2000, 
            "Sølv+Crom+Kobber":500000, 
            }

"""
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
          "Kuldioid (CO2)":1, 
          "kuldioxid":1, 
          "Kuldioxid (CO2) ifm. Naturgas":1, 
          "Kuldioxid (CO2) ifm. El":1}


#dictionary m^3
dict_m3={
    "Kuldioxid (CO2)":680,
    "Co2":680, 
    "CO2":680, 
    "kuddioxid":680, 
    "Kuldioxid":680, 
    "Kuldioid (CO2)":680, 
    "kuldioxid":680}

#dictinary l
dict_l={"Kobber og kobberforbindelser":8940}





#####routine###################################################################




#initialiaze
base="https://dma.mst.dk/prtr/offentlig/produktionsenhed"
count=0

#loop 600
for i in range(23,660):
 print("page" + str(i))
 url=base+"?searchProductionUnitName=&searchAdressLine=&searchPostalDistrict=&authorityCode=&searchCHR=&action=search&searchCVR=&page="+str(i)+"&searchYear=&searchPNumber="
 html=requests.get(url).text 
 soup=bs(html,features='html.parser')
 rows=soup.find("tbody").findAll("tr")

 for row in rows: 
   count=count+1 
   print(str(count))
   company_name= row.find("td").find("a").text.strip()
   year=row.find("td").findNext("td").text.strip()
   cvr_firm=row.find("td").findNext("td").findNext("td").findNext("td").findNext("td").findNext("td").text.strip()
   p_number=row.find("td").findNext("td").findNext("td").findNext("td").findNext("td").findNext("td").findNext("td").text.strip()
   identifier = row.find("td").find("a")["href"]
   identifier = identifier.replace("/prtr/offentlig/produktionsenhed","")
   url1=base+identifier
   html=requests.get(url1).text 
   soup=bs(html,features='html.parser')

#emissions

   measure_list="" 
   strange_list=""
   problems=0
 
#air 
   if "Produktionsenheden har ikke oplyst, at den har udledninger til luft for det pågældende regnskabsår" in html: 
     air=0
     strange_air=0
     GHGs=0
   else:    
     air=0
     strange_air=0
     GHGs=0
     substances=soup.find("div", {"id":"pollutantAir"}).findNext("tbody").findAll("tr")
     
     for substance in substances: 
         name=substance.findNext("td").text.strip() 
         value=substance.findNext("td").findNext("td").findNext("td").text.strip()
         value=value.replace(".","")
         value=value.replace(",",".")
         measure=substance.findNext("td").findNext("td").findNext("td").findNext("td").findNext("td").text.strip().lower()
         
         
         
         #health hazardous pollutants 
         if name in list(dict_air.keys()):
             
             
             #see if the value is missing 
             if value.strip()=='': 
                 value=0
                 print('!!!!MISSING!!!!')
             else: 
                 value=float(value)
                 
             
             #see if the measure is kilograms 
             if measure.startswith("t"): 
                 value = value*1000             
             elif measure.startswith("k"): 
                 value = value            
             elif measure.startswith("m"): 
                 if name in list(dict_m3.keys()): 
                     position=list(dict_m3.keys()).index(name)
                     value=value*list(dict_m3.values())[position]
                 else: 
                     value=value 
                     print("problem")
                     problems=problems+1
             elif measure.startswith("l"): 
                 value=value
             else: 
                 value=value 
                 problems=problems+1
                 print("problem")
             
             #toxicity weight        
             position=list(dict_air.keys()).index(name)
             value=value*list(dict_air.values())[position]
             air=air + value 
             measure_list=measure_list+name+" ("+measure+")" + " !!! "
             
             
             
        #GHG 
         elif name in list(dict_GHG.keys()): 
             
             
             #see if the value is missing 
             if value.strip()=='': 
                 value=0
                 print('!!!!MISSING!!!!')
             else: 
                 value=float(value)
                 
                 
            #see if the measure is kilogram
             if measure.startswith("t"): 
                 value = value*1000             
             elif measure.startswith("k"): 
                 value = value            
             elif measure.startswith("m"): 
                 if name in list(dict_m3.keys()): 
                     position=list(dict_m3.keys()).index(name)
                     value=value*list(dict_m3.values())[position]
                 else: 
                     value=value 
                     print("problem")
                     problems=problems+1
             elif measure.startswith("l"): 
                 value=value
             else: 
                 value=value 
                 problems=problems+1   
                 print("problem")
                 
            #toxicity weight 
             position= list(dict_GHG.keys()).index(name)
             value=value*list(dict_GHG.values())[position]
             GHGs=GHGs + value 
             measure_list=measure_list+name+" ("+measure+")" + " !!! "
             
             
             
        #record the substance and its measure if it is not in the list of air or GHG 
         else: 
             strange_list=strange_list+name+" ("+measure+")" + " !!! "
             strange_air=strange_air+1
 



#water recipient 
   if "Produktionsenheden har ikke oplyst, at den har udledninger til vand - recipient for det pågældende regnskabsår" in html: 
     water_rec=0
     strange_water_rec=0
   else: 
     water_rec=0 
     strange_water_rec=0 
     substances=soup.find("div", {"id":"pollutantWaterRecipient"}).findNext("tbody").findAll("tr")
     
     for substance in substances: 
         name=substance.findNext("td").text.strip() 
         value=substance.findNext("td").findNext("td").findNext("td").text.strip()
         value=value.replace(".","")
         value=value.replace(",",".")
         measure=substance.findNext("td").findNext("td").findNext("td").findNext("td").findNext("td").text.strip().lower()
         
         
         if name in list(dict_water.keys()): 
             
             #see if the value is 0 
             if value.strip()=='': 
                 value=0
                 print('!!!!MISSING!!!!')
             else: 
                 value=float(value)

            
             #see if the measure is kilogram
             if measure.startswith("t"): 
                 value = value*1000             
             elif measure.startswith("k"): 
                 value = value            
             elif measure.startswith("m"): 
                 if name in list(dict_m3.keys()): 
                     position=list(dict_m3.keys()).index(name)
                     value=value*list(dict_m3.values())[position]
                 else: 
                     value=value 
                     print("problem")
                     problems=problems+1
             elif measure.startswith("l"): 
                 value=value
             else: 
                 value=value 
                 problems=problems+1  
                 print("problem")
               
                
             #toxicity weight 
             position= list(dict_water.keys()).index(name)
             value=value*list(dict_water.values())[position]
             water_rec=water_rec + value 
             measure_list=measure_list+name+" ("+measure+")" + " !!! "
             
             
         #record the substance if it is not in PRTR 
         else: 
             strange_list=strange_list+name+" ("+measure+")" + " !!! "
             strange_water_rec=strange_water_rec+1




#water sewer 
   if "Produktionsenheden har ikke oplyst, at den har udledninger til vand - kloak for det pågældende regnskabsår" in html: 
     water_sew=0
     strange_water_sew=0
   else: 
     water_sew=0 
     strange_water_sew=0 
     substances=soup.find("div", {"id":"pollutantWaterSewer"}).findNext("tbody").findAll("tr")
     
     for substance in substances: 
         name=substance.findNext("td").text.strip() 
         value=substance.findNext("td").findNext("td").findNext("td").text.strip()
         value=value.replace(".","")
         value=value.replace(",",".")
         measure=substance.findNext("td").findNext("td").findNext("td").findNext("td").findNext("td").text.strip().lower()
         
         
         #see if the value is missing 
         if name in list(dict_water.keys()): 
             if value.strip()=='': 
                 value=0
                 print('!!!!MISSING!!!!')
             else: 
                 value=float(value)
             
            #see if the measure is kilogram
             if measure.startswith("t"): 
                 value = value*1000             
             elif measure.startswith("k"): 
                 value = value            
             elif measure.startswith("m"): 
                 if name in list(dict_m3.keys()): 
                     position=list(dict_m3.keys()).index(name)
                     value=value*list(dict_m3.values())[position]
                 else: 
                     value=value 
                     print("problem")
                     problems=problems+1
             elif measure.startswith("l"): 
                 value=value
             else: 
                 value=value 
                 problems=problems+1  
                 print("problem")   
                 
             #toxicity weight 
             position= list(dict_water.keys()).index(name)
             value=value*list(dict_water.values())[position]
             water_sew=water_sew + value 
             measure_list=measure_list+name+" ("+measure+")" + " !!! "
             
        #record the strange substance 
         else: 
             strange_list=strange_list+name+" ("+measure+")" + " !!! "
             strange_water_sew=strange_water_sew+1



#add a row 
   values_to_add={'company_name':company_name, 'cvr_firm':cvr_firm, 'p_number':p_number, 
                'year':year, 'air':air, 'GHGs':GHGs, 'strange_air':strange_air,
                'water_rec':water_rec, 'strange_water_rec':strange_water_rec, 
                'water_sew':water_sew, 'strange_water_sew':strange_water_sew, 
                'strange_list':strange_list, 'measure_list':measure_list, 'problems':problems}
   row_to_add=pd.Series(values_to_add)
   df=df.append(row_to_add, ignore_index=True)
 
 

#save
writer=pd.ExcelWriter('GreenAccounts2010_NewFormat.xlsx')
df.to_excel(writer, index=False)
writer.save()