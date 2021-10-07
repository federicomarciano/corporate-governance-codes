###############################################################################
#GreenAccounts.py 10/06/2021 ##################################################
###############################################################################
#
# AIM: collect RAW data on green accounts from EPA's website; in this code I do
# not perform the weighting, but I just collect data on emissions. 
#
# 
#
###############################################################################








#####PRELIMINARIES#############################################################


#libraries
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd


#define the dataset
df = pd.DataFrame(columns=['company_name', 'cvr_firm', 'p_number','year',
                           
                           'hexachlorcyclohexan_air',
                           'tetrachlorethan_air',
                           'trichlorethan_air', 
                           'dichlorethan_air', 
                           'aladrin_air',
                           'anthracen_air', 
                           'nmovoc_air',
                           'arsenic_air', 
                           'benzen_air', 
                           'benzo_air', 
                           'bly_air', 
                           'cadmium_air', 
                           'chlordan_air', 
                           'chrom_air', 
                           'ddt_air', 
                           'dichlormethan_air', 
                           'diphthalat_air', 
                           'dieldrin_air', 
                           'endrin_air', 
                           'ethylenoxid_air', 
                           'heptachlor_air', 
                           'hexachlorbenzen_air', 
                           'kobber_air', 
                           'mercury_air', 
                           'lindan_air', 
                           'mirex_air', 
                           'naphthalen_air', 
                           'nikkel_air',
                           'dioxin_air', 
                           'pentachlorbenzen_air',
                           'pentachlorphenol_air',
                           'polychlorerede_air', 
                           'polycykliske_air', 
                           'tetrachlorethylen_air', 
                           'tetrachlormethan_air', 
                           'toxaphen_air', 
                           'trichlorethylen_air', 
                           'trichlormethan_air', 
                           'vinylchlorid_air', 
                           'zink_air', 
                           
                           'hexachlorcyclohexan_water_receiver',
                           'dichlorethan_water_receiver',
                           'aldrin_water_receiver',
                           'anthracen_water_receiver',
                           'arsen_water_receiver',
                           'atrazin_water_receiver',
                           'benzen_water_receiver',
                           'benzo_water_receiver',
                           'bly_water_receiver',
                           'cadmium_water_receiver',
                           'chlordan_water_receiver',
                           'chrom_water_receiver',
                           'chlorpyrifos_water_receiver',
                           'ddt_water_receiver',
                           'dichlormethan_water_receiver',
                           'diphthalat_water_receiver',
                           'dieldrin_water_receiver',
                           'diuron_water_receiver',
                           'endosulfan_water_receiver',
                           'endrin_water_receiver',
                           'ethylbenzen_water_receiver',
                           'ethylenoxid_water_receiver',
                           'fluoranthen_water_receiver',
                           'halogenerede_water_receiver',
                           'heptachlor_water_receiver',
                           'hexachlorbenzen_water_receiver',
                           'hexachlorbutadien_water_receiver',
                           'kobber_water_receiver',
                           'mercury_water_receiver',
                           'lindan_water_receiver',
                           'mirex_water_receiver',
                           'naphthalen_water_receiver',
                           'nikkel_water_receiver',
                           'dioxin_water_receiver',
                           'pentachlorbenzen_water_receiver',
                           'pentachlorphenol_water_receiver',
                           'phenoler_water_receiver',
                           'polychlorerede_water_receiver',
                           'polycykliske_water_receiver',
                           'simazin_water_receiver',
                           'tetrachlorethylen_water_receiver',
                           'tetrachlormethan_water_receiver',
                           'toluen_water_receiver',
                           'toxaphen_water_receiver',
                           'trichlorethylen_water_receiver',
                           'trichlormethan_water_receiver',
                           'trifluralin_water_receiver',
                           'vinylchlorid_water_receiver',
                           'xylener_water_receiver',
                           'zynk_water_receiver',
                           
                           'hexachlorcyclohexan_water_sewer',
                           'dichlorethan_water_sewer',
                           'aldrin_water_sewer',
                           'anthracen_water_sewer',
                           'arsen_water_sewer',
                           'atrazin_water_sewer',
                           'benzen_water_sewer',
                           'benzo_water_sewer',
                           'bly_water_sewer',
                           'cadmium_water_sewer',
                           'chlordan_water_sewer',
                           'chrom_water_sewer',
                           'chlorpyrifos_water_sewer',
                           'ddt_water_sewer',
                           'dichlormethan_water_sewer',
                           'diphthalat_water_sewer',
                           'dieldrin_water_sewer',
                           'diuron_water_sewer',
                           'endosulfan_water_sewer',
                           'endrin_water_sewer',
                           'ethylbenzen_water_sewer',
                           'ethylenoxid_water_sewer',
                           'fluoranthen_water_sewer',
                           'halogenerede_water_sewer',
                           'heptachlor_water_sewer',
                           'hexachlorbenzen_water_sewer',
                           'hexachlorbutadien_water_sewer',
                           'kobber_water_sewer',
                           'mercury_water_sewer',
                           'lindan_water_sewer',
                           'mirex_water_sewer',
                           'naphthalen_water_sewer',
                           'nikkel_water_sewer',
                           'dioxin_water_sewer',
                           'pentachlorbenzen_water_sewer',
                           'pentachlorphenol_water_sewer',
                           'phenoler_water_sewer',
                           'polychlorerede_water_sewer',
                           'polycykliske_water_sewer',
                           'simazin_water_sewer',
                           'tetrachlorethylen_water_sewer',
                           'tetrachlormethan_water_sewer',
                           'toluen_water_sewer',
                           'toxaphen_water_sewer',
                           'trichlorethylen_water_sewer',
                           'trichlormethan_water_sewer',
                           'trifluralin_water_sewer',
                           'vinylchlorid_water_sewer',
                           'xylener_water_sewer',
                           'zynk_water_sewer',                           
                           
                           'carbon_ghg', 
                           'chlorfluorcarboner_ghg', 
                           "dinitrogenoxid_ghg", 
                           "hydrochlorfluorcarboner_ghg",
                           "hydrofluorcarboner_ghg",
                           "metan_ghg",
                           "perfluorcarboner_ghg",
                           "svovlhexafluorid_ghg" 

                           
                           ])

#lists for the variables:
list_air=[                 'hexachlorcyclohexan_air',
                           'tetrachlorethan_air',
                           'trichlorethan_air', 
                           'dichlorethan_air', 
                           'aladrin_air',
                           'anthracen_air', 
                           'nmovoc_air',
                           'arsenic_air', 
                           'benzen_air', 
                           'benzo_air', 
                           'bly_air', 
                           'cadmium_air', 
                           'chlordan_air', 
                           'chrom_air', 
                           'ddt_air', 
                           'dichlormethan_air', 
                           'diphthalat_air', 
                           'dieldrin_air', 
                           'endrin_air', 
                           'ethylenoxid_air', 
                           'heptachlor_air', 
                           'hexachlorbenzen_air', 
                           'kobber_air', 
                           'mercury_air', 
                           'lindan_air', 
                           'mirex_air', 
                           'naphthalen_air', 
                           'nikkel_air',
                           'dioxin_air', 
                           'pentachlorbenzen_air',
                           'pentachlorphenol_air',
                           'polychlorerede_air', 
                           'polycykliske_air', 
                           'tetrachlorethylen_air', 
                           'tetrachlormethan_air', 
                           'toxaphen_air', 
                           'trichlorethylen_air', 
                           'trichlormethan_air', 
                           'vinylchlorid_air', 
                           'zink_air' ]

list_water_receiver=[      'hexachlorcyclohexan_water_receiver',
                           'dichlorethan_water_receiver',
                           'aldrin_water_receiver',
                           'anthracen_water_receiver',
                           'arsen_water_receiver',
                           'atrazin_water_receiver',
                           'benzen_water_receiver',
                           'benzo_water_receiver',
                           'bly_water_receiver',
                           'cadmium_water_receiver',
                           'chlordan_water_receiver',
                           'chrom_water_receiver',
                           'chlorpyrifos_water_receiver',
                           'ddt_water_receiver',
                           'dichlormethan_water_receiver',
                           'diphthalat_water_receiver',
                           'dieldrin_water_receiver',
                           'diuron_water_receiver',
                           'endosulfan_water_receiver',
                           'endrin_water_receiver',
                           'ethylbenzen_water_receiver',
                           'ethylenoxid_water_receiver',
                           'fluoranthen_water_receiver',
                           'halogenerede_water_receiver',
                           'heptachlor_water_receiver',
                           'hexachlorbenzen_water_receiver',
                           'hexachlorbutadien_water_receiver',
                           'kobber_water_receiver',
                           'mercury_water_receiver',
                           'lindan_water_receiver',
                           'mirex_water_receiver',
                           'naphthalen_water_receiver',
                           'nikkel_water_receiver',
                           'dioxin_water_receiver',
                           'pentachlorbenzen_water_receiver',
                           'pentachlorphenol_water_receiver',
                           'phenoler_water_receiver',
                           'polychlorerede_water_receiver',
                           'polycykliske_water_receiver',
                           'simazin_water_receiver',
                           'tetrachlorethylen_water_receiver',
                           'tetrachlormethan_water_receiver',
                           'toluen_water_receiver',
                           'toxaphen_water_receiver',
                           'trichlorethylen_water_receiver',
                           'trichlormethan_water_receiver',
                           'trifluralin_water_receiver',
                           'vinylchlorid_water_receiver',
                           'xylener_water_receiver',
                           'zynk_water_receiver']

list_water_sewer=[         'hexachlorcyclohexan_water_sewer',
                           'dichlorethan_water_sewer',
                           'aldrin_water_sewer',
                           'anthracen_water_sewer',
                           'arsen_water_sewer',
                           'atrazin_water_sewer',
                           'benzen_water_sewer',
                           'benzo_water_sewer',
                           'bly_water_sewer',
                           'cadmium_water_sewer',
                           'chlordan_water_sewer',
                           'chrom_water_sewer',
                           'chlorpyrifos_water_sewer',
                           'ddt_water_sewer',
                           'dichlormethan_water_sewer',
                           'diphthalat_water_sewer',
                           'dieldrin_water_sewer',
                           'diuron_water_sewer',
                           'endosulfan_water_sewer',
                           'endrin_water_sewer',
                           'ethylbenzen_water_sewer',
                           'ethylenoxid_water_sewer',
                           'fluoranthen_water_sewer',
                           'halogenerede_water_sewer',
                           'heptachlor_water_sewer',
                           'hexachlorbenzen_water_sewer',
                           'hexachlorbutadien_water_sewer',
                           'kobber_water_sewer',
                           'mercury_water_sewer',
                           'lindan_water_sewer',
                           'mirex_water_sewer',
                           'naphthalen_water_sewer',
                           'nikkel_water_sewer',
                           'dioxin_water_sewer',
                           'pentachlorbenzen_water_sewer',
                           'pentachlorphenol_water_sewer',
                           'phenoler_water_sewer',
                           'polychlorerede_water_sewer',
                           'polycykliske_water_sewer',
                           'simazin_water_sewer',
                           'tetrachlorethylen_water_sewer',
                           'tetrachlormethan_water_sewer',
                           'toluen_water_sewer',
                           'toxaphen_water_sewer',
                           'trichlorethylen_water_sewer',
                           'trichlormethan_water_sewer',
                           'trifluralin_water_sewer',
                           'vinylchlorid_water_sewer',
                           'xylener_water_sewer',
                           'zynk_water_sewer'] 

list_GHG=[                 'carbon_ghg', 
                           'chlorfluorcarboner_ghg', 
                           "dinitrogenoxid_ghg", 
                           "hydrochlorfluorcarboner_ghg",
                           "hydrofluorcarboner_ghg",
                           "metan_ghg",
                           "perfluorcarboner_ghg",
                           "svovlhexafluorid_ghg" ]        
    
#lists for the names of substances for air emission toxicity weights
list_air_name=["1,2,3,4,5,6-hexachlorcyclohexan(HCH)",
          
          "1,1,2,2-tetrachlorethan",
          
          "1,1,1-trichlorethan",
    
    
          "1,2-dichlorethan (EDC)",
     
          "Aldrin",
          
          "Anthracen",
    
          "Andre flygtige organiske forbindelser end methan (NMVOC)",
          
          "Arsen og arsenforbindelser (som As)", 
          
          "Benzen",
          
          "Benzo(g,h,i)perylen",
          
          "Bly og blyforbindelser (som Pb)",       
          
          "Cadmium og cadmiumforbindelser (som Cd)",
          
          "Chlordan",
          
          "Chrom og chromforbindelser (som Cr)" ,
          
          "DDT",
          
          "Dichlormethan (DCM)",
          
          "Di-(2-ethylhexyl)phthalat (DEHP)",
          
          "Dieldrin",
          
          "Endrin",
          
          "Ethylenoxid",
          
          "Heptachlor", 
          
          "Hexachlorbenzen (HCB)",
          
          "Kobber og kobberforbindelser (som Cu)",
          
          "Kviksølv og kviksølvforbindelser (som Hg)", 
          
          "Lindan",
          
          "Mirex",
          
          "Naphthalen",
          
          "Nikkel og nikkelforbindelser (som Ni)",
          
          "PCDD + PCDF (dioxiner + furaner) (som Teq)",
          
          "Pentachlorbenzen",
          
          "Pentachlorphenol (PCP)", 
          
          "Polychlorerede biphenyler (PCB)",
          
          "Polycykliske aromatiske kulbrinter (PAH)", 
          
          "Tetrachlorethylen (PER)",
          
          "Tetrachlormethan (TCM)",
          
          "Toxaphen",
          
          "Trichlorethylen",
          
          "Trichlormethan",
          
          "Vinylchlorid",
          
          "Zink og zonkforbindelser (som ZN)"]



list_water_name=["1,2,3,4,5,6-hexachlorcyclohexan(HCH)",
    
            "1,2-dichlorethan (EDC)",
            
            "Aldrin",
            
            "Anthracen",
    
            "Arsen og arsenforbindelser (som As)",
            
            "Atrazin", 
            
            "Benzen",
            
            "Benzo(g,h,i)perylen",
            
            "Bly og blyforbindelser (som Pb)",
            
            "Cadmium og cadmiumforbindelser (som Cd)",
            
            "Chlordan", 
            
            "Chrom og chromforbindelser (som Cr)", 
            
            "Chlorpyrifos",
            
            "DDT", 
            
            "Dichlormethan (DCM)",
            
            "Di-(2-ethylhexyl)phthalat (DEHP)", 
            
            "Dieldrin", 
            
            "Diuron", 
            
            "Endosulfan", 
            
            "Endrin",
            
            "Ethylbenzen",
            
            "Ethylenoxid",
            
            "Fluoranthen",
            
            "Halogenerede organiske forbindelser (som AOX)",
            
            "Heptachlor",
            
            "Hexachlorbenzen (HCB)",
            
            "Hexachlorbutadien (HCBD)", 
            
            "Kobber og kobberforbindelser (som Cu)",
            
            "Kviksølv og kviksølvforbindelser (som Hg)",
             
            "Lindan",
            
            "Mirex",
            
            "Naphthalen",
            
            "Nikkel og nikkelforbindelser (som Ni)",
            
            "PCDD + PCDF (dioxiner + furaner) (som Teq)",
            
            "Pentachlorbenzen", 
            
            "Pentachlorphenol (PCP)", 
            
            "Phenoler (som total C)",
            
            "Polychlorerede biphenyler (PCB)",
            
            "Polycykliske aromatiske kulbrinter (PAH)",
            
            "Simazin",
            
            "Tetrachlorethylen (PER)",
            
            "Tetrachlormethan (TCM)",
            
            "Toluen",
            
            "Toxaphen",
            
            "Trichlorethylen",
            
            "Trichlormethan",
            
            "Trifluralin",
            
            "Vinylchlorid", 
            
            "Xylener",
            
            "Zink og zonkforbindelser (som ZN)"
            ]

#dictionary for GHG 
list_GHG_name=["Kuldioxid (CO2)",
    
          "Chlorfluorcarboner (CFC)",
               
          "Dinitrogenoxid (N2O)", 
          
          "Hydrochlorfluorcarboner (HCFC)",
          
          "Hydrofluorcarboner (HFC)",
          
          "Methan (CH4)",
          
          "Perfluorcarboner (PFC)",
          
          "Svovlhexafluorid (SF6)"]




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

list_check=[]




#####routine###################################################################




#initialiaze
base="https://dma.mst.dk/prtr/offentlig/produktionsenhed"
count=0

#loop 23 660
for i in range(23,661):
 print("page" + str(i))
 url=base+"?searchProductionUnitName=&searchAdressLine=&searchPostalDistrict=&authorityCode=&searchCHR=&action=search&searchCVR=&page="+str(i)+"&searchYear=&searchPNumber="
 html=requests.get(url).text 
 soup=bs(html,features='html.parser')
 rows=soup.find("tbody").findAll("tr")

 for row in rows: 
   try:
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
    hexachlorcyclohexan_air=0
    tetrachlorethan_air=0
    trichlorethan_air=0
    dichlorethan_air=0
    aladrin_air=0
    anthracen_air=0
    nmovoc_air=0
    arsenic_air=0
    benzen_air=0
    benzo_air=0
    bly_air=0
    cadmium_air=0
    chlordan_air=0
    chrom_air=0
    ddt_air=0
    dichlormethan_air=0
    diphthalat_air=0
    dieldrin_air=0
    endrin_air=0
    ethylenoxid_air=0
    heptachlor_air=0
    hexachlorbenzen_air=0
    kobber_air=0
    mercury_air=0
    lindan_air=0
    mirex_air=0
    naphthalen_air=0
    nikkel_air=0
    dioxin_air=0
    pentachlorbenzen_air=0
    pentachlorphenol_air=0
    polychlorerede_air=0
    polycykliske_air=0
    tetrachlorethylen_air=0
    tetrachlormethan_air=0
    toxaphen_air=0
    trichlorethylen_air=0
    trichlormethan_air=0
    vinylchlorid_air=0
    zink_air=0
    hexachlorcyclohexan_water_receiver=0
    dichlorethan_water_receiver=0
    aldrin_water_receiver=0
    anthracen_water_receiver=0
    arsen_water_receiver=0
    atrazin_water_receiver=0
    benzen_water_receiver=0
    benzo_water_receiver=0
    bly_water_receiver=0
    cadmium_water_receiver=0
    chlordan_water_receiver=0
    chrom_water_receiver=0
    chlorpyrifos_water_receiver=0
    ddt_water_receiver=0
    dichlormethan_water_receiver=0
    diphthalat_water_receiver=0
    dieldrin_water_receiver=0
    diuron_water_receiver=0
    endosulfan_water_receiver=0
    endrin_water_receiver=0
    ethylbenzen_water_receiver=0
    ethylenoxid_water_receiver=0
    fluoranthen_water_receiver=0
    halogenerede_water_receiver=0
    heptachlor_water_receiver=0
    hexachlorbenzen_water_receiver=0
    hexachlorbutadien_water_receiver=0
    kobber_water_receiver=0
    mercury_water_receiver=0
    lindan_water_receiver=0
    mirex_water_receiver=0
    naphthalen_water_receiver=0
    nikkel_water_receiver=0
    dioxin_water_receiver=0
    pentachlorbenzen_water_receiver=0
    pentachlorphenol_water_receiver=0
    phenoler_water_receiver=0
    polychlorerede_water_receiver=0
    polycykliske_water_receiver=0
    simazin_water_receiver=0
    tetrachlorethylen_water_receiver=0
    tetrachlormethan_water_receiver=0
    toluen_water_receiver=0
    toxaphen_water_receiver=0
    trichlorethylen_water_receiver=0
    trichlormethan_water_receiver=0
    trifluralin_water_receiver=0
    vinylchlorid_water_receiver=0
    xylener_water_receiver=0
    zynk_water_receiver=0
    hexachlorcyclohexan_water_sewer=0
    dichlorethan_water_sewer=0
    aldrin_water_sewer=0
    anthracen_water_sewer=0
    arsen_water_sewer=0
    atrazin_water_sewer=0
    benzen_water_sewer=0
    benzo_water_sewer=0
    bly_water_sewer=0
    cadmium_water_sewer=0
    chlordan_water_sewer=0
    chrom_water_sewer=0
    chlorpyrifos_water_sewer=0
    ddt_water_sewer=0
    dichlormethan_water_sewer=0
    diphthalat_water_sewer=0
    dieldrin_water_sewer=0
    diuron_water_sewer=0
    endosulfan_water_sewer=0
    endrin_water_sewer=0
    ethylbenzen_water_sewer=0
    ethylenoxid_water_sewer=0
    fluoranthen_water_sewer=0
    halogenerede_water_sewer=0
    heptachlor_water_sewer=0
    hexachlorbenzen_water_sewer=0
    hexachlorbutadien_water_sewer=0
    kobber_water_sewer=0
    mercury_water_sewer=0
    lindan_water_sewer=0
    mirex_water_sewer=0
    naphthalen_water_sewer=0
    nikkel_water_sewer=0
    dioxin_water_sewer=0
    pentachlorbenzen_water_sewer=0
    pentachlorphenol_water_sewer=0
    phenoler_water_sewer=0
    polychlorerede_water_sewer=0
    polycykliske_water_sewer=0
    simazin_water_sewer=0
    tetrachlorethylen_water_sewer=0
    tetrachlormethan_water_sewer=0
    toluen_water_sewer=0
    toxaphen_water_sewer=0
    trichlorethylen_water_sewer=0
    trichlormethan_water_sewer=0
    trifluralin_water_sewer=0
    vinylchlorid_water_sewer=0
    xylener_water_sewer=0
    zynk_water_sewer=0
    carbon_ghg=0
    chlorfluorcarboner_ghg=0
    dinitrogenoxid_ghg=0
    hydrochlorfluorcarboner_ghg=0
    hydrofluorcarboner_ghg=0
    metan_ghg=0
    perfluorcarboner_ghg=0
    svovlhexafluorid_ghg=0  
    measure_list="" 
    strange_list=""
    problems=0
    strange_air=0 
    strange_water_rec=0
    strange_water_sew=0

 
#air 
    if "Produktionsenheden har ikke oplyst, at den har udledninger til luft for det pågældende regnskabsår" not in html:     
     substances=soup.find("div", {"id":"pollutantAir"}).findNext("tbody").findAll("tr")
     
     for substance in substances: 
         name=substance.findNext("td").text.strip() 
         value=substance.findNext("td").findNext("td").findNext("td").text.strip()
         value=value.replace(".","")
         value=value.replace(",",".")
         measure=substance.findNext("td").findNext("td").findNext("td").findNext("td").findNext("td").text.strip().lower()
         
         
         
         #health hazardous pollutants 
         if name in list_air_name:
             
             
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
             
             #record the value          
             measure_list=measure_list+name+" ("+measure+")" + " !!! "
             position=list_air_name.index(name)
             exec(list_air[position]+"=value")
             
             
             
        #GHG 
         elif name in list_GHG_name: 
             
             
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
             position=list_GHG_name.index(name)
             exec(list_GHG[position]+"=value")
             measure_list=measure_list+name+" ("+measure+")" + " !!! "
             
             
             
        #record the substance and its measure if it is not in the list of air or GHG 
         else: 
             strange_list=strange_list+name+" ("+measure+")" + " !!! "
             strange_air=strange_air+1
 



#water recipient 
    if "Produktionsenheden har ikke oplyst, at den har udledninger til vand - recipient for det pågældende regnskabsår" not in html: 
        
     substances=soup.find("div", {"id":"pollutantWaterRecipient"}).findNext("tbody").findAll("tr")
     
     for substance in substances: 
         name=substance.findNext("td").text.strip() 
         value=substance.findNext("td").findNext("td").findNext("td").text.strip()
         value=value.replace(".","")
         value=value.replace(",",".")
         measure=substance.findNext("td").findNext("td").findNext("td").findNext("td").findNext("td").text.strip().lower()
         
         
         if name in list_water_name: 
             
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
               
                
             #record the value  
             position= list_water_name.index(name)
             exec(list_water_receiver[position]+"=value")
             measure_list=measure_list+name+" ("+measure+")" + " !!! "
             
             
         #record the substance if it is not in PRTR 
         else: 
             strange_list=strange_list+name+" ("+measure+")" + " !!! "
             strange_water_rec=strange_water_rec+1




#water sewer 
    if "Produktionsenheden har ikke oplyst, at den har udledninger til vand - kloak for det pågældende regnskabsår" not in html: 
     substances=soup.find("div", {"id":"pollutantWaterSewer"}).findNext("tbody").findAll("tr")
     
     for substance in substances: 
         name=substance.findNext("td").text.strip() 
         value=substance.findNext("td").findNext("td").findNext("td").text.strip()
         value=value.replace(".","")
         value=value.replace(",",".")
         measure=substance.findNext("td").findNext("td").findNext("td").findNext("td").findNext("td").text.strip().lower()
         
         
         #see if the value is missing 
         if name in list_water_name: 
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
                 
             #record the value  
             position= list_water_name.index(name)
             exec(list_water_sewer[position]+"=value")
             measure_list=measure_list+name+" ("+measure+")" + " !!! "
             
        #record the strange substance 
         else: 
             strange_list=strange_list+name+" ("+measure+")" + " !!! "
             strange_water_sew=strange_water_sew+1



#add a row 
    values_to_add={'company_name':company_name, 'cvr_firm':cvr_firm, 'p_number':p_number, 
               'year':year, 
               'hexachlorcyclohexan_air':hexachlorcyclohexan_air,
               'tetrachlorethan_air':tetrachlorethan_air,
               'trichlorethan_air':trichlorethan_air,
               'dichlorethan_air':dichlorethan_air,
               'aladrin_air':aladrin_air,
               'anthracen_air':anthracen_air,
               'nmovoc_air':nmovoc_air,
               'arsenic_air':arsenic_air,
               'benzen_air':benzen_air,
               'benzo_air':benzo_air,
               'bly_air':bly_air,
               'cadmium_air':cadmium_air,
               'chlordan_air':chlordan_air,
               'chrom_air':chrom_air,
               'ddt_air':ddt_air,
               'dichlormethan_air':dichlormethan_air,
               'diphthalat_air':diphthalat_air,
               'dieldrin_air':dieldrin_air,
               'endrin_air':endrin_air,
               'ethylenoxid_air':ethylenoxid_air,
               'heptachlor_air':heptachlor_air,
               'hexachlorbenzen_air':hexachlorbenzen_air,
               'kobber_air':kobber_air,
               'mercury_air':mercury_air,
               'lindan_air':lindan_air,
               'mirex_air':mirex_air,
               'naphthalen_air':naphthalen_air,
               'nikkel_air':nikkel_air,
               'dioxin_air':dioxin_air,
               'pentachlorbenzen_air':pentachlorbenzen_air,
               'pentachlorphenol_air':pentachlorphenol_air,
               'polychlorerede_air':polychlorerede_air,
               'polycykliske_air':polycykliske_air,
               'tetrachlorethylen_air':tetrachlorethylen_air,
               'tetrachlormethan_air':tetrachlormethan_air,
               'toxaphen_air':toxaphen_air,
               'trichlorethylen_air':trichlorethylen_air,
               'trichlormethan_air':trichlormethan_air,
               'vinylchlorid_air':vinylchlorid_air,
               'zink_air':zink_air,
               
               'hexachlorcyclohexan_water_receiver':hexachlorcyclohexan_water_receiver,
               'dichlorethan_water_receiver':dichlorethan_water_receiver,
               'aldrin_water_receiver':aldrin_water_receiver,
               'anthracen_water_receiver':anthracen_water_receiver,
               'arsen_water_receiver':arsen_water_receiver,
               'atrazin_water_receiver':atrazin_water_receiver,
               'benzen_water_receiver':benzen_water_receiver,
               'benzo_water_receiver':benzo_water_receiver,
               'bly_water_receiver':bly_water_receiver,
               'cadmium_water_receiver':cadmium_water_receiver,
               'chlordan_water_receiver':chlordan_water_receiver,
               'chrom_water_receiver':chrom_water_receiver,
               'chlorpyrifos_water_receiver':chlorpyrifos_water_receiver,
               'ddt_water_receiver':ddt_water_receiver,
               'dichlormethan_water_receiver':dichlormethan_water_receiver,
               'diphthalat_water_receiver':diphthalat_water_receiver,
               'dieldrin_water_receiver':dieldrin_water_receiver,
               'diuron_water_receiver':diuron_water_receiver,
               'endosulfan_water_receiver':endosulfan_water_receiver,
               'endrin_water_receiver':endrin_water_receiver,
               'ethylbenzen_water_receiver':ethylbenzen_water_receiver,
               'ethylenoxid_water_receiver':ethylenoxid_water_receiver,
               'fluoranthen_water_receiver':fluoranthen_water_receiver,
               'halogenerede_water_receiver':halogenerede_water_receiver,
               'heptachlor_water_receiver':heptachlor_water_receiver,
               'hexachlorbenzen_water_receiver':hexachlorbenzen_water_receiver,
               'hexachlorbutadien_water_receiver':hexachlorbutadien_water_receiver,
               'kobber_water_receiver':kobber_water_receiver,
               'mercury_water_receiver':mercury_water_receiver,
               'lindan_water_receiver':lindan_water_receiver,
               'mirex_water_receiver':mirex_water_receiver,
               'naphthalen_water_receiver':naphthalen_water_receiver,
               'nikkel_water_receiver':nikkel_water_receiver,
               'dioxin_water_receiver':dioxin_water_receiver,
               'pentachlorbenzen_water_receiver':pentachlorbenzen_water_receiver,
               'pentachlorphenol_water_receiver':pentachlorphenol_water_receiver,
               'phenoler_water_receiver':phenoler_water_receiver,
               'polychlorerede_water_receiver':polychlorerede_water_receiver,
               'polycykliske_water_receiver':polycykliske_water_receiver,
               'simazin_water_receiver':simazin_water_receiver,
               'tetrachlorethylen_water_receiver':tetrachlorethylen_water_receiver,
               'tetrachlormethan_water_receiver':tetrachlormethan_water_receiver,
               'toluen_water_receiver':toluen_water_receiver,
               'toxaphen_water_receiver':toxaphen_water_receiver,
               'trichlorethylen_water_receiver':trichlorethylen_water_receiver,
               'trichlormethan_water_receiver':trichlormethan_water_receiver,
               'trifluralin_water_receiver':trifluralin_water_receiver,
               'vinylchlorid_water_receiver':vinylchlorid_water_receiver,
               'xylener_water_receiver':xylener_water_receiver,
               'zynk_water_receiver':zynk_water_receiver,  
               
               'hexachlorcyclohexan_water_sewer':hexachlorcyclohexan_water_sewer,
               'dichlorethan_water_sewer':dichlorethan_water_sewer,
               'aldrin_water_sewer':aldrin_water_sewer,
               'anthracen_water_sewer':anthracen_water_sewer,
               'arsen_water_sewer':arsen_water_sewer,
               'atrazin_water_sewer':atrazin_water_sewer,
               'benzen_water_sewer':benzen_water_sewer,
               'benzo_water_sewer':benzo_water_sewer,
               'bly_water_sewer':bly_water_sewer,
               'cadmium_water_sewer':cadmium_water_sewer,
               'chlordan_water_sewer':chlordan_water_sewer,
               'chrom_water_sewer':chrom_water_sewer,
               'chlorpyrifos_water_sewer':chlorpyrifos_water_sewer,
               'ddt_water_sewer':ddt_water_sewer,
               'dichlormethan_water_sewer':dichlormethan_water_sewer,
               'diphthalat_water_sewer':diphthalat_water_sewer,
               'dieldrin_water_sewer':dieldrin_water_sewer,
               'diuron_water_sewer':diuron_water_sewer,
               'endosulfan_water_sewer':endosulfan_water_sewer,
               'endrin_water_sewer':endrin_water_sewer,
               'ethylbenzen_water_sewer':ethylbenzen_water_sewer,
               'ethylenoxid_water_sewer':ethylenoxid_water_sewer,
               'fluoranthen_water_sewer':fluoranthen_water_sewer,
               'halogenerede_water_sewer':halogenerede_water_sewer,
               'heptachlor_water_sewer':heptachlor_water_sewer,
               'hexachlorbenzen_water_sewer':hexachlorbenzen_water_sewer,
               'hexachlorbutadien_water_sewer':hexachlorbutadien_water_sewer,
               'kobber_water_sewer':kobber_water_sewer,
               'mercury_water_sewer':mercury_water_sewer,
               'lindan_water_sewer':lindan_water_sewer,
               'mirex_water_sewer':mirex_water_sewer,
               'naphthalen_water_sewer':naphthalen_water_sewer,
               'nikkel_water_sewer':nikkel_water_sewer,
               'dioxin_water_sewer':dioxin_water_sewer,
               'pentachlorbenzen_water_sewer':pentachlorbenzen_water_sewer,
               'pentachlorphenol_water_sewer':pentachlorphenol_water_sewer,
               'phenoler_water_sewer':phenoler_water_sewer,
               'polychlorerede_water_sewer':polychlorerede_water_sewer,
               'polycykliske_water_sewer':polycykliske_water_sewer,
               'simazin_water_sewer':simazin_water_sewer,
               'tetrachlorethylen_water_sewer':tetrachlorethylen_water_sewer,
               'tetrachlormethan_water_sewer':tetrachlormethan_water_sewer,
               'toluen_water_sewer':toluen_water_sewer,
               'toxaphen_water_sewer':toxaphen_water_sewer,
               'trichlorethylen_water_sewer':trichlorethylen_water_sewer,
               'trichlormethan_water_sewer':trichlormethan_water_sewer,
               'trifluralin_water_sewer':trifluralin_water_sewer,
               'vinylchlorid_water_sewer':vinylchlorid_water_sewer,
               'xylener_water_sewer':xylener_water_sewer,
               'zynk_water_sewer':zynk_water_sewer,
               'carbon_ghg':carbon_ghg,
               'chlorfluorcarboner_ghg':chlorfluorcarboner_ghg,
               'dinitrogenoxid_ghg':dinitrogenoxid_ghg,
               'hydrochlorfluorcarboner_ghg':hydrochlorfluorcarboner_ghg,
               'hydrofluorcarboner_ghg':hydrofluorcarboner_ghg,
               'metan_ghg':metan_ghg,
               'perfluorcarboner_ghg':perfluorcarboner_ghg,
               'svovlhexafluorid_ghg':svovlhexafluorid_ghg,               
               
               'strange_air':strange_air,
               'strange_water_rec':strange_water_rec, 
               'strange_water_sew':strange_water_sew, 
               'strange_list':strange_list, 'measure_list':measure_list, 'problems':problems}
    row_to_add=pd.Series(values_to_add)
    df=df.append(row_to_add, ignore_index=True)
   except:
    list_check.append("page"+str(i)+" - "+str(count))

 
 

#save
writer=pd.ExcelWriter('GreenAccounts2010_RawFormat.xlsx')
df.to_excel(writer, index=False)
writer.save()
