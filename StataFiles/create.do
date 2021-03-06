********************************************************************************
* create.do   09\17\2021 *******************************************************
********************************************************************************
/*

Aim: 
This code creates the master dataset (CorporateGovernance_Master.dta) on which 
the CorporateGovernance project is based.

Legend: 
*SECTION*****  
*Subsection------ 
*Paragraph.......


WARNING: to run this code, you should set as current directory a folder containing
both CorporateGovernance_HeavyData and CorporateGoverance_Main
*/






*PRELIMINARIES******************************************************************
clear all
*!!!!!!!!!!!!!!!!!!INSETRT DIRECTORY!!!!!!!!!!!!********************************
cd ""
********************************************************************************
set maxvar 10000

*create directory for temporary data
capture mkdir CorporateGovernance_HeavyData\TempData






*PREPARE THE DATESETS BEFORE MERGING********************************************
/*
the master file will mostly rely on Ownership.dta, in which each company has many
observations associated with each year, depending on the number of owners. 
The dataset contains information on the shares of these owners. We need also information on: 
1. legal forms of the companies and their owners
2. controllers 
3. balance sheet 
4. ultimate owners
5. country of the controllers
6. great place to work ranking
7. ISO data  
These pieces of information are in other datasets, which need to be prepared before
running the merge command. This section deals with these operations.
*/





*Ownership.dta------------------------------------------------------------------
/*Each firm is associated with different owners
in every year; long format; each owner is a row, so we have more than one observation
per firm-year. The key variables are: 
1. cvr_vote: voting share according to cvrdenmark
2. cvr_share: profit share according to cvrdenmark
3. share: profit share according to Experian 
I decide to rely on cvrdenmark */
use CorporateGovernance_HeavyData\Ownership.dta, clear

*simplify (these are just variables which contain some details on how the shares have been computed)
drop ownership_source share_rec greater share_corrected share_corrected_y /*
*/ share_corrected_f cvr_vote_rec cvr_share_rec cvr_share_corrected cvr_share_corrected_y cvr_share_corrected_f

* owner_id: unique identification number of an owner 
gen owner_id = "." 
replace owner_id = owner_enhedsnummer if owner_enhedsnummer != ""
replace owner_id = owner_cvr if owner_enhedsnummer ==""

* only kob: this means that the owner is present only according to Experian and has no voting share
gen only_kob=0
replace only_kob=1 if share!=. & cvr_share==. & cvr_vote==. 
replace only_kob=1 if share!=. & cvr_share==0 & cvr_vote==0

* natural_person: equal to 1 if the owner is a natural person, 0 if it is an organization
* it is better to say natural person than person because there exist legal persons.  
gen natural_person = 0
replace natural_person = 1 if owner_enhedsnummer != ""
drop owner_enhedsnummer owner_cvr

*check if a couple of cvr_firm year can be a good panel identifier
duplicates report cvr_firm year owner_id 

* n_owners: number of owners associated with a company in a given year
* n_voting_owners: number of owners with a strictly positive voting share
* num orders the owners according to their voting share and will be the j variable when reshaping
gsort cvr_firm year -cvr_vote 
by cvr_firm year: gen num=_n
by cvr_firm year: egen n_owners=max(num)
/*this command is needed because some owners have a voting share which is equal to zero;
but I don't consider them to compute the number of voting owners*/           
gen exception = 1 if cvr_vote==0 | cvr_vote==. 
by cvr_firm year: egen s_exception=sum(exception)
gen n_voting_owners = n_owners - s_exception 
drop exception s_exception

/*compute the total voting share and the total profit share in order to find problems
(they should always add up to 1)*/ 
bysort cvr_firm year (num): egen Tshare= sum(cvr_vote)
bysort cvr_firm year (num): egen T_pro_share=sum(cvr_share)

*additions to run the merge command smoothly 
gen ctrl_owner_id=owner_id 
gen ultimate_owner_id=owner_id 

*measure of concentration based on voting shares
*Herfendal index
gen H_share = cvr_vote^2 
bysort cvr_firm year (num): egen HHI=sum(H_share)
drop H_share 
*voting share of the top 3 owners
gen share_top3 =0
replace share_top3=1 if n_voting_owners<4 
bysort cvr_firm year (num): replace share_top3 =cvr_vote[1]+cvr_vote[2]+cvr_vote[3] if n_voting_owners>3 

*save
save CorporateGovernance_HeavyData\TempData\Temp_Ownership_1, replace 





*CompanyBase--------------------------------------------------------------------
/*This dataset contains information on the legal form of the companies. It is a 
longitudinal panel, long format. The key variables are: 
1. cvr_type: legal type according to cvrdenmark 
2. cvr_type_all: legal type according to cvrdenmark, in more detail
3. kob_type: legal type according to Experian */ 
use CorporateGovernance_HeavyData\CompanyBase.dta, clear

*listed: 1 if a company is listed (kob_listed is 1 2 instead of 0 1)
*WARNING: this variable relies on Experian data and we need to come up with a better measure
gen listed=kob_listed - 1 
*the variable takes value 0 when it should be missing; then we need these modifications
*I consider listed equal to missing if kob type is missing; when we know that 
*the year before and the year after the firm was listed, then I consider it as listed. 
replace listed=. if kob_type==. 
bysort cvr_firm (year): replace listed=1 if listed==. & listed[_n-1]==1 & listed[_n+1]==1 & _n!=1 

*create owner_id to merge smoothly later on
gen owner_id=cvr_firm

*create ctrl_owner_id to merge smoothly later on
gen ctrl_owner_id=cvr_firm

*create ultimate_owner_id to merge smoothly later on 
gen ultimate_owner_id=cvr_firm

*see which values of kob_type need to be translated 
*rationale: I want to rely on CVR data and use Experian when CVR data is missing;
*the problem with Experian is that there are too many missing values. 
preserve 
keep if kob_type!=. & cvr_type_all==.
tab kob_type 
restore 

*translation
replace cvr_type_all=1 if kob_type==1 & cvr_type_all==. 
replace cvr_type_all=2 if kob_type==2 & cvr_type_all==. 
replace cvr_type_all=4 if kob_type==3 & cvr_type_all==. 
replace cvr_type_all=5 if kob_type==4 & cvr_type_all==. 
replace cvr_type_all=7 if kob_type==5 & cvr_type_all==. 
replace cvr_type_all=10 if kob_type==7 & cvr_type_all==. 
replace cvr_type_all=11 if kob_type==8 & cvr_type_all==. 
replace cvr_type_all=22 if kob_type==13 & cvr_type_all==. 
replace cvr_type_all=23 if kob_type==14 & cvr_type_all==. 
replace cvr_type_all=24 if kob_type==15 & cvr_type_all==. 
replace cvr_type_all=25 if kob_type==16 & cvr_type_all==. 
replace cvr_type_all=32 if kob_type==19 & cvr_type_all==. 
*Notice: this translation will not be relevant to our restricted sample

*type variables: legal form of the company. The main variable is cvr_type_all. 
*I create copies of these variables for owners and controllers so that I can run the merge command smoothly later on. 
foreach i in cvr_type cvr_type_all kob_type {
	gen `i'_owner = `i' 
	label values `i'_owner num_`i'
	gen `i'_ctrl_owner = `i'
	label values `i'_ctrl_owner num_`i'
	gen `i'_ultimate_owner = `i'
	label values `i'_ultimate_owner num_`i'
}

*sector variables and listed 
foreach i in cvr_nace cvr_isic kob_db07_1 kob_db07_2 kob_db07_3 listed  {
	gen `i'_owner = `i' 
	gen `i'_ctrl_owner = `i'
	gen `i'_ultimate_owner = `i'
}

*save 
compress
save CorporateGovernance_HeavyData\TempData\Temp_CompanyBase_1.dta, replace 





*Ctrl_CVRV----------------------------------------------------------------------
/*This dataset associates each company with a controller. It is returned by Andrea's
algorithm and considers CVR voting shares to identify the controllers. It is a 
longitudinal panel, long format. */
use CorporateGovernance_HeavyData\Ctrl_CVRV.dta, clear

*rename and generate to merge smoothly later on 
gen cvr_firm=firmid
rename ctrl_ownid ctrl_owner_id 
gen ownid = ctrl_owner_id

*natural person is 1 if the controller is a natural person 
gen natural_person=0
replace natural_person=1 if ctrl_owner_type=="p"
drop ctrl_owner_type

*see whether the controller is also a direct owner 
merge 1:1 cvr_firm ctrl_owner_id year using CorporateGovernance_HeavyData\TempData\Temp_Ownership_1.dta,/*
*/ keepusing(cvr_firm ctrl_owner_id year cvr_vote) keep(master match)
gen direct=0 
replace direct=1 if _merge==3 & cvr_vote!=0 & cvr_vote!=.  
drop _merge 

*merge with CompanyBase in order to have information on legal form and sector of the controllers
merge m:1 ctrl_owner_id year using CorporateGovernance_HeavyData\TempData\Temp_CompanyBase_1.dta, /*
*/keepusing(cvr_type_ctrl_owner cvr_type_all_ctrl_owner kob_type_ctrl_owner cvr_nace_ctrl_owner cvr_isic_ctrl_owner /*
*/kob_db07_1_ctrl_owner kob_db07_2_ctrl_owner kob_db07_3_ctrl_owner listed_ctrl_owner) keep(match master) nogenerate

*merge with Vote_CVR.dta to see the length of the path leading to the controller
merge 1:1 firmid year ownid using CorporateGovernance_HeavyData\Vote_CVRV.dta, keepusing (min_path_len) keep(master match) nogenerate 

*drop 
drop ownid firmid 

*distance: if the controller is a direct owner, then distance is zero (min_path_len would be 1)
rename min_path_len distance 
replace distance = distance - 1 

*cvr_share (voting share of the controller as a direct owner)
rename cvr_vote cvr_vote_ctrl_owner 

*save 
compress
save CorporateGovernance_HeavyData\TempData\Temp_Ctrl_CVRV_1.dta, replace





* Vote_CVR.dta------------------------------------------------------------------ 
/* It is a longitudinal panel, long format. Each row corresponds to one ultimate owner.
So there are more observations per firm-year. */

*change variables names to run the merge command smoothly
use CorporateGovernance_HeavyData\Vote_CVRV.dta, clear 
rename ownid ultimate_owner_id
rename firmid cvr_firm
gen natural_person=0
replace natural_person=1 if type=="p"
drop type

*see if there are duplicates 
duplicates report cvr_firm year ultimate_owner_id 

*compute the number of ultimate owners 
gsort cvr_firm year -voting_share 
by cvr_firm year: gen num=_n
by cvr_firm year: egen n_ultimate_owners=max(num)

*measures of concentration based on voting shares
*HHI
gen H_share = voting_share^2 
bysort cvr_firm year (num): egen HHI_ultimate=sum(H_share)
drop H_share 
*voting share of the top 3 owners
gen share_top3_ultimate=0
replace share_top3_ultimate=1 if n_ultimate_owners<4 
bysort cvr_firm year (num): replace share_top3_ultimate=voting_share[1]+voting_share[2]+voting_share[3] if n_ultimate_owners>3 

*see which ultimate owners are also direct owners
merge 1:1 cvr_firm ultimate_owner_id year using CorporateGovernance_HeavyData\TempData\Temp_Ownership_1.dta,/*
*/ keepusing(cvr_firm ultimate_owner_id year cvr_vote) keep(master match)
gen direct=0 
replace direct=1 if _merge==3 & cvr_vote!=0 & cvr_vote!=.
drop _merge 

*merge with CompanyBase to have the types of the utlimate owners
merge m:1 ultimate_owner_id year using CorporateGovernance_HeavyData\TempData\Temp_CompanyBase_1.dta,/*
*/ keepusing(cvr_type_all_ultimate_owner listed_ultimate_owner) keep(match master) nogenerate


*compute share 
*natural persons 
*Tnat_ultimate_share: the voting share of the company belonging to natural persons
*nat_ultimate_owned: 1 if natural persons have a voting share other than zero
gen nat_share=0
replace nat_share=voting_share if natural_person==1 
bysort cvr_firm year (num): egen Tnat_ultimate_share=sum(nat_share)
drop nat_share 
gen nat_ultimate_owned=0
replace nat_ultimate_owned=1 if Tnat_ultimate_share!=0 


*listed companies
*Tlisted_ultimate_share: the voting share of the company belonging to listed companies 
*listed_ultimate_owned: 1 if listed companies have a voting share other than zero
gen listed_share=0
replace listed_share=voting_share if listed_ultimate_owner==1
bysort cvr_firm year (num): egen Tlisted_ultimate_share=sum(listed_share)
drop listed_share 
gen listed_ultimate_owned=0
replace listed_ultimate_owned=1 if Tlisted_ultimate_share!=0 


*government 
*Tgov_ultimate_share: the voting share of the company belonging to the governemnt and the like 
*gov_ultimate_owned: 1 if the governemnt and the like have a voting share other than zero
gen gov_share=0
replace gov_share=voting_share if cvr_type_all_ultimate_owner==3 | cvr_type_all_ultimate_owner==26 | cvr_type_all_ultimate_owner==30 | cvr_type_all_ultimate_owner==34
bysort cvr_firm year (num): egen Tgov_ultimate_share=sum(gov_share)
drop gov_share 
gen gov_ultimate_owned=0
replace gov_ultimate_owned=1 if Tgov_ultimate_share!=0 


*private (Assumption: private means whatever is not government; correct?)
gen priv_ultimate_owned=0 
replace priv_ultimate_owned=1 if Tgov_ultimate_share!=1 


*foundations 
gen found_share=0
replace found_share=voting_share if cvr_type_all_ultimate_owner==7 | cvr_type_all_ultimate_owner==19 
bysort cvr_firm year (num): egen Tfound_ultimate_share=sum(found_share)
drop found_share 
gen found_ultimate_owned=0
replace found_ultimate_owned=1 if Tfound_ultimate_share!=0 


*share of direct owners
gen direct_share=0
replace direct_share=voting_share if direct==1
bysort cvr_firm year (num): egen Tdirect_share=sum(direct_share)
drop direct_share 
gen direct_owned=0
replace direct_owned=1 if Tdirect_share!=0 


*simplify 
drop if num!=1 
keep cvr_firm year nat_ultimate_owned gov_ultimate_owned priv_ultimate_owned listed_ultimate_owned found_ultimate_owned /*
*/ direct_owned Tnat_ultimate_share Tgov_ultimate_share /*
*/Tlisted_ultimate_share Tfound_ultimate_share Tdirect_share n_ultimate_owners /*
*/HHI_ultimate share_top3_ultimate

*save 
save CorporateGovernance_HeavyData\TempData\Temp_Vote_CVR_1.dta, replace 





*Accounting---------------------------------------------------------------------
/*This dataset contains information on balance sheet data of the companies. It is 
a longitudinal panel, long format. */


/*There are some cases in which a firm has two observations associated with the same year.
One is conglomerate the other is not. I drop the latter */
foreach i in 2011 2020 {
	use CorporateGovernance_HeavyData\Accounts_`i', clear 
	bysort cvr_firm (year): gen couple=1 if year[_n]==year[_n-1] & _n!=1
	bysort cvr_firm (year): replace couple=1 if year[_n]==year[_n+1] & _n!=_N
	drop if couple==1 & conglomerate==0
	save CorporateGovernance_HeavyData\TempData\Temp_Accounts_`i', replace 
}
clear 


*append to have only one dataset with balance sheet data 
append using CorporateGovernance_HeavyData\TempData\Temp_Accounts_2011 CorporateGovernance_HeavyData\TempData\Temp_Accounts_2020

*checking for duplicates 
duplicates report cvr_firm year 

*save 
save CorporateGovernance_HeavyData\TempData\Temp_Accounts.dta, replace





*id_name------------------------------------------------------------------------
/*This dataset has information on the countries the controllers are from. */
use CorporateGovernance_HeavyData\id_name.dta, clear

*make modifications to merge smoothly later on
tostring id, replace 
rename id ctrl_owner_id
rename name ctrl_owner_name 

*foreign: 1 if the controller is not from Denmark. 
gen foreign=1
replace foreign=0 if country=="DK"
drop type country
save CorporateGovernance_HeavyData\TempData\Temp_id_name_1.dta, replace 





*GPW----------------------------------------------------------------------------
use CorporateGovernance_HeavyData\GPW.dta, clear

*rename 
rename Year year 
rename CVR cvr_firm 

*normalize the ranking 
bysort year Size (Ranking): egen Tot=max(Ranking)
replace Ranking = Ranking/Tot 

*removing the observations without a cvr (they can't be merged with the master dataset)
drop if cvr_firm ==. 


*string 
tostring cvr_firm, replace 
tostring P, replace

*dropping duplicates in order to merge smoothly later on
*this duplicates occur when a company is awarded more than once because the award is 
*assigned to production units 
gen many=0 
bysort cvr_firm year (Ranking): gen num=_n 
bysort cvr_firm year (Ranking): gen n_obs=_N 
replace many=1 if n_obs>1 
drop if num!=1 
*in these cases I consider the highest ranking 

*create variables which can be useful to run regressions 
gen Enter_corrected = .
replace Enter_corrected=1 if Enter==1 
replace Enter_corrected=0 if Exit==1 
gen stay_lim=0 
replace stay_lim=1 if Enter_corrected==. | Enter_corrected==1

*drop 
drop if cvr_firm=="."

drop Size P n_obs num Address id Companyname Tot Enter_corrected 

*save
save CorporateGovernance_HeavyData\TempData\Temp_GPW.dta, replace 





*ISO----------------------------------------------------------------------------
use CorporateGovernance_HeavyData\ISOdata, clear 

*rename 
rename yearstamp year 
rename iso_enviroment iso_environment

*drop the observations which do not correspond to iso certifications
drop if iso_environment == 0 & iso_production == 0 & iso_workenviro == 0 

*generate dummies. 
bysort cvr_firm year (iso_environment): gen num=_n 
foreach i in iso_environment iso_production iso_workenviro {
	bysort cvr_firm year (num): egen s_`i'=sum(`i' )
	gen  `i'_yes=0
	replace `i'_yes=1 if s_`i'!=0 
	drop `i'
	rename `i'_yes `i' 
}
keep if num==1 
keep cvr_firm year iso_environment iso_production iso_workenviro

*save 
compress 
save CorporateGovernance_HeavyData\TempData\Temp_ISOdata.dta, replace 





*GREEN ACCOUNTS-----------------------------------------------------------------
use CorporateGovernance_Main/Data/GreenAccounts/GreenAccounts2010, clear 


/*find observations for which the cvr is missing and see if it is possible 
to get it by hand */ 
gen num=_n
sort cvr_firm
replace cvr_firm="71271919" if company_name=="Dallerupg??rd A/S" & year==2007
replace cvr_firm="28036493" if company_name=="Simon Salling Syrik" & year==2007
replace cvr_firm="12298641" if company_name=="Bent Jensen" & year==2007
replace cvr_firm="55133018" if company_name=="??rhus KOM.V??RKER" & year==2004
replace cvr_firm="74249515" if company_name=="Asger Pedersen" & year==2007
replace cvr_firm="30174968" if company_name=="Varmecentral, Sanderum" & year==2004
replace cvr_firm="30174968" if company_name=="Varmecentral, Dalum" & year==2004
replace cvr_firm="30174968" if company_name=="Varmecentral, Vollsmose" & year==2004
replace cvr_firm="30174968" if company_name=="Varmecentral, Syd??st" & year==2004
replace cvr_firm="30174968" if company_name=="Varmecentral, Billedsk??rervej" & year==2004
replace cvr_firm="29189854" if company_name=="Sdr. Hostrup Losseplads" & year==2004
replace cvr_firm="47872812" if company_name=="??sterg??rd Hovedg??rdv/Tommy Hensberg" & year==2007
replace cvr_firm="34208115" if company_name=="Amager Ressource Center alias I/S AMAGERFORBR??NDINGEN" & year==2017
replace cvr_firm="34208115" if company_name=="Amager Ressource Center alias I/S AMAGERFORBR??NDINGEN" & year==2018
replace cvr_firm="19232344" if company_name=="Gitte Lerche-Simonsen Aps" & year==2007
replace cvr_firm="28434456" if company_name=="Morten Kuhr" & year==2007
replace cvr_firm="16840378" if company_name=="Mogens S??rensen" & year==2007
replace cvr_firm="19046141" if company_name=="Kj??lbygaardv/Mads & Jens Henrik  Th??gersen" & year==2007
replace cvr_firm="14768343" if company_name=="I/S Revas" & year==2007
replace cvr_firm="69857817" if company_name=="Poul Sloth" & year==2007
replace cvr_firm="55133018" if company_name=="??rhusv??rket" & year==2004
replace cvr_firm="11517838" if company_name=="Gab??l N??rregaardv/Poul Marquard Mathiasen" & year==2007
replace cvr_firm="11254217" if company_name=="Landbrug" & year==2001 & num==3025
replace cvr_firm="72476115" if company_name=="Landbrug" & year==2001 & num==3045
replace cvr_firm="84718068" if company_name=="Asmus Johannsen Damm" & year==2007
replace cvr_firm="92746151" if company_name=="Maskinstation ogLandbrug/Asbj??rn Holst Nielsen" & year==2007
replace cvr_firm="21506435" if company_name=="Jens Hovalt Bertelsen" & year==2004
replace cvr_firm="44305011" if company_name=="Gren?? Forbr??ndingsanl??g" & year==2004
replace cvr_firm="27401724" if company_name=="Kolding Affaldskraftvarmev??rk" & year==2001
replace cvr_firm="12059434" if company_name=="Bedstedg??rdV/Bent Eriksson" & year==2007
replace cvr_firm="25935977" if company_name=="Ravnholt v/Anders Heckmann H??y" & year==2007
replace cvr_firm="14018387" if company_name=="G??rdejerJens Otto Ladefoged" & year==2007
replace cvr_firm="11423418" if company_name=="BHJ A/S Protien Food" & year==2004
replace cvr_firm="17137999" if company_name=="Mourits Rahbek" & year==2007
replace cvr_firm="79456314" if company_name=="Lars Bojsen" & year==2007
replace cvr_firm="24202879" if company_name=="Aarhusegnens Andel AMBA" & year==2007
replace cvr_firm="25495942" if company_name=="Haderslev Kraftvarmev??rk A/S" & year==2004
replace cvr_firm="29734097" if company_name=="Karsten Thier Larsen" & year==2007
replace cvr_firm="20445084" if company_name=="Fredsholm Multisite K/S" & year==2007
replace cvr_firm="20247797" if company_name=="Skanska Asfalt I/S" & year==2007
replace cvr_firm="26390370" if company_name=="Stenager mark  I/S" & year==2007
replace cvr_firm="55133018" if company_name=="??rhus KOMMUNALE V??RKER" & year==2004
replace cvr_firm="16406899" if company_name=="NYCOMED Danmark A/S" & year==2007



replace cvr_firm = subinstr(cvr_firm, " ", "",. )
replace cvr_firm = subinstr(cvr_firm, "  ", "",. )
gen leng=length(cvr_firm)
drop if leng==1
replace cvr_firm=substr(cvr_firm,1,8)
bysort cvr_firm year (p_number): gen n_plants=_N 
drop p_number 
collapse (sum) air water_rec water_sew nhaz_waste_rec nhaz_waste_disp haz_waste_rec haz_waste_disp, by(cvr_firm year n_plants)
save CorporateGovernance_HeavyData\TempData\Temp_GreenAccounts.dta, replace 








*MERGE**************************************************************************
use CorporateGovernance_HeavyData\TempData\Temp_Ownership_1, clear 
drop ctrl_owner_id ultimate_owner_id





*Threshold----------------------------------------------------------------------

/*merge with accounts to get balance sheet data (we need the number of employees 
to set the threshold and the variables which represent important controls according 
to the literature); we also need green accounts info*/
merge m:1 cvr_firm year using CorporateGovernance_HeavyData\TempData\Temp_Accounts, /*
*/keepusing(employees acc_currency rd_cost gross_turnover fixed_assets_tot assets_tot/*
*/ equity_tot liquidity long_debt_tot liabilities_tot staff_cost net_income capital_stock) keep(3) nogenerate 


merge m:1 cvr_firm year using CorporateGovernance_HeavyData\TempData\Temp_GreenAccounts, keep(match master) nogenerate 


*THIS IS THE THRESHOLD: 
********************************************************************************
*I drop those companies which have less than 10 employees or missing employees every year; 
*I don't drop companies if they are in the gree accounts 
*Afterwards also the companies for which we never have a total voting share 
*which is equal to one will be dropped. 
******************************************************************************** 
gen dropped = 0 
replace dropped = 1 if (employees<10 & air==.) | (employees==. & air==. )
bysort cvr_firm (year): egen s_dropped=sum(dropped)
bysort cvr_firm (year): gen n_obs=_N
drop if s_dropped == n_obs 
drop dropped s_dropped n_obs





*Owners-------------------------------------------------------------------------
*get data on types and sectors of owners from CompanyBase 
merge m:1 owner_id year using CorporateGovernance_HeavyData\TempData\Temp_CompanyBase_1.dta, /*
*/keepusing(cvr_type_owner cvr_type_all_owner kob_type_owner cvr_nace_owner cvr_isic_owner /*
*/kob_db07_1_owner kob_db07_2_owner kob_db07_3_owner listed_owner) keep(match master) nogenerate

*company
*get data on types and sectors of each company 
merge m:1 cvr_firm year using CorporateGovernance_HeavyData\TempData\Temp_CompanyBase_1.dta, /*
*/keepusing(cvr_type cvr_type_all kob_type cvr_nace cvr_isic kob_db07_1 kob_db07_2 /*
*/kob_db07_3 listed ) keep(match master) nogenerate 






*Shares-------------------------------------------------------------------------
*compute voting and profit shares held by diftypes of owners per firm-year 


*natural persons 
*Tnat_share: the voting share of the company belonging to natural persons
*Tnat_pro_share: the profit share of the company belonging to natural persons
*nat_owned: 1 if natural persons have a voting share other than zero
*nat_pro_owned: 1 if natural persons have a profit share other than zero
gen nat_share=0
gen nat_pro_share=0
replace nat_share=cvr_vote if natural_person==1 
replace nat_pro_share=cvr_share if natural_person==1
bysort cvr_firm year (num): egen Tnat_share=sum(nat_share)
bysort cvr_firm year (num): egen Tnat_pro_share=sum(nat_pro_share)
drop nat_share nat_pro_share
gen nat_owned=0
gen nat_pro_owned=0
replace nat_owned=1 if Tnat_share!=0 
replace nat_pro_owned=1 if Tnat_pro_share!=0


*listed companies
*Tlisted_share: the voting share of the company belonging to listed companies 
*Tlisted_pro_share: the profit share of the company belonging to listed companies 
*listed_owned: 1 if listed companies have a voting share other than zero
*listed_pro_owned: 1 if listed companies have a profit share other than zero
gen listed_share=0
gen listed_pro_share=0
replace listed_share=cvr_vote if listed_owner==1
replace listed_pro_share=cvr_share if listed_owner==1
bysort cvr_firm year (num): egen Tlisted_share=sum(listed_share)
bysort cvr_firm year (num): egen Tlisted_pro_share=sum(listed_pro_share)
drop listed_share listed_pro_share 
gen listed_owned=0
gen listed_pro_owned=0
replace listed_owned=1 if Tlisted_share!=0 
replace listed_pro_owned=1 if Tlisted_pro_share!=0


*goverment 
*Tgov_share: the voting share of the company belonging to the governemnt and the like 
*Tgov_pro_share: the profit share of the company belonging to the governemnt and the like 
*gov_owned: 1 if the governemnt and the like have a voting share other than zero
*gov_pro_owned: 1 if the governemnt and the like have a profit share other than zero
gen gov_share=0
gen gov_pro_share=0
replace gov_share=cvr_vote if cvr_type_all_owner==3 | cvr_type_all_owner==26 | cvr_type_all_owner==30 | cvr_type_all_owner==34
replace gov_pro_share=cvr_share if cvr_type_all_owner==3 | cvr_type_all_owner==26 | cvr_type_all_owner==30 | cvr_type_all_owner==34
bysort cvr_firm year (num): egen Tgov_share=sum(gov_share)
bysort cvr_firm year (num): egen Tgov_pro_share=sum(gov_pro_share)
drop gov_share gov_pro_share
gen gov_owned=0
gen gov_pro_owned=0
replace gov_owned=1 if Tgov_share!=0 
replace gov_pro_owned=1 if Tgov_pro_share!=0


*private (Assumption: private means whatever is not government; correct?)
gen priv_owned=0 
gen priv_pro_owned=0
replace priv_owned=1 if Tgov_share!=1 
replace priv_pro_owned=1 if Tgov_pro_share!=1


*foundations 
gen found_share=0
gen found_pro_share=0
replace found_share=cvr_vote if cvr_type_all_owner==7 | cvr_type_all_owner==19
replace found_pro_share=cvr_share if cvr_type_all_owner==7 | cvr_type_all_owner==19 
bysort cvr_firm year (num): egen Tfound_share=sum(found_share)
bysort cvr_firm year (num): egen Tfound_pro_share=sum(found_pro_share)
drop found_share found_pro_share
gen found_owned=0
gen found_pro_owned=0
replace found_owned=1 if Tfound_share!=0 
replace found_pro_owned=1 if Tfound_pro_share!=0 


*save
save CorporateGovernance_HeavyData\TempData\Temp_Ownership_2, replace





*Deal with the number of owners-------------------------------------------------
/*if we kept details on each owner for the companies with more than 20 owners 
the master dataset would be too cumbersome; notice that 0.0003 is the fraction of firms
with more than 20 owners. */
gen own20=0
replace own20=1 if n_owners>20

*drop: I do not keep the details related to the owners after owner20 (the order relies on voting shares)
drop if num>20 


/*reshape in order to have one observation per year per company; each owner 
will correspond to a column*/
reshape wide owner_id natural_person share cvr_share cvr_vote  cvr_type_owner /*
*/cvr_type_all_owner kob_type_owner cvr_nace_owner cvr_isic_owner /*
*/kob_db07_1_owner kob_db07_2_owner kob_db07_3_owner listed_owner only_kob, /*
*/ i(cvr_firm year) j(num) 


*merge to have controllers from Ctrl_CVRV
merge 1:1 cvr_firm year using CorporateGovernance_HeavyData\TempData\Temp_Ctrl_CVRV_1.dta, keep(match master) nogenerate


*merge to have information on the controller 
merge m:1 ctrl_owner_id using CorporateGovernance_HeavyData\TempData\Temp_id_name_1,/*
*/ keep(match master) nogenerate 

*merge with the ultimate owners 
merge 1:1 cvr_firm year using CorporateGovernance_HeavyData\TempData\Temp_Vote_CVR_1.dta, keep(match master) nogenerate 

*merge with great place to work 
merge 1:1 cvr_firm year using CorporateGovernance_HeavyData\TempData\Temp_GPW.dta, keep(match master) nogenerate

*merge with iso 
merge 1:1 cvr_firm year using CorporateGovernance_HeavyData\TempData\Temp_ISOdata, keep(match master) nogenerate




*MANIPULATIONS TO THE MASTER DATASET******************************************** 

*creating variables 
*sublisted is 1 if a company is not listed but the controller is 
gen sublisted=0 
replace sublisted=1 if listed_ctrl_owner==1 & listed==0
replace sublisted=. if ctrl_owner_id=="" 

*drop if it never has a good total voting share 
gen problem = 1 if Tshare<0.9 | Tshare>1.1 
bysort cvr_firm (year): egen s_problem = sum(problem)
bysort cvr_firm (year): gen n_obs = _N 
drop if s_problem == n_obs 
drop n_obs problem s_problem 

*replace 0 with missing when there is no voting owner 
foreach i in nat_owned gov_owned priv_owned listed_owned found_owned Tnat_share /*
*/Tgov_share Tlisted_share Tfound_share HHI share_top3 {
	replace `i'=. if n_voting_owners==0 
}

*replace 0 with missing when the total profit share is 0 according to cvr denmark 
foreach i in nat_pro_owned gov_pro_owned priv_pro_owned listed_pro_owned found_pro_owned Tnat_pro_share /*
*/Tgov_pro_share Tlisted_pro_share Tfound_pro_share {
	replace `i'=. if T_pro_share==0 
}

*replace values with missing when there is no controller 
foreach i in natural_person foreign nat_ultimate_owned gov_ultimate_owned priv_ultimate_owned listed_ultimate_owned found_ultimate_owned /*
*/ direct_owned Tnat_ultimate_share Tgov_ultimate_share Tlisted_ultimate_share /*
*/Tfound_ultimate_share Tdirect_share HHI_ultimate share_top3_ultimate {
	replace `i'=. if ctrl_owner_id==""
}


*create stay_lim and  stay_gen for GPW 
bysort cvr_firm (year): egen s_stay_lim=sum(stay_lim)
replace stay_lim=0 if stay_lim==. & s_stay_lim > 0
gen stay_gen=stay_lim 
replace stay_gen=0 if stay_gen ==.
drop s_stay_lim 

*create Ranking_gen for GPW 
gen Ranking_gen=Ranking 
replace Ranking_gen=0 if Ranking==. 

*creat Ranking_lim for GPW 
gen Ranking_lim=. 
replace Ranking_lim=0 if stay_lim==0 
replace Ranking_lim=Ranking if stay_lim==1 

*create variables for iso
*x_lim is missing when a firm never receives the certification
*x_gen is never missing 
foreach i in iso_environment iso_workenviro iso_production{
    
	*this is needed because in ISOdata.dta in the variables are zero when should be missing
	replace `i' =. if `i'==0
	
	*presence takes value zero if you never receive the certification.
	by cvr_firm: egen presence_`i'=sum(`i')
	
	*this wil become x_lim 
	replace `i'=0 if `i'==. & presence_`i'!=0  
     
	*I make sure to have a dummy which takes value one when the certification is still valid. 
	gen `i'_ext=`i'
	bysort cvr_firm(year): replace `i'_ext=1 if `i'[_n-1]==1 & _n!=1
	bysort cvr_firm(year): replace `i'_ext=1 if `i'[_n-2]==1 & _n!=2
	
	
	drop `i' 
	rename `i'_ext `i'_lim 
	gen `i'_gen= `i'_lim
	replace `i'_gen=0 if `i'_gen==. 
}




*creating accounting control variables------------------------------------------


*conversion into dkk 
tab acc_currency

foreach i in rd_cost gross_turnover fixed_assets_tot assets_tot/*
*/ equity_tot liquidity long_debt_tot liabilities_tot staff_cost {
	replace `i'=4.65*`i' if acc_currency=="AUD" 
	replace `i'=6.87*`i' if acc_currency=="CHF"
	replace `i'=7.44*`i' if acc_currency=="EUR"
	replace `i'=8.69*`i' if acc_currency=="GBP"
	replace `i'=0.085*`i' if acc_currency=="INR"
	replace `i'=0.050*`i' if acc_currency=="ISK"
	replace `i'=0.057*`i' if acc_currency=="JPY"
	replace `i'=0.0055*`i' if acc_currency=="KRW"
	replace `i'=0.71*`i' if acc_currency=="NOK"
	replace `i'=1.74*`i' if acc_currency=="QAR"
	replace `i'=0.73*`i' if acc_currency=="SEK"
	replace `i'=6.32*`i' if acc_currency=="USD"
}

foreach i in rd_cost staff_cost {
	replace `i'=-1*`i'
}




*computing the cumulative RD expenditure........................................
*my objective is to compute the cumulative RD expenditure with a 15% depreciation rate 
*checking if some years are skipped 
bysort cvr_firm (year): gen strange=1 if year[_n]!=year[_n-1]+1 & _n!=1
egen s=sum(strange)
*yes, some years are skipped. So I will fill in the dataset in order to compute the depreciation rate 
drop strange s 

preserve 
keep cvr_firm year rd_cost
fillin cvr_firm year 
gen old = 0
replace old=1 if _fillin==0 
drop _fillin
*gen sum creates a running sum
bysort cvr_firm (year): gen s=sum(old) 
*I drop the years which are before the first year the firm enters our sample
drop if s==0 
bysort cvr_firm (year): ipolate rd_cost year, generate(rd_cost_new)
bysort cvr_firm (year): gen num=_n 
drop rd_cost old s
gen cum_rd=0
gen rd_cost_new_new=rd_cost_new
replace rd_cost_new_new=0 if rd_cost_new_new==. 
reshape wide rd_cost_new year cum_rd rd_cost_new_new, i(cvr_firm) j(num)

forvalues i=1/17 {
	forvalues j=1/`i' {
		replace cum_rd`i'=cum_rd`i'+rd_cost_new_new`j'*0.85^(`i'-`j')
	}
}
reshape long
gen missing=1 if rd_cost_new==. 
bysort cvr_firm (year): gen s=sum(missing)
bysort cvr_firm (year): replace cum_rd=. if _n==s | rd_cost_new==.
drop if year==.  
save CorporateGovernance_HeavyData\TempData\Temp_rd, replace
restore 
merge 1:1 cvr_firm year using CorporateGovernance_HeavyData\TempData\Temp_rd, keepusing(cum_rd) keep(match master) nogenerate 
gen  cum_rd_corr=cum_rd 
replace cum_rd_corr=. if cum_rd<0 

*KLratio
gen KLratio=fixed_assets/employees 
gen KLratio_corr= KLratio 
replace KLratio_corr=. if KLratio<0 


*TobinQ
gen TobinQ=equity_tot/assets_tot
gen TobinQ_corr=TobinQ
replace TobinQ_corr=. if equity_tot<0 | assets_tot<0

*slack 
gen slack= liquidity/long_debt_tot
gen slack_corr=slack 
replace slack_corr=. if liquidity<0 | long_debt_tot<0   

*leverage 
gen leverage = liabilities_tot/equity_tot
gen leverage_corr=leverage 
replace leverage_corr=. if liabilities_tot<0 | equity_tot<0  

*return on capital 
bysort cvr_firm (year): gen  return_K=100* net_income / ((fixed_assets_tot[_n]+fixed_assets_tot[_n-1])/2) if _n!=1 
gen return_K_corr = return_K 
bysort cvr_firm (year): replace return_K_corr = . if fixed_assets_tot[_n]<0 | (fixed_assets_tot[_n-1]<0 & _n!=1 )

*fixed assets 
gen fixed_assets_tot_corr= fixed_assets_tot
replace fixed_assets_tot_corr=. if fixed_assets_tot_corr<0

*capital stock 
gen capital_stock_corr= capital_stock
replace capital_stock_corr=. if capital_stock<0

*assets_tot 
gen assets_tot_corr=assets_tot 
replace assets_tot_corr=. if assets_tot<0 

*Asset Growth
gen assets_growth=. 
bysort cvr_firm (year): replace assets_growth=100*log(assets_tot[_n]/assets_tot[_n-1]) if _n!=1
gen assets_growth_corr=. 
bysort cvr_firm (year): replace assets_growth_corr=100*log(assets_tot_corr[_n]/assets_tot_corr[_n-1]) if _n!=1





*creating the dependent variables for environmental performance-----------------
gen water=water_rec+water_sew
gen waste_rec=haz_waste_rec/2 + nhaz_waste_rec/2000
gen waste_disp=haz_waste_disp/2 + nhaz_waste_disp/2000




*create labels for sectors------------------------------------------------------
label define sectors 1 "Agriculture, hunting, forestry and fishing"/*
*/2 "forestry" /*
*/3 "Fisheries and aquaculture"/*
*/5 "Extraction of coal and lignite"/*
*/6 "Extraction of crude oil and natural gas"/*
*/7 "Mining of metal ores"/*
*/8 "Other raw material extraction"/*
*/9 "Services related to raw material extraction"/*
*/10 "Manufacture of food products"/*
*/11 "Manufacture of beverages"/*
*/12 "Manufacture of tobacco products"/*
*/13 "Manufacture of textiles"/*
*/14 "Manufacture of wearing apparel"/*
*/15 "Manufacture of leather and leather goods"/*
*/16 "Manufacture of wood and of products of wood and cork, except furniture; manufacture of articles of straw and plaiting materials"/*
*/17 "Manufacture of paper and paper products"/*
*/18 "Printing and reproduction of recorded media"/*
*/19 "Manufacture of coke and refined petroleum products"/*
*/20 "Manufacture of chemical products"/*
*/21 "Preparation of pharmaceutical raw materials and pharmaceutical preparations"/*
*/22 "Manufacture of rubber and plastic products"/*
*/23 "Manufacture of other non-metallic mineral products"/*
*/24 "Manufacture of basic metals" /*
*/25 "Iron and metal industry, excluding machinery and equipment" /*
*/26 "Manufacture of computer, electronic and optical products" /*
*/27 "Manufacture of electrical equipment" /*
*/28 "Manufacture of machinery and equipment nec" /*
*/29 "Manufacture of motor vehicles, trailers and semi-trailers" /*
*/30 "Manufacture of other transport equipment" /*
*/31 "Manufacture of furniture" /*
*/32 "Other manufacturing" /*
*/33 "Repair and installation of machinery and equipment" /*
*/35 "Electricity, gas and district heating supply" /*
*/36 "Water supply" /*
*/37 "Collection and treatment of wastewater" /*
*/38 "Indsamling, behandling og bortskaffelse af affald; genbrug" /*
*/39 "Soil and groundwater treatment and other forms of pollution control" /*
*/41 "Construction of buildings" /*
*/42 "Construction work" /*
*/43 "Construction that requires specialization" /*
*/45 "Trade in cars and motorcycles, and repair thereof" /*
*/46 "Wholesale trade, except of motor vehicles and motorcycles" /*
*/47 "Detailhandel undtagen med motork??ret??jer og motorcykler" /*
*/49 "Land transport; pipe transport" /*
*/50 "Shipping" /*
*/51 "Aviation" /*
*/52 "Auxiliary services in connection with transport" /*
*/53 "Postal and courier services" /*
*/55 "Accommodation facilities" /*
*/56 "Restaurant business" /*
*/58 "Udgivervirksomhed" /*
*/59 "Production of films, video and television programs, sound recordings and music releases" /*
*/60 "Radio and television business" /*
*/61 "Telekommunikation" /*
*/62 "Computer programming, consultancy services relating to information technology and similar activities" /*
*/63 "Information services" /*
*/64 "Pengeinstitut- og finansieringsvirksomhed undtagen forsikring og pensionsforsikring" /*
*/65 "Insurance, reinsurance and pension insurance with the exception of statutory social insurance" /*
*/66 "Assistance services in connection with financing activities and insurance" /*
*/67 "Real estate" /*
*/69 "Legal assistance, bookkeeping and auditing" /*
*/70 "Headquarters business; business consulting" /*
*/71 "Architectural and engineering services; technical testing and analysis" /*
*/72 "Scientific research and development" /*
*/73 "Advertising and market analysis" /*
*/74 "Other liberal, scientific and technical services" /*
*/75 "Veterinarians" /*
*/77 "Rental and leasing" /*
*/78 "Employment agency" /*
*/79 "Travel agencies 'and tour operators' activities, reservation services and related services" /*
*/80 "Guard and security services and surveillance" /*
*/81 "Real estate and landscaping services" /*
*/82 "Administrative services, office services and other business services" /*
*/84 "Public administration and defense; social Security" /*
*/85 "Education" /*
*/86 "Healthcare" /*
*/87 "Institutionsophold" /*
*/88 "Social measures without institutional stay" /*
*/90 "Creative activities, art and rides" /*
*/91 "Libraries, archives, museums and other cultural activities" /*
*/92 "Lotteri- og anden spillevirksomhed" /*
*/93 "Sports, amusements and leisure activities" /*
*/94 "Organizations and associations" /*
*/95 "Repair of computers and goods for personal and household use" /*
*/96 "Other personal services" /*
*/97 "Households with employed assistants" /*
*/98 "Private households' production of goods and services for own use, i.a.n." /*
*/99 "Ekstraterritoriale organisationer og organer" 

label values cvr_nace sectors 


*identify the firms which should have drawn up green accounts and PRTR reports
gen green_accounts=0 
replace green_accounts=1 if cvr_nace==1 |/*
*/ cvr_nace==5 |/*
*/ cvr_nace==6 |/*
*/ cvr_nace==7 |/*
*/ cvr_nace==10 |/*
*/ cvr_nace==13 |/*
*/ (cvr_nace==16 & employees>20) |/*
*/ cvr_nace==17 |/*
*/ cvr_nace==19 |/*
*/ cvr_nace==20 |/*
*/ cvr_nace==21 |/*
*/ cvr_nace==22 |/*
*/ cvr_nace==25 |/*
*/ cvr_nace==26 |/*
*/ cvr_nace==35 |/*
*/ cvr_nace==37 |/*
*/ cvr_nace==38 |/*
*/ cvr_nace==39 





*order
order cvr_firm year n_owners n_voting_owners cvr_type cvr_type_all kob_type cvr_nace cvr_isic kob_db07_1 kob_db07_2 kob_db07_3 listed sublisted natural_person /*
*/foreign nat_owned gov_owned priv_owned listed_owned found_owned Tnat_share /*
*/Tgov_share Tlisted_share Tfound_share nat_pro_owned gov_pro_owned priv_pro_owned /*
*/listed_pro_owned found_pro_owned Tnat_pro_share Tgov_pro_share Tlisted_pro_share Tfound_pro_share HHI share_top3 Tshare T_pro_share/*
*/ ctrl_owner_id distance direct cvr_type_ctrl_owner cvr_type_all_ctrl_owner kob_type_ctrl_owner /*
*/cvr_nace_ctrl_owner cvr_isic_ctrl_owner kob_db07_1_ctrl_owner kob_db07_2_ctrl_owner kob_db07_3_ctrl_owner/*
*/ ctrl_voting_share listed_ctrl_owner owner_id1




order nat_ultimate_owned gov_ultimate_owned priv_ultimate_owned listed_ultimate_owned found_ultimate_owned /*
*/ direct_owned  Tnat_ultimate_share Tgov_ultimate_share Tlisted_ultimate_share /*
*/Tfound_ultimate_share Tdirect_share HHI_ultimate share_top3_ultimate  n_ultimate_owners/*
*/ stay_lim stay_gen Ranking Enter Exit /*
*/iso_environment_lim iso_production_lim iso_workenviro_lim iso_environment_gen iso_production_gen iso_workenviro_gen /*
*/ cum_rd gross_turnover KLratio TobinQ slack leverage return_K assets_tot /*
*/cum_rd_corr gross_turnover KLratio_corr TobinQ_corr slack_corr leverage_corr return_K_corr assets_tot_corr/*
*/ air water_rec water_sew nhaz_waste_rec nhaz_waste_disp haz_waste_rec haz_waste_disp n_plants water waste_rec waste_disp, last 




*SAVE***************************************************************************
save CorporateGovernance_Main/DoFiles/CorporateGovernance_Master.dta, replace 




 

