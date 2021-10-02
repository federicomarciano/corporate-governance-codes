********************************************************************************
*tables.do 09/28/2021 **********************************************************
********************************************************************************
/*
Aim: this code produces the tables and pictures related to the master dataset of the 
CorporateGovernance research project. Tables are store in a log file.  

Legend: 

*SECTION****

*Subsection----

*Paragraph.....

*/






*PRELIMINARIES******************************************************************
clear all
local dir `c(pwd)'
cd "`dir'"
use CorporateGovernance_Master.dta, clear
capture mkdir "TablesAndGraphs"
capture mkdir "TablesAndGraphs/UltimateOwners"
capture mkdir "TablesAndGraphs/Accounting"
capture mkdir "TablesAndGraphs/LargeFirms"
capture log close
log using "TablesAndGraphs\TablesMasterLog", replace






*RIGHT-HAND SIDE****************************************************************
*produces tables consirdering the master dataset. 



*simplify 
forvalues i=1/20 {
	drop natural_person`i' share`i' cvr_share`i' cvr_vote`i' cvr_type_owner`i' cvr_type_all_owner`i' /*
	*/ kob_type_owner`i' cvr_nace_owner`i' cvr_isic_owner`i' kob_db07_1_owner`i' /*
	*/ kob_db07_2_owner`i' kob_db07_3_owner`i' listed_owner`i' 
} 


*number of observation
display(_N)


*number of firms 
preserve 
bysort cvr_firm (year): gen num=_n 
keep if num==1 
gen n_firms=_N
display(n_firms)
restore 


*number of firms per year 
preserve 
bysort year (cvr_firm): gen N=_N 
by year: sum N 
*as time progresses, we get more firms
restore


*number of firms for which we don't have Experian data 
preserve 
drop if kob_type==.
bysort cvr_firm (year): gen num=_n 
keep if num==1 
gen n_firms=_N
display(n_firms)
restore 


*how unbalanced is the panel?
preserve 
bysort cvr_firm (year): gen n_obs=_N
bysort cvr_firm (year): gen num=_n 
keep if num==1 
tab n_obs 
restore





*general------------------------------------------------------------------------


*n_owners 
preserve 
keep cvr_firm year n_owners
sort year 
by year: egen av_n_owners=mean(n_owners)
set scheme s2color
line av_n_owners year, xtitle("Year") ytitle("Average number of owners") ysc(r(0 2)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(0.5)2) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\n_owners.png", as(png) name("Graph") replace
restore 


*% of firms whose controller is a natural person
preserve 
keep cvr_firm year natural_person 
sort year 
by year: egen s_nat_per=mean(natural_person)
replace s_nat_per=100*s_nat_per
set scheme s2color
line s_nat_per year, xtitle("Year") ytitle("% firms whose controller is a natural person") ysc(r(0 100)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(20)100) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\nat_pers.png", as(png) name("Graph") replace
restore 


*% of listed companies 
preserve 
sort year 
by year: egen av_listed=mean(listed)
replace av_listed=100*av_listed 
set scheme s2color
line av_listed year, xtitle("Year") ytitle("% listed firms") ysc(r(0 1)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(0.1)1) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\listed.png", as(png) name("Graph") replace
restore 


*sublisted companies
preserve 
sort year
keep cvr_firm year sublisted 
by year: egen av_sublisted=mean(sublisted)
replace av_sublisted=100*av_sublisted 
set scheme s2color
line av_sublisted year, xtitle("Year") ytitle("% sublisted firms") ysc(r(0 1)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(0.1)1) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\sublisted.png", as(png) name("Graph") replace
restore 


*foreign controller 
preserve 
sort year 
keep cvr_firm year foreign 
by year: egen av_foreign =mean(foreign)
replace av_foreign=100*av_foreign
set scheme s2color
line av_foreign year, xtitle("Year") ytitle("% of firms with a foreign controller") ysc(r(0 100)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(20)100) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\foreign.png", as(png) name("Graph") replace
restore 


*foreign controller in listed firms
preserve 
sort year 
keep cvr_firm year foreign listed 
keep if listed==1
by year: egen av_foreign =mean(foreign)
replace av_foreign=100*av_foreign
set scheme s2color
line av_foreign year, xtitle("Year") ytitle("% of firms with a foreign controller among listed firms") ysc(r(0 100)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(20)100) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\foreign_listed.png", as(png) name("Graph") replace
restore 


*changing controller 
preserve 
keep cvr_firm year ctrl_owner_id
gen ctrl_change=0 
bysort cvr_firm (year): replace ctrl_change = 1 if ctrl_owner_id[_n]!=ctrl_owner_id[_n - 1] & ctrl_owner_id[_n - 1]!="" & ctrl_owner_id[_n]!="" & _n!=1
by cvr_firm: egen t_ctrl_change = sum(ctrl_change)
sort year 
by year: egen av_ctrl_change =mean(ctrl_change)
replace av_ctrl_change=100*av_ctrl_change 
set scheme s2color
line av_ctrl_change year, xtitle("Year") ytitle("% of firms changing controller") ysc(r(0 10)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(2)10) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\change.png", as(png) name("Graph") replace
*this is to have one observation per company (otherwise you give different weights)
bysort cvr_firm (year): gen num=_n
keep if num==1 
sum t_ctrl_change, detail 
restore


*voting share of the controller
preserve 
keep cvr_firm year ctrl_voting_share 
sort year
by year: egen av_ctrl_share = mean(ctrl_voting_share)
replace av_ctrl_share=100*av_ctrl_share 
set scheme s2color
line av_ctrl_share year, xtitle("Year") ytitle("Average voting share of the controller") ysc(r(0 100)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(20)100) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\ctrl_share.png", as(png) name("Graph") replace
restore 


*direct controller 
preserve
keep cvr_firm year direct
sort year  
by year: egen av_dir=mean(direct)
replace av_dir=100*av_dir 
set scheme s2color
line av_dir year, xtitle("Year") ytitle("% of firms whose controller is a direct owner") ysc(r(0 100)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(20)100) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\av_direct.png", as(png) name("Graph") replace 
restore 
preserve 
keep cvr_firm year direct foreign 
keep if foreign == 1 
sort year 
by year: egen av_dir=mean(direct)
replace av_dir=100*av_dir 
set scheme s2color
line av_dir year, xtitle("Year") ytitle("% of firms whose foreign controller is a direct owner") ysc(r(0 2)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(20)100) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\av_direct_foreign.png", as(png) name("Graph") replace 
restore
 

*hhi 
preserve 
keep if employees>50 
sort year 
keep cvr_firm year HHI HHI_ultimate    
by year: egen av_hhi =mean(HHI)
by year: egen av_hhi_ulti = mean(HHI_ultimate) 
set scheme s2color
line av_hhi year, xtitle("Year") ytitle("Average HHI") ysc(r(0 1)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(0.1)1) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\hhi.png", as(png) name("Graph") replace
line av_hhi_ulti year, xtitle("Year") ytitle("Average HHI") ysc(r(0 1)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(0.1)1) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\hhi_ultimate.png", as(png) name("Graph") replace
restore 





*Direct Owners------------------------------------------------------------------


*n_owners 
preserve 
keep cvr_firm year n_owners
sort year 
by year: egen av_n_owners=mean(n_owners)
set scheme s2color
line av_n_owners year, xtitle("Year") ytitle("Average number of direct owners") ysc(r(0 2)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(0.5)2) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\n_owners.png", as(png) name("Graph") replace
restore 


*voting share of natural persons
preserve 
sort year 
keep cvr_firm year Tnat_share 
by year: egen av_Tnat_share =mean(Tnat_share)
replace av_Tnat_share=100*av_Tnat_share 
set scheme s2color
line av_Tnat_share year, xtitle("Year") ytitle("Voting share of natural persons") ysc(r(0 100)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(20)100) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\share_nat_peo.png", as(png) name("Graph") replace
restore 


*profit share of natural persons 
preserve 
sort year 
keep cvr_firm year Tnat_pro_share 
by year: egen av_Tnat_share =mean(Tnat_pro_share)
replace av_Tnat_share=100*av_Tnat_share 
set scheme s2color
line av_Tnat_share year, xtitle("Year") ytitle("Profit share of natural persons") ysc(r(0 100)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(20)100) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\share_nat_peo_profit.png", as(png) name("Graph") replace
restore 


*voting share of listed companies 
preserve 
sort year 
keep cvr_firm year Tlisted_share  
by year: egen av_Tlisted_share =mean(Tlisted_share)
replace av_Tlisted_share=100*av_Tlisted_share 
set scheme s2color
line av_Tlisted_share year, xtitle("Year") ytitle("Voting share of listed companies") ysc(r(0 10)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(2)10) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\share_listed.png", as(png) name("Graph") replace
restore 


*profit share of listed companies 
preserve 
sort year 
keep cvr_firm year Tlisted_pro_share  
by year: egen av_Tlisted_share =mean(Tlisted_pro_share)
replace av_Tlisted_share=100*av_Tlisted_share 
set scheme s2color
line av_Tlisted_share year, xtitle("Year") ytitle("Profit share of listed companies") ysc(r(0 10)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(2)10) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\share_listed_profit.png", as(png) name("Graph") replace
restore 


*voting share of the governement
preserve 
sort year 
keep cvr_firm year Tgov_share  
by year: egen av_Tgov_share =mean(Tgov_share)
replace av_Tgov_share=100*av_Tgov_share 
set scheme s2color
line av_Tgov_share year, xtitle("Year") ytitle("Voting share of Government ") ysc(r(0 10)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(2)10) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\share_gov.png", as(png) name("Graph") replace
restore 


*profit share of the government 
preserve 
sort year 
keep cvr_firm year Tgov_pro_share  
by year: egen av_Tgov_share =mean(Tgov_pro_share)
replace av_Tgov_share=100*av_Tgov_share 
set scheme s2color
line av_Tgov_share year, xtitle("Year") ytitle("Profit share of Government ") ysc(r(0 10)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(2)10) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\share_gov_profit.png", as(png) name("Graph") replace
restore


*voting share of foundation
preserve 
sort year 
keep cvr_firm year Tfound_share  
by year: egen av_Tfound_share =mean(Tfound_share)
replace av_Tfound_share=100*av_Tfound_share 
set scheme s2color
line av_Tfound_share year, xtitle("Year") ytitle("Voting share of foundations") ysc(r(0 10)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(2)10) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\share_found.png", as(png) name("Graph") replace
restore 


*profit share of foundations
preserve 
sort year 
keep cvr_firm year Tfound_pro_share  
by year: egen av_Tfound_share =mean(Tfound_pro_share)
replace av_Tfound_share=100*av_Tfound_share 
set scheme s2color
line av_Tfound_share year, xtitle("Year") ytitle("Profit share of foundations") ysc(r(0 10)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(2)10) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\share_found_profit.png", as(png) name("Graph") replace
restore 


*firms privately owned 
preserve 
sort year 
keep cvr_firm year priv_pro_owned 
by year: egen av_priv_owned =mean(priv_pro_owned)
replace av_priv_owned=100*av_priv_owned
set scheme s2color
line av_priv_owned year, xtitle("Year") ytitle("% of firms which are owned by the private sector") ysc(r(90 100)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(90(2)100) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\priv_owned.png", as(png) name("Graph") replace
restore 





*Ultimate Owners----------------------------------------------------------------


*n_owners 
preserve 
keep cvr_firm year n_ultimate_owners
sort year 
by year: egen av_n_owners=mean(n_ultimate_owners)
set scheme s2color
line av_n_owners year, xtitle("Year") ytitle("Average number of ultimate owners") ysc(r(0 2)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(0.5)2) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\UltimateOwners\n_owners.png", as(png) name("Graph") replace
restore 


*voting share of natural persons
preserve 
sort year 
keep cvr_firm year Tnat_ultimate_share 
by year: egen av_Tnat_share =mean(Tnat_ultimate_share)
replace av_Tnat_share=100*av_Tnat_share 
set scheme s2color
line av_Tnat_share year, xtitle("Year") ytitle("Voting share of natural persons as ultimate owners") ysc(r(90 100)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(20)100) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\UltimateOwners\share_nat_peo.png", as(png) name("Graph") replace
restore 


*voting share of listed companies 
preserve 
sort year 
keep cvr_firm year Tlisted_ultimate_share  
by year: egen av_Tlisted_share = mean(Tlisted_ultimate_share)
replace av_Tlisted_share=100*av_Tlisted_share 
set scheme s2color
line av_Tlisted_share year, xtitle("Year") ytitle("Voting share of listed companies as ultimate owners") ysc(r(0 1.2)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(0.2)1.2) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\UltimateOwners\share_listed.png", as(png) name("Graph") replace
restore 


*voting share of the governement
preserve 
sort year 
keep cvr_firm year Tgov_ultimate_share  
by year: egen av_Tgov_share =mean(Tgov_ultimate_share)
replace av_Tgov_share=100*av_Tgov_share 
set scheme s2color
line av_Tgov_share year, xtitle("Year") ytitle("Voting share of Government as ultimate owner") ysc(r(0 0.1)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(0.02)0.1) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\UltimateOwners\share_gov.png", as(png) name("Graph") replace
restore 


*voting share of foundations
preserve 
sort year 
keep cvr_firm year Tfound_ultimate_share  
by year: egen av_Tfound_share =mean(Tfound_ultimate_share)
replace av_Tfound_share=100*av_Tfound_share 
set scheme s2color
line av_Tfound_share year, xtitle("Year") ytitle("Voting share of foundations as ultimate owners") ysc(r(0 10)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(2)10) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\UltimateOwners\share_found.png", as(png) name("Graph") replace
restore 


*direct 
preserve 
sort year 
keep cvr_firm year Tdirect_share  
by year: egen av_Tfound_share =mean(Tdirect_share)
replace av_Tfound_share=100*av_Tfound_share 
set scheme s2color
line av_Tfound_share year, xtitle("Year") ytitle("Voting share of direct owners as ultimate owners") ysc(r(0 10)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(20)100) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\UltimateOwners\share_direct.png", as(png) name("Graph") replace
restore 

 



*By size------------------------------------------------------------------------
 
 
preserve 
keep year cvr_firm employees 
gen yes=0 
replace yes=1 if employees==. 
sort year
by year: egen frac_50=mean(yes)
line frac_50 year, xtitle("Year") ytitle("% of firms for which the number of employees is missing") ysc(r(0 2)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(20)100) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\LargeFirms\missing.png", as(png) name("Graph") replace
restore 

preserve 
keep year cvr_firm employees 
gen yes=0 
replace yes=1 if employees>50 
sort year
by year: egen frac_50=mean(yes)
line frac_50 year, xtitle("Year") ytitle("% of firms with more than 50 employees") ysc(r(0 2)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(0.5)2) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\LargeFirms\fraction.png", as(png) name("Graph") replace
restore

preserve 
keep if employees>50 
sort year 
keep cvr_firm year n_owners   
by year: egen av_owners =mean(n_owners) 
set scheme s2color
line av_owners year, xtitle("Year") ytitle("Average number of owners") ysc(r(0 2)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(0.5)2) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\LargeFirms\n_owners.png", as(png) name("Graph") replace
restore

preserve 
keep if employees>50 
sort year 
keep cvr_firm year listed   
by year: egen av_listed =mean(listed) 
set scheme s2color
line av_listed year, xtitle("Year") ytitle("% of listed firms") ysc(r(0 1)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(0.1)1) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\LargeFirms\listed.png", as(png) name("Graph") replace
restore


preserve 
keep if employees>50 
sort year 
keep cvr_firm year Tgov_ultimate_share   
by year: egen av_gov =mean(Tgov_ultimate_share) 
set scheme s2color
line av_gov year, xtitle("Year") ytitle("Voting share of the government as ultimate owner") ysc(r(0 1)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(0.1)1) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\LargeFirms\governemnt.png", as(png) name("Graph") replace
restore

preserve 
keep if employees>50 
sort year 
keep cvr_firm year Tfound_ultimate_share   
by year: egen av_found =mean(Tfound_ultimate_share) 
set scheme s2color
line av_found year, xtitle("Year") ytitle("Voting share of foundations as ultimate owners") ysc(r(0 1)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(0.1)1) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\LargeFirms\foundations.png", as(png) name("Graph") replace
restore

preserve 
keep if employees>50 
sort year 
keep cvr_firm year HHI_ultimate   
by year: egen av_hhi =mean(HHI_ultimate) 
set scheme s2color
line av_hhi year, xtitle("Year") ytitle("Average HHI") ysc(r(0 1)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(0.1)1) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\LargeFirms\hhi.png", as(png) name("Graph") replace
restore





*Distributions------------------------------------------------------------------
histogram Tnat_share, percent bin(50) xtitle("Voting share of natural persons") ysc(r(0 1)) ylabel(0(20)100) graphregion(color(white)) bgcolor(white) 
graph export "TablesAndGraphs\hist_tnatshare.png", as(png) name("Graph") replace

histogram Tnat_ultimate_share, percent bin(50) xtitle("Voting share of natural persons") ysc(r(0 1)) ylabel(0(20)100) graphregion(color(white)) bgcolor(white) 
graph export "TablesAndGraphs\hist_tnatshare_ultimate.png", as(png) name("Graph") replace

histogram Tfound_share, percent bin(50) xtitle("Voting share of foundations") ysc(r(0 1)) ylabel(0(20)100) graphregion(color(white)) bgcolor(white) 
graph export "TablesAndGraphs\hist_tfoundshare.png", as(png) name("Graph") replace

histogram Tfound_ultimate_share, percent bin(50) xtitle("Voting share of foundations") ysc(r(0 1)) ylabel(0(20)100) graphregion(color(white)) bgcolor(white) 
graph export "TablesAndGraphs\hist_tfoundshare_ultimate.png", as(png) name("Graph") replace

histogram Tlisted_share, percent bin(50) xtitle("Voting share of listed firms") ysc(r(0 1)) ylabel(0(20)100) graphregion(color(white)) bgcolor(white) 
graph export "TablesAndGraphs\hist_tlisted.png", as(png) name("Graph") replace

histogram Tlisted_ultimate_share, percent bin(50) xtitle("Voting share of listed firms") ysc(r(0 1)) ylabel(0(20)100) graphregion(color(white)) bgcolor(white) 
graph export "TablesAndGraphs\hist_tlisted_ultimate.png", as(png) name("Graph") replace





*Accounting---------------------------------------------------------------------

preserve 
foreach i in cum_rd gross_turnover assets_tot/*
*/ fixed_assets_tot capital_stock  cum_rd_corr assets_tot_corr/*
*/ fixed_assets_tot_corr capital_stock_corr {
	replace `i' = `i'/6.32 
}

estpost tabstat cum_rd gross_turnover KLratio TobinQ slack leverage return_K assets_tot assets_growth /*
*/ fixed_assets_tot capital_stock employees, c(stat) stat(mean sd n)
esttab using "TablesAndGraphs/table_accounting.tex", ///
 cells("mean(fmt(%13.2fc)) sd(fmt(%13.2fc)) count") nonumber ///
  nomtitle nonote noobs label collabels("Mean" "SD" "N of firm-years")

estpost tabstat cum_rd_corr gross_turnover KLratio_corr TobinQ_corr slack_corr leverage_corr return_K_corr assets_tot_corr assets_growth_corr /*
*/ fixed_assets_tot_corr capital_stock_corr employees, c(stat) stat(mean sd n)
esttab using "TablesAndGraphs/table_accounting_corr.tex", ///
 cells("mean(fmt(%13.2fc)) sd(fmt(%13.2fc)) count") nonumber ///
  nomtitle nonote noobs label collabels("Mean" "SD" "N of firm-years")


*histograms 
foreach i in cum_rd KLratio leverage assets_tot  /*
*/  employees {
	histogram `i', percent bin(50) xtitle("`i'") ysc(r(0 1)) ylabel(0(20)100) graphregion(color(white)) bgcolor(white) 
    graph export "TablesAndGraphs/Accounting/`i'.png", as(png) name("Graph") replace 
}


gen employees_corr=employees 
foreach i in cum_rd_corr KLratio_corr leverage_corr assets_tot_corr employees_corr {
	replace `i'=log(`i'+1)
}
foreach i in cum_rd_corr KLratio_corr leverage_corr assets_tot_corr employees_corr {
	histogram `i', percent bin(50) xtitle("`i'") ysc(r(0 1)) ylabel(0(20)100) graphregion(color(white)) bgcolor(white) 
    graph export "TablesAndGraphs/Accounting/`i'.png", as(png) name("Graph") replace 
}
restore 





*PRIVATE EQUITY-----------------------------------------------------------------
cd .. 
use Data\PrivateEquity\PrivateEquity2.dta, clear 
*there is a duplicate, for previous name (I created two observations so that they can be matched)
drop if company_name=="Capital Partners"


*create new variables 
gen esg=0
replace esg=1 if environment==1 & cor_gov==1 & social==1
gen no_impact=0 
replace no_impact=1 if environment==0 & cor_gov==0 & social==0
gen foreign=0 
replace foreign=1 if country!="Denmark" 
label define mylabel 0 "Danish" 1 "Foreign", add
label values foreign mylabel


*modification to have percentages 
foreach i in environment social cor_gov esg no_impact cli_change eco_services health rights labor_sta ethics {
  replace `i'=100*`i'
  }

  

estpost tabstat environment social cor_gov foreign esg no_impact cli_change eco_services health rights labor_sta ethics, statistics(mean) columns(statistics)
esttab using "DoFiles/TablesAndGraphs/table_PE.tex", replace ///
 cells("mean(fmt(%13.2fc))") nonumber ///
  nomtitle nonote noobs label collabels("Percentage")




*Aggregate----------------------------------------------------------------------
cd ..
clear 
capture mkdir DoFiles\TablesAndGraphs\Aggregate 
import excel Data\AggregatePollution\gdp, firstrow
destring year, replace 
replace gdp = gdp/1000000
line gdp year, xtitle("Year") ytitle("GDP") xlabel(1990(5)2020) graphregion(color(white)) bgcolor(white) ylabel( ,angle(0))
graph export "DoFiles\TablesAndGraphs\Aggregate\gdp.png", as(png) name("Graph") replace

clear 
import excel Data\AggregatePollution\GHS_UN, firstrow
destring year, replace
replace gas=gas/1000 
line gas year, xtitle("Year") ytitle("CO2") graphregion(color(white)) bgcolor(white) xline(1996, lpattern(dash))  text(82 1996 "Introduction") xline(2002, lpattern(dash) lcolor(blue))  text(82 2002 "Amendment 1") xline(2010, lpattern(dash) lcolor(grey))  text(82 2010 "Amendment 2") xline(2015, lpattern(dash) lcolor(green))  text(82 2015 "Abolition") xlabel(1990(3)2018) ylabel( ,angle(0))
graph export "DoFiles\TablesAndGraphs\Aggregate\GHG_UN.png", as(png) name("Graph") replace
rename gas gas0 
save Data\AggregatePollution\GHG_UN.dta, replace 

clear 
import excel Data\AggregatePollution\GHS_SD, firstrow
destring year, replace 
drop if year>2018
merge 1:1 year using  Data\AggregatePollution\GHG_UN.dta, nogenerate 
replace gas = gas/1000
label variable gas "Inc International Transport"
label variable gas0 "Exc International Transport"
line gas gas0 year, xtitle("Year") ytitle("CO2") graphregion(color(white)) bgcolor(white) xline(1996, lpattern(dash))  text(125 1996 "Introduction") xline(2002, lpattern(dash) lcolor(blue))  text(125 2002 "Amendment 1") xline(2010, lpattern(dash) lcolor(grey))  text(125 2010 "Amendment 2") xline(2015, lpattern(dash) lcolor(green))  text(125 2015 "Abolition") xlabel(1990(3)2018) legend(size(medsmall)) ysc(r(0 120)) ylabel(0(30)120, angle(0))
graph export "DoFiles\TablesAndGraphs\Aggregate\GHG_SD.png", as(png) name("Graph") replace

clear 
import excel Data\AggregatePollution\GHS_ind, firstrow
destring year, replace
gen others=Finance + Public_Administration + Real_Estate + Information
label variable others "Others"
foreach i in Agriculture Mining Manufacturing Utilities Construction Transport others{
    replace `i'=`i'/1000
}
line Agriculture Mining Manufacturing Utilities Construction Transport others year, xtitle("Year") ytitle("CO2") graphregion(color(white)) bgcolor(white) xline(1996, lpattern(dash))  text(55 1996 "Introduction") xline(2002, lpattern(dash) lcolor(blue))  text(55 2002 "Amendment 1") xline(2010, lpattern(dash) lcolor(grey))  text(55 2010 "Amendment 2") xline(2015, lpattern(dash) lcolor(green))  text(55 2015 "Abolition") xlabel(1990(3)2018) legend(size(medsmall)) ylabel(,angle(0))
graph export "DoFiles\TablesAndGraphs\Aggregate\GHG_Ind.png", as(png) name("Graph") replace



clear 
import excel Data\AggregatePollution\other_UN, firstrow
destring year, replace 
save Data\AggregatePollution\other_UN.dta, replace 
clear
import excel Data\AggregatePollution\other_SD, firstrow
destring year, replace 
merge 1:1 year using Data\AggregatePollution\other_UN.dta, nogenerate 
foreach i in SO2 NH3 NOX PM {
    replace `i'=`i'/1000 
	replace `i'0=`i'0/1000 
	label variable `i' "Inc International Transport"
	label variable `i'0 "Exc International Transport"
	if `i'==NH3 | `i'==PM {
	line `i' `i'0 year, xtitle("Year") ytitle("`i'") xsc(r(1990 2018)) ysc(r(0 150)) graphregion(color(white)) bgcolor(white) xline(1996, lpattern(dash))     text(160 1996 "Introduction") xline(2002, lpattern(dash) lcolor(blue))  text(160 2002 "Amendment 1") xline(2010, lpattern(dash) lcolor(grey))  text(160 2010 "Amendment 2") xline(2015,     lpattern(dash) lcolor(green))  text(160 2015 "Abolition") xlabel(1990(3)2018) legend(size(medsmall)) ylabel(0(30)150, angle(0))  
graph export "`i'.png", as(png) name("Graph") replace
	}
	if `i'==SO2 | `i'==NOX {
	   	line `i' `i'0 year, xtitle("Year") ytitle("`i'") xsc(r(1990 2018)) ysc(r(0 1500)) graphregion(color(white)) bgcolor(white) xline(1996, lpattern(dash))     text(1600 1996 "Introduction") xline(2002, lpattern(dash) lcolor(blue))  text(1600 2002 "Amendment 1") xline(2010, lpattern(dash) lcolor(grey))  text(1600 2010 "Amendment 2") xline(2015,     lpattern(dash) lcolor(green))  text(1600 2015 "Abolition") xlabel(1990(3)2018) legend(size(medsmall)) ylabel(0(300)1500, angle(0))  
graph export "`i'.png", as(png) name("Graph") replace
	} 

}


clear 
import excel Data\AggregatePollution\SO2_ind, firstrow
destring year, replace
gen others=Finance + Public_Administration + Real_Estate + Information
label variable others "Others"
foreach i in Agriculture Mining Manufacturing Utilities Construction Transport others{
    replace `i'=`i'/1000
}
line Agriculture Mining Manufacturing Utilities Construction Transport others year, xtitle("Year") ytitle("SO2") graphregion(color(white)) bgcolor(white) xline(1996, lpattern(dash))  text(950 1996 "Introduction") xline(2002, lpattern(dash) lcolor(blue))  text(950 2002 "Amendment 1") xline(2010, lpattern(dash) lcolor(grey))  text(950 2010 "Amendment 2") xline(2015, lpattern(dash) lcolor(green))  text(950 2015 "Abolition") xlabel(1990(3)2018) legend(size(medsmall)) ylabel(,angle(0)) 
graph export "DoFiles\TablesAndGraphs\Aggregate\SO2_Ind.png", as(png) name("Graph") replace


clear 
import excel Data\AggregatePollution\NH3_ind, firstrow
destring year, replace
gen others=Finance + Public_Administration + Real_Estate + Information
label variable others "Others"
foreach i in Agriculture Mining Manufacturing Utilities Construction Transport others{
    replace `i'=`i'/1000
}
line Agriculture Mining Manufacturing Utilities Construction Transport others year, xtitle("Year") ytitle("NH3") graphregion(color(white)) bgcolor(white) xline(1996, lpattern(dash))  text(160 1996 "Introduction") xline(2002, lpattern(dash) lcolor(blue))  text(160 2002 "Amendment 1") xline(2010, lpattern(dash) lcolor(grey))  text(160 2010 "Amendment 2") xline(2015, lpattern(dash) lcolor(green))  text(160 2015 "Abolition") xlabel(1990(3)2018) legend(size(medsmall)) ylabel(,angle(0)) 
graph export "DoFiles\TablesAndGraphs\Aggregate\NH3_Ind.png", as(png) name("Graph") replace


clear 
import excel Data\AggregatePollution\NOX_ind, firstrow
destring year, replace
gen others=Finance + Public_Administration + Real_Estate + Information
label variable others "Others"
foreach i in Agriculture Mining Manufacturing Utilities Construction Transport others{
    replace `i'=`i'/1000
}
line Agriculture Mining Manufacturing Utilities Construction Transport others year, xtitle("Year") ytitle("NOX") graphregion(color(white)) bgcolor(white) xline(1996, lpattern(dash))  text(1600 1996 "Introduction") xline(2002, lpattern(dash) lcolor(blue))  text(1600 2002 "Amendment 1") xline(2010, lpattern(dash) lcolor(grey))  text(1600 2010 "Amendment 2") xline(2015, lpattern(dash) lcolor(green))  text(1600 2015 "Abolition") xlabel(1990(3)2018) legend(size(medsmall)) ylabel(,angle(0)) 
graph export "DoFiles\TablesAndGraphs\Aggregate\NOX_Ind.png", as(png) name("Graph") replace



clear 
import excel Data\AggregatePollution\PM_ind, firstrow
destring year, replace
gen others=Finance + Public_Administration + Real_Estate + Information
label variable others "Others"
foreach i in Agriculture Mining Manufacturing Utilities Construction Transport others{
    replace `i'=`i'/1000
}
line Agriculture Mining Manufacturing Utilities Construction Transport others year, xtitle("Year") ytitle("PM") graphregion(color(white)) bgcolor(white) xline(1996, lpattern(dash))  text(120 1996 "Introduction") xline(2002, lpattern(dash) lcolor(blue))  text(120 2002 "Amendment 1") xline(2010, lpattern(dash) lcolor(grey))  text(120 2010 "Amendment 2") xline(2015, lpattern(dash) lcolor(green))  text(120 2015 "Abolition") xlabel(1990(3)2018) legend(size(medsmall)) ylabel(,angle(0)) 
graph export "DoFiles\TablesAndGraphs\Aggregate\PM_Ind.png", as(png) name("Graph") replace



clear 
import excel Data\AggregatePollution\CitiesNOX, firstrow 
drop if city=="Odense" 
destring NOX, replace 
preserve 
keep if city=="Copenhagen" 
rename NOX Copenhagen 
tempfile Copenhagen 
save `Copenhagen' 
restore 
preserve 
keep if city=="Aalborg" 
rename NOX Aalborg
tempfile Aalborg
save `Aalborg' 
restore 
keep if city=="Aarhus" 
rename NOX Aarhus 
merge 1:1 year using `Aalborg', nogenerate 
merge 1:1 year using `Copenhagen'
foreach i in Copenhagen Aalborg Aarhus {
	label variable `i' "`i'"
}
drop if year<1990 
line Copenhagen Aalborg Aarhus year, xtitle("Year") ytitle("NOX") graphregion(color(white)) bgcolor(white) xline(1996, lpattern(dash))  text(225 1996 "Introduction") xline(2002, lpattern(dash) lcolor(blue))  text(225 2002 "Amendment 1") xline(2010, lpattern(dash) lcolor(grey))  text(225 2010 "Amendment 2") xline(2015, lpattern(dash) lcolor(green))  text(225 2015 "Abolition")  legend(size(medsmall))  ysc(r(0 220)) ylabel(,angle(0)) xlabel(1990(3)2020) 
graph export "DoFiles\TablesAndGraphs\Aggregate\NOX_Cities.png", as(png) name("Graph") replace


clear 
import excel Data\AggregatePollution\CitiesSO2, firstrow 
drop if city=="Odense" 
destring SO2, replace 
preserve 
keep if city=="Copenhagen" 
rename SO2 Copenhagen 
tempfile Copenhagen 
save `Copenhagen' 
restore 
preserve 
keep if city=="Aalborg" 
rename SO2 Aalborg
tempfile Aalborg
save `Aalborg' 
restore 
keep if city=="Aarhus" 
rename SO2 Aarhus 
merge 1:1 year using `Aalborg', nogenerate 
merge 1:1 year using `Copenhagen'
foreach i in Copenhagen Aalborg Aarhus {
	label variable `i' "`i'"
}
drop if year<1990 
line Copenhagen Aalborg Aarhus year, xtitle("Year") ytitle("SO2") graphregion(color(white)) bgcolor(white) xline(1996, lpattern(dash))  text(16 1996 "Introduction") xline(2002, lpattern(dash) lcolor(blue))  text(16 2002 "Amendment 1") xline(2010, lpattern(dash) lcolor(grey))  text(16 2010 "Amendment 2") xline(2015, lpattern(dash) lcolor(green))  text(16 2015 "Abolition")  legend(size(medsmall))  ysc(r(0 15)) ylabel(,angle(0)) xlabel(1990(3)2020) 
graph export "DoFiles\TablesAndGraphs\Aggregate\SO2_Cities.png", as(png) name("Graph") replace


clear 
import excel Data\AggregatePollution\CitiesPM, firstrow 
drop if city=="Odense" 
destring PM, replace 
preserve 
keep if city=="Copenhagen" 
rename PM Copenhagen 
tempfile Copenhagen 
save `Copenhagen' 
restore 
preserve 
keep if city=="Aalborg" 
rename PM Aalborg
tempfile Aalborg
save `Aalborg' 
restore 
keep if city=="Aarhus" 
rename PM Aarhus 
merge 1:1 year using `Aalborg', nogenerate 
merge 1:1 year using `Copenhagen'
foreach i in Copenhagen Aalborg Aarhus {
	label variable `i' "`i'"
}
drop if year<1990 
line Copenhagen Aalborg Aarhus year, xtitle("Year") ytitle("PM") graphregion(color(white)) bgcolor(white) xline(1996, lpattern(dash))  text(42 1996 "Introduction") xline(2002, lpattern(dash) lcolor(blue))  text(42 2002 "Amendment 1") xline(2010, lpattern(dash) lcolor(grey))  text(42 2010 "Amendment 2") xline(2015, lpattern(dash) lcolor(green))  text(42 2015 "Abolition")  legend(size(medsmall))  ysc(r(0 40)) ylabel(0(10)40,angle(0)) xlabel(1990(3)2020) 
graph export "DoFiles\TablesAndGraphs\Aggregate\PM_Cities.png", as(png) name("Graph") replace

clear 
import excel Data\AggregatePollution\WaterNational, firstrow
destring year, replace 
replace Water=Water/1000 
line Water year, xtitle("Year") ytitle("Waste Water") graphregion(color(white)) bgcolor(white)  xline(2010, lpattern(dash) lcolor(grey))  text(715 2010.5 "Amendment 2") xline(2015, lpattern(dash) lcolor(green))  text(715 2015 "Abolition")  legend(size(medsmall)) ysc(r(300 700)) ylabel(300(50)700,angle(0))  
graph export "DoFiles\TablesAndGraphs\Aggregate\Water.png", as(png) name("Graph") replace



clear 
import excel Data\AggregatePollution\WaterInd, firstrow
destring year, replace
gen others=Finance + Public_Administration + Real_Estate + Information + Oth + Art 
label variable others "Others"
foreach i in Agriculture Mining Manufacturing Utilities Construction Transport others{
    replace `i'=`i'/1000
}
line Agriculture Mining Manufacturing Utilities Construction Transport others year, xtitle("Year") ytitle("Waste Water") graphregion(color(white)) bgcolor(white)  xline(2010, lpattern(dash) lcolor(grey))  text(420 2011 "Amendment 2") xline(2015, lpattern(dash) lcolor(green))  text(420 2015 "Abolition") legend(size(medsmall)) ylabel(0(50)400,angle(0)) 
graph export "DoFiles\TablesAndGraphs\Aggregate\Water_Ind.png", as(png) name("Graph") replace




clear 
import excel Data\AggregatePollution\WaterRegion, firstrow
destring year, replace 
collapse (sum) Hovedstaden Sjælland Syddanmark Midtjylland Nordjylland, by(year)
foreach i in Hovedstaden Sjælland Syddanmark Midtjylland Nordjylland {
	replace `i'=`i'/1000 
	label variable `i' "`i'"
}
line Hovedstaden Sjælland Syddanmark Midtjylland Nordjylland year, xtitle("Year") ytitle("Waste Water") graphregion(color(white)) bgcolor(white)  xline(2015, lpattern(dash) lcolor(green))  text(420 2015 "Abolition") legend(size(medsmall)) ylabel(0(50)400,angle(0)) xline(2010, lpattern(dash) lcolor(grey))  text(420 2011 "Amendment 2")
graph export "DoFiles\TablesAndGraphs\Aggregate\Water_Reg.png", as(png) name("Graph") replace


clear 
import excel Data\AggregatePollution\WasteNational, firstrow
destring year, replace
replace waste=waste/1000000
line waste year, xtitle("Year") ytitle("Waste") graphregion(color(white)) bgcolor(white)  xline(2015, lpattern(dash) lcolor(green))  text(24.5 2015 "Abolition")  legend(size(medsmall)) ylabel(10(2)24,angle(0))  ysc(r(10 24)) 
graph export "DoFiles\TablesAndGraphs\Aggregate\waste_nat.png", as(png) name("Graph") replace

clear 
import excel Data\AggregatePollution\WasteInd, firstrow
destring year, replace
gen others=Finance + Public_Adminsitration + Real_Estate + Information + Oth + Art 
label variable others "Others"
foreach i in Agriculture Mining Manufacturing Utilities Construction Transport others{
    replace `i'=`i'/1000000
}
line Agriculture Mining Manufacturing Utilities Construction Transport others year, xtitle("Year") ytitle("Waste") graphregion(color(white)) bgcolor(white)  xline(2015, lpattern(dash) lcolor(green))  text(21 2015 "Abolition") legend(size(medsmall)) ylabel(0(5)20,angle(0)) 
graph export "DoFiles\TablesAndGraphs\Aggregate\Waste_Ind.png", as(png) name("Graph") replace

clear 
import excel Data\AggregatePollution\shipping\EU_output_total.xls, firstrow
foreach i in EU Denmark Italy Germany Sweden {
rename `i' `i'_total
}
tempfile output_total 
save `output_total'
clear 
import excel Data\AggregatePollution\shipping\EU_output_water.xls, firstrow
merge 1:1 year using `output_total', nogenerate 
foreach i in EU Denmark Italy Germany Sweden {
gen share_`i'=`i'/`i'_total*100
label variable share_`i' "`i'"
}
destring year, replace 
line share_EU share_Denmark share_Italy share_Germany share_Sweden year, xtitle("Year") ytitle("Share of GDP (%)") graphregion(color(white)) bgcolor(white) ylabel(0(0.25)2.25, angle(0)) xlabel(1990(1)2019, angle(90)) 
graph export "DoFiles\TablesAndGraphs\Aggregate\Shipping\share.png", replace 


clear 
import excel Data\AggregatePollution\Shipping\Output_Real_Water, firstrow
destring year, replace 
replace output = output/1000
line output year, xtitle("Year") ytitle("Real Output") graphregion(color(white)) bgcolor(white) ylabel(0(25)250, angle(0)) xlabel(1990(1)2019, angle(90))
graph export "DoFiles\TablesAndGraphs\Aggregate\Shipping\Output_Denamrk.png", replace 
 
clear 
import excel Data\AggregatePollution\Shipping\EU_GHG_water, firstrow
keep year Denmark 
rename Denmark Denmark_GHG 
tempfile GHG 
save `GHG'
import excel Data\AggregatePollution\Shipping\Output_Real_Water, firstrow clear 
merge 1:1 year using `GHG', nogenerate  
replace output=output
replace Denmark_GHG=Denmark_GHG/1000000
label variable Denmark_GHG "GHG Emissions"
gen ratio = Denmark_GHG/output 
label variable ratio "Output-Adjusted GHG Emissions"
destring year, replace
line Denmark_GHG year, xtitle("Year") graphregion(color(white)) bgcolor(white) xlabel(1990(1)2019, angle(90))|| line ratio year,   xtitle("Year") graphregion(color(white)) bgcolor(white)  xlabel(1990(1)2019, angle(90)) yaxis(2)
graph export "DoFiles\TablesAndGraphs\Aggregate\Shipping\GHG_ad.png", replace 


clear 
import excel Data\AggregatePollution\Shipping\EU_SO2_water, firstrow
keep year Denmark 
rename Denmark Denmark_GHG 
tempfile GHG 
save `GHG'
import excel Data\AggregatePollution\Shipping\Output_Real_Water, firstrow clear 
merge 1:1 year using `GHG', nogenerate  
label variable Denmark_GHG "SO2 Emissions"
gen ratio = Denmark_GHG/output 
label variable ratio "Output-Adjusted SO2 Emissions"
destring year, replace 
replace Denmark_GHG = Denmark_GHG/1000
line Denmark_GHG year, xtitle("Year") graphregion(color(white)) bgcolor(white) xlabel(1995(1)2019, angle(90)) || line ratio year,   xtitle("Year") graphregion(color(white)) bgcolor(white)  xlabel(1990(1)2019, angle(90)) yaxis(2)
graph export "DoFiles\TablesAndGraphs\Aggregate\Shipping\SO2_ad.png", replace 



clear 
import excel Data\AggregatePollution\Shipping\EU_NOX_water, firstrow
keep year Denmark 
rename Denmark Denmark_GHG 
tempfile GHG 
save `GHG'
import excel Data\AggregatePollution\Shipping\Output_Real_Water, firstrow clear 
merge 1:1 year using `GHG', nogenerate  
replace output=output
label variable Denmark_GHG "NOx Emissions"
gen ratio = Denmark_GHG/output 
label variable ratio "Output-Adjusted NOx Emissions"
destring year, replace 
replace Denmark_GHG = Denmark_GHG/1000
line Denmark_GHG year, xtitle("Year") graphregion(color(white)) bgcolor(white) xlabel(1995(1)2019, angle(90)) || line ratio year,   xtitle("Year") graphregion(color(white)) bgcolor(white)  xlabel(1990(1)2019, angle(90)) yaxis(2)
graph export "DoFiles\TablesAndGraphs\Aggregate\Shipping\NOx_ad.png", replace



clear 
import excel Data\AggregatePollution\Shipping\EU_PM10_water, firstrow
keep year Denmark 
rename Denmark Denmark_GHG 
tempfile GHG 
save `GHG'
import excel Data\AggregatePollution\Shipping\Output_Real_Water, firstrow clear 
merge 1:1 year using `GHG', nogenerate  
replace output=output
label variable Denmark_GHG "PM10 Emissions"
gen ratio = Denmark_GHG/output 
label variable ratio "Out-Adj PM10 Emissions"
destring year, replace 
replace Denmark_GHG = Denmark_GHG/1000
line Denmark_GHG year, xtitle("Year") graphregion(color(white)) bgcolor(white) xlabel(1995(1)2019, angle(90)) || line ratio year,   xtitle("Year") graphregion(color(white)) bgcolor(white)  xlabel(1990(1)2019, angle(90)) yaxis(2)
graph export "DoFiles\TablesAndGraphs\Aggregate\Shipping\PM10_ad.png", replace



clear 
import excel Data\AggregatePollution\Shipping\EU_PM25_water, firstrow
keep year Denmark 
rename Denmark Denmark_GHG 
tempfile GHG 
save `GHG'
import excel Data\AggregatePollution\Shipping\Output_Real_Water, firstrow clear 
merge 1:1 year using `GHG', nogenerate  
replace output=output
label variable Denmark_GHG "PM2.5 Emissions"
gen ratio = Denmark_GHG/output 
label variable ratio "Out-Adj PM2.5 Emissions"
destring year, replace
replace Denmark_GHG = Denmark_GHG/1000
line Denmark_GHG year, xtitle("Year") graphregion(color(white)) bgcolor(white) xlabel(1995(1)2019, angle(90)) || line ratio year,   xtitle("Year") graphregion(color(white)) bgcolor(white)  xlabel(1990(1)2019, angle(90)) yaxis(2)
graph export "DoFiles\TablesAndGraphs\Aggregate\Shipping\PM25_ad.png", replace



 
foreach j in GHG SO2 NOX PM10 PM25 {
clear
import excel Data\AggregatePollution\Shipping\EU_`j'_total, firstrow
foreach i in EU Denmark Italy Germany Sweden {
rename `i' `i'_total
}
tempfile total 
save `total'
clear 
import excel Data\AggregatePollution\shipping\EU_`j'_water, firstrow
merge 1:1 year using `total', nogenerate 
foreach i in EU Denmark Italy Germany Sweden {
gen share_`i'=`i'/`i'_total*100
label variable share_`i' "`i'"
}
destring year, replace 
line share_EU share_Denmark share_Italy share_Germany share_Sweden year, xtitle("Year") ytitle("Share of `j' Emissions (%)") graphregion(color(white)) bgcolor(white) ylabel(, angle(0)) xlabel(1990(1)2019, angle(90)) 
graph export `j'_share.png, replace 
}

clear 
import excel Data\AggregatePollution\Shipping\EU_outputreal_water, firstrow
destring year, replace 
foreach i in EU Denmark Italy Germany Sweden {
	replace `i'=`i'/1000 
}
line Denmark Italy Germany Sweden year, xtitle("Year") ytitle("Shipping Real GDP") graphregion(color(white)) bgcolor(white) ylabel(, angle(0)) xlabel(1990(1)2019, angle(90)) 
graph export gdp_shipping.png, replace  



clear 
import excel Data\AggregatePollution\Shipping\EU_outputreal_water, firstrow 
foreach i in EU Denmark Germany Italy Sweden {
	replace `i'=`i'*1000
	rename `i' `i'_gdp
}
tempfile gdp 
save `gdp'
clear
import excel Data\AggregatePollution\Shipping\EU_GHG_water, firstrow 
merge 1:1 year using `gdp', nogenerate 
foreach i in EU Denmark Germany Italy Sweden {
	gen `i'_emissions = `i'/`i'_gdp
	label variable `i'_emissions "`i'"
}
destring year, replace 
line EU_emissions Denmark_emissions Germany_emissions Italy_emissions Sweden_emissions year, xtitle("Year") ytitle("GDP-Adjusted GHG Emissions") graphregion(color(white)) bgcolor(white) ylabel(, angle(0)) xlabel(1990(1)2019, angle(90)) 
graph export gdp_GHG.png, replace   



clear 
import excel Data\AggregatePollution\Shipping\EU_outputreal_water, firstrow 
foreach i in EU Denmark Germany Italy Sweden {
	replace `i'=`i'
	rename `i' `i'_gdp
}
tempfile gdp 
save `gdp'
clear
import excel Data\AggregatePollution\Shipping\EU_SO2_water, firstrow 
merge 1:1 year using `gdp', nogenerate 
foreach i in EU Denmark Germany Italy Sweden {
	gen `i'_emissions = `i'/`i'_gdp
	label variable `i'_emissions "`i'"
}
destring year, replace
line EU_emissions Denmark_emissions Germany_emissions Italy_emissions Sweden_emissions year, xtitle("Year") ytitle("GDP-Adjusted SO2 Emissions") graphregion(color(white)) bgcolor(white) ylabel(, angle(0)) xlabel(1990(1)2019, angle(90)) 
graph export gdp_SO2.png, replace   

clear 
import excel Data\AggregatePollution\Shipping\EU_outputreal_water, firstrow 
foreach i in EU Denmark Germany Italy Sweden {
	replace `i'=`i'
	rename `i' `i'_gdp
}
tempfile gdp 
save `gdp'
clear
import excel Data\AggregatePollution\Shipping\EU_NOX_water, firstrow 
merge 1:1 year using `gdp', nogenerate 
foreach i in EU Denmark Germany Italy Sweden {
	gen `i'_emissions = `i'/`i'_gdp
	label variable `i'_emissions "`i'"
}
destring year, replace 
line EU_emissions Denmark_emissions Germany_emissions Italy_emissions Sweden_emissions year, xtitle("Year") ytitle("GDP-Adjusted NOx Emissions") graphregion(color(white)) bgcolor(white) ylabel(, angle(0)) xlabel(1990(1)2019, angle(90)) 
graph export gdp_NOX.png, replace 


clear 
import excel Data\AggregatePollution\Shipping\EU_outputreal_water, firstrow 
foreach i in EU Denmark Germany Italy Sweden {
	replace `i'=`i'
	rename `i' `i'_gdp
}
tempfile gdp 
save `gdp'
clear
import excel Data\AggregatePollution\Shipping\EU_PM10_water, firstrow 
merge 1:1 year using `gdp', nogenerate 
foreach i in EU Denmark Germany Italy Sweden {
	gen `i'_emissions = `i'/`i'_gdp
	label variable `i'_emissions "`i'"
}
destring year, replace 
line EU_emissions Denmark_emissions Germany_emissions Italy_emissions Sweden_emissions year, xtitle("Year") ytitle("GDP-Adjusted PM10 Emissions") graphregion(color(white)) bgcolor(white) ylabel(, angle(0)) xlabel(1990(1)2019, angle(90)) 
graph export gdp_PM10.png, replace 


clear 
import excel Data\AggregatePollution\Shipping\EU_outputreal_water, firstrow 
foreach i in EU Denmark Germany Italy Sweden {
	replace `i'=`i'
	rename `i' `i'_gdp
}
tempfile gdp 
save `gdp'
clear
import excel Data\AggregatePollution\Shipping\EU_PM25_water, firstrow 
merge 1:1 year using `gdp', nogenerate 
foreach i in EU Denmark Germany Italy Sweden {
	gen `i'_emissions = `i'/`i'_gdp
	label variable `i'_emissions "`i'"
}
destring year, replace 
line EU_emissions Denmark_emissions Germany_emissions Italy_emissions Sweden_emissions year, xtitle("Year") ytitle("GDP-Adjusted PM2.5 Emissions") graphregion(color(white)) bgcolor(white) ylabel(, angle(0)) xlabel(1990(1)2019, angle(90)) 
graph export gdp_PM25.png, replace 



clear 
import excel Data\AggregatePollution\others\emissions_water, firstrow
replace CO2=CO2*1000
replace CH4=CH4*25 
replace N2O=N2O*298 
foreach i in SF6 PFC HFC {
    replace `i'=0 if `i'==. 
}
gen GHG = CO2 + CH4 + N2O + SF6 + PFC + HFC  
tempfile emissions_water 
save `emissions_water'
clear
import excel Data\AggregatePollution\others\emissions_total, firstrow
replace CO2=CO2*1000
replace CH4=CH4*25 
replace N2O=N2O*298 
foreach i in SF6 PFC HFC {
    replace `i'=0 if `i'==. 
}
gen GHG = CO2 + CH4 + N2O + SF6 + PFC + HFC 
foreach i in GHG NOX SO2 PM10 PM25 {
    rename `i' `i'_total
}
export excel total_statistics, replace 
tempfile emissions_total 
save `emissions_total'
clear 
import excel Data\AggregatePollution\others\gdp_water, firstrow
tempfile gdp_water 
save `gdp_water' 
clear
import excel Data\AggregatePollution\others\emissions_ut, firstrow
replace CO2_ut=CO2_ut*1000
replace CH4_ut=CH4_ut*25 
replace N2O_ut=N2O_ut*298 
foreach i in SF6_ut PFC_ut HFC_ut {
    replace `i'=0 if `i'==. 
}
gen GHG_ut = CO2_ut + CH4_ut + N2O_ut + SF6_ut + PFC_ut + HFC_ut 
tempfile emissions_ut 
save `emissions_ut'
clear 
import excel Data\AggregatePollution\others\gdp_ut, firstrow
tempfile gdp_ut 
save `gdp_ut'
clear
import excel Data\AggregatePollution\others\gdp_total, firstrow
merge 1:1 year using `emissions_water', nogenerate 
merge 1:1 year using `emissions_total', nogenerate
merge 1:1 year using  `gdp_water', nogenerate 
merge 1:1 year using `gdp_ut', nogenerate 
merge 1:1 year using `emissions_ut'
destring year, replace 
drop if year==2020 
destring gdp_water, replace 
foreach i in GHG NOX SO2 PM10 PM25 {
    replace `i'=`i'/gdp_water
	label variable `i' "Shipping"
	replace `i'_total=(`i'_total-`i')/(gdp_total-gdp_water)
	label variable `i'_total "Other Industries"
}
line GHG GHG_total year, xtitle("Year") ytitle("GDP-Adjusted GHG Emissions") graphregion(color(white)) bgcolor(white) ylabel(, angle(0)) xlabel(1990(1)2019, angle(90))
graph export "DoFiles\TablesAndGraphs\Aggregate\Shipping\GHG_others.png", replace
line NOX NOX_total year, xtitle("Year") ytitle("GDP-Adjusted NOX Emissions") graphregion(color(white)) bgcolor(white) ylabel(, angle(0)) xlabel(1990(1)2019, angle(90))
graph export "DoFiles\TablesAndGraphs\Aggregate\Shipping\NOX_others.png", replace
line SO2 SO2_total year, xtitle("Year") ytitle("GDP-Adjusted SO2 Emissions") graphregion(color(white)) bgcolor(white) ylabel(, angle(0)) xlabel(1990(1)2019, angle(90))
graph export "DoFiles\TablesAndGraphs\Aggregate\Shipping\SO2_others.png", replace
line PM10 PM10_total year, xtitle("Year") ytitle("GDP-Adjusted PM10 Emissions") graphregion(color(white)) bgcolor(white) ylabel(, angle(0)) xlabel(1990(1)2019, angle(90))
graph export "DoFiles\TablesAndGraphs\Aggregate\Shipping\PM10_others.png", replace
line PM25 PM25_total year, xtitle("Year") ytitle("GDP-Adjusted PM2.5 Emissions") graphregion(color(white)) bgcolor(white) ylabel(, angle(0)) xlabel(1990(1)2019, angle(90))
graph export "DoFiles\TablesAndGraphs\Aggregate\Shipping\PM25_others.png", replace


clear 
import excel Data\AggregatePollution\others\emissions_water, firstrow
replace CO2=CO2*1000
replace CH4=CH4*25 
replace N2O=N2O*298 
foreach i in SF6 PFC HFC {
    replace `i'=0 if `i'==. 
}
gen GHG = CO2 + CH4 + N2O + SF6 + PFC + HFC  
tempfile emissions_water 
save `emissions_water'
clear
import excel Data\AggregatePollution\others\emissions_ut, firstrow
replace CO2_ut=CO2_ut*1000
replace CH4_ut=CH4_ut*25 
replace N2O_ut=N2O_ut*298 
foreach i in SF6_ut PFC_ut HFC_ut {
    replace `i'=0 if `i'==. 
}
gen GHG_ut = CO2_ut + CH4_ut + N2O_ut + SF6_ut + PFC_ut + HFC_ut 
tempfile emissions_ut 
save `emissions_ut'
clear 
import excel Data\AggregatePollution\others\gdp_water, firstrow
tempfile gdp_water 
save `gdp_water' 
clear 
import excel Data\AggregatePollution\others\gdp_ut, firstrow
tempfile gdp_ut 
save `gdp_ut'
merge 1:1 year using `emissions_water', nogenerate 
merge 1:1 year using `emissions_ut', nogenerate
merge 1:1 year using  `gdp_water', nogenerate 
destring year, replace 
drop if year==2020 
destring gdp_water, replace 
foreach i in GHG NOX SO2 PM10 PM25 {
    replace `i'=`i'/gdp_water
	label variable `i' "Shipping"
	replace `i'_ut=`i'_ut/gdp_ut
	label variable `i'_ut "Utilities"
}
line GHG GHG_ut year, xtitle("Year") ytitle("GDP-Adjusted GHG Emissions") graphregion(color(white)) bgcolor(white) ylabel(, angle(0)) xlabel(1990(1)2019, angle(90))
graph export "DoFiles\TablesAndGraphs\Aggregate\Shipping\GHG_ut.png", replace
line NOX NOX_ut year, xtitle("Year") ytitle("GDP-Adjusted NOX Emissions") graphregion(color(white)) bgcolor(white) ylabel(, angle(0)) xlabel(1990(1)2019, angle(90))
graph export "DoFiles\TablesAndGraphs\Aggregate\Shipping\NOX_ut.png", replace
line SO2 SO2_ut year, xtitle("Year") ytitle("GDP-Adjusted SO2 Emissions") graphregion(color(white)) bgcolor(white) ylabel(, angle(0)) xlabel(1990(1)2019, angle(90))
graph export "DoFiles\TablesAndGraphs\Aggregate\Shipping\SO2_ut.png", replace
line PM10 PM10_ut year, xtitle("Year") ytitle("GDP-Adjusted PM10 Emissions") graphregion(color(white)) bgcolor(white) ylabel(, angle(0)) xlabel(1990(1)2019, angle(90))
graph export "DoFiles\TablesAndGraphs\Aggregate\Shipping\PM10_ut.png", replace
line PM25 PM25_ut year, xtitle("Year") ytitle("GDP-Adjusted PM2.5 Emissions") graphregion(color(white)) bgcolor(white) ylabel(, angle(0)) xlabel(1990(1)2019, angle(90))
graph export "DoFiles\TablesAndGraphs\Aggregate\Shipping\PM25_ut.png", replace







*Shipping-----------------------------------------------------------------------
clear all
local dir `c(pwd)'
cd "`dir'"
use CorporateGovernance_Master.dta, clear
keep if cvr_nace==50
preserve
display(_N)
fillin cvr_firm year 
keep if year==2003  
display(_N)
restore 
estpost tabstat HHI_ultimate Tgov_share Tfound_share Tgov_ultimate_share Tfound_ultimate_share direct distance natural_person/*
*/ employees assets_tot_corr stay_gen iso_environment_gen iso_production_gen iso_workenviro_gen air , c(stat) stat(mean sd min max n)
esttab using "TablesAndGraphs/table_shipping.tex", ///
 cells("mean(fmt(%13.2fc)) sd(fmt(%13.2fc)) min max count") nonumber ///
  nomtitle nonote noobs label collabels("Mean" "SD" "Min" "Max" "N of firm-years") replace 




*Substances---------------------------------------------------------------------
cd "C:\Users\Dell\Dropbox\Il mio PC (DESKTOP-6UGDNEK)\Desktop\research_professional\corporate_governance_task\chicago\corporate_governance\CorporateGovernance_Main\Data\substances"

foreach i in CO2 CH4 HFCs N2O NF3 NH3 NOX PFCs PM25 SF6 SO2 Dioxins+Furans PCBs Hexabromobiphenyl{
clear
import excel `i', firstrow
sort year
destring citations, replace  
by year: gen n_articles=_N 
by year: egen cit=sum(citations)
destring year, replace 
collapse cit n_articles, by(year)
drop if year<1980
drop if year>2015
label variable n_articles "N articles related to `i'"
label variable cit "Citations"
line n_articles year, xtitle("Year") graphregion(color(white)) bgcolor(white)  xlabel(1980(5)2015)|| line cit year,   xtitle("Year") xlabel(1980(5)2015) graphregion(color(white)) bgcolor(white) yaxis(2)
graph export `i'.png, replace
}
*LEFT-HAND SIDE*****************************************************************

use CorporateGovernance_Master.dta, clear




*GPW----------------------------------------------------------------------------
preserve
sort year
by year: egen av_stay = mean(stay_gen)
replace av_stay=100*av_stay
set scheme s2color
line av_stay year, xtitle("Year") ytitle("% of firms included in the GPW ranking") ysc(r(0 1)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(0.1)1) graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\stayGPW.png", as(png) name("Graph") replace
restore 





*ISO----------------------------------------------------------------------------
preserve 
sort year 
foreach i in iso_production_gen iso_environment_gen iso_workenviro_gen {	
	by year: egen av_`i'=mean(`i')
	set scheme s2color
    line av_`i' year, xtitle("Year") ytitle("Average number of `i'") ysc(r(0 2)) xsc(r(2002 2020)) xlabel(2003(2)2019) ylabel(0(1)5) graphregion(color(white)) bgcolor(white)
    graph export "TablesAndGraphs\av_`i'.png", as(png) name("Graph") replace
	
}
restore





/* *Green Accounts-----------------------------------------------------------------

keep if air!=. 
*number of firm-years and number of firms 
display(_N)
preserve 
bysort cvr_firm (year): gen num=_n 
drop if num!=1 
display(_N)
restore 

preserve 
drop if year<2010
estpost tabulate year 
esttab . using "TablesAndGraphs/table_GreenAccounts.tex", cells("b") noobs replace 
restore 

preserve
drop if year<2009
fillin cvr_firm year 
gen enter_green=.
gen exit_green=.
bysort cvr_firm (year): replace exit_green=1 if _fillin[_n]==1 & _fillin[_n-1]==0 & _n!=1 
drop if year==2009 
bysort cvr_firm (year): replace enter_green=1 if _fillin[_n]==0 & _fillin[_n-1]==1 & _n!=1 

sort year
by year: sum enter_green
by year: sum exit_green 


bysort cvr_firm (year): egen entries=sum(enter_green)
bysort cvr_firm (year): egen exits=sum(exit_green)
keep if year==2010
tabulate entries exits 
esttab . using "TablesAndGraphs/table_GreenAccounts_EntEx.tex", cells("b") noobs replace 
restore 

estpost tabstat air water waste_disp waste_rec listed sublisted natural_person gov_owned found_owned /*
*/ direct distance foreign HHI employees, c(stat) stat(mean sd n)
esttab using "TablesAndGraphs/GreenAccountsSum.tex", replace ///
 cells("mean(fmt(%13.2fc)) sd(fmt(%13.2fc)) count") nonumber ///
  nomtitle nonote noobs label collabels("Mean" "SD" "N of Firm-Years")

cd .. 
use Data\GreenAccounts\GreenAccountsArticles, clear 

*num of firms with national papers articles
preserve 
bysort cvr_firm (positive): gen num=_n 
keep if num==1 
gen national=1 if no_nat_art==0 
gen no_national=1 if no_nat_art==1 & no_art==0 
egen s_nat=sum(national)
egen s_no_nat=sum(no_national)
display(s_nat)
display(s_no_nat)  
restore 

preserve 
keep if no_nat_art==0 
display(_N)
foreach i in positive negative neutral {
sum `i'
display r(sum)
}
restore 

preserve 
keep if no_nat_art==1 & no_art==0 
display(_N)
foreach i in positive negative neutral {
sum `i'
display r(sum)
}
restore */
clear all
local dir `c(pwd)'
cd "`dir'"
use CorporateGovernance_Master.dta, clear

*sample size 



*summary statistics 
preserve 
keep if toxicity !=. 
display(_N) 
replace toxicity=toxicity/1400000000
label variable toxicity "Toxicity"
replace GHGs=GHGs/1000000
estpost tabstat toxicity toxicity_water GHGs, statistics(mean sd min max) columns(statistics)
collapse (mean) toxicity toxicity_water GHGs, by(year) 
drop if year<2007
line toxicity year, xtitle("Year") ytitle("Toxicity") xsc(r(2007 2019)) xlabel(2007(1)2019, angle(90)) ylabel(,angle(0))  graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\toxicity.png", as(png) name("Graph") replace
line GHGs year, xtitle("Year") ytitle("GHGs") xsc(r(2010 2019)) xlabel(2007(1)2019, angle(90)) ylabel(0(20)120, angle(0))  graphregion(color(white)) bgcolor(white)
graph export "TablesAndGraphs\GHGs.png", as(png) name("Graph") replace
restore


*top polluting toxicity 
preserve 
drop if toxicity==. 
gsort year -toxicity 
by year: gen num=_n 
drop if num>3
keep cvr_firm year toxicity 
replace toxicity=toxicity/1400000000
export excel TablesAndGraphs/toppoluting_toxicity, replace 
drop cvr_firm
dataout, save(TablesAndGraphs/toppoluting_toxicity) replace tex
restore 


*top polluitng GHgs 
preserve 
drop if toxicity==. 
gsort year -GHGs 
by year: gen num=_n 
drop if num>3
keep cvr_firm year GHGs 
replace GHGs=GHGs/1000000
export excel TablesAndGraphs/toppoluting_ghg, replace 
drop cvr_firm
dataout, save(TablesAndGraphs/toppoluting_ghg) replace tex

restore 

*top reducing 
preserve 
drop if toxicity==. 
gsort year -toxicity 
by year: gen num=_n 
gen top_t=1 if num<4
replace toxicity=toxicity/1400000000
gsort year -GHGs
by year: gen num2=_n 
gen top_g=1 if num2 <4 
replace GHGs=GHGs/1000000 
bysort cvr_firm(year): egen t=sum(top_t)
bysort cvr_firm(year): egen g=sum(top_g)
drop if t==0 & g==0
bysort cvr_firm (year): gen dif_toc=toxicity[_N]-toxicity[1]
bysort cvr_firm (year): gen dif_g=GHGs[_N]-GHGs[1]
bysort cvr_firm (year): drop if _n!=1 & _n!=_N 
keep cvr_firm year dif_toc dif_g t g 
drop if t==0 
sort dif_t
restore 




*CORRELATIONS*******************************************************************
local dir `c(pwd)'
cd "`dir'"
use CorporateGovernance_Master, clear 

gen ctrl_change=0 
bysort cvr_firm (year): replace ctrl_change = 1 if ctrl_owner_id[_n]!=ctrl_owner_id[_n - 1] & ctrl_owner_id[_n - 1]!="" & ctrl_owner_id[_n]!="" & _n!=1
replace ctrl_change=. if year==2003 




preserve 
gen employees_corr=employees 
foreach i in cum_rd_corr KLratio_corr leverage_corr slack_corr TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr {
	replace `i'=log(`i')
}
*direct 
pwcorr Ranking_gen stay_gen iso_production_gen iso_environment_gen iso_workenviro_gen /*
*/listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share HHI/*
*/ share_top3 ctrl_voting_share ctrl_change cum_rd_corr KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, star(3)

*ultimate 
pwcorr Ranking_gen stay_gen iso_production_gen iso_environment_gen iso_workenviro_gen /*
*/listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share HHI_ultimate /*
*/ share_top3_ultimate ctrl_voting_share ctrl_change cum_rd_corr KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, star(3) 

restore 


 

 
*Private Equity-----------------------------------------------------------------
cd .. 
use Data\PrivateEquity\PrivateEquity2.dta, clear 
drop if company_name=="Capital Partners"
corrtex environment social cor_gov, file(DoFiles/TablesAndGraphs/table_PE_correlations) sig replace



*Green Accounts-----------------------------------------------------------------
local dir `c(pwd)'
cd "`dir'"
use CorporateGovernance_Master.dta, clear 
corrtex iso_production_gen iso_environment_gen iso_workenviro_gen stay_gen air water waste_disp waste_rec,  file(TablesAndGraphs/table_GreenAccounts_correlations) sig






*PRELIMINARY REGRESSIONS********************************************************


gen employees_corr=employees 
foreach i in cum_rd_corr KLratio_corr leverage_corr slack_corr TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr {
	replace `i'=log(`i'+1)
}

*scale coefficients 
foreach i in listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share HHI ctrl_voting_share share_top3 /*
*/ cum_rd_corr KLratio_corr leverage_corr slack_corr TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr{
	replace `i'=0.01*`i'
}




*GPW---------------------------------------------------------------------------- 




*stay_gen.......................................................................


*direct ownership 
set pformat %5.4f, perm
reg stay_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share HHI, robust 
outreg2 using "TablesAndGraphs\regression1", tex replace

reg stay_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share ctrl_voting_share, robust
outreg2 using "TablesAndGraphs\regression1", tex append

reg stay_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share share_top3, robust
outreg2 using "TablesAndGraphs\regression1", tex append 

*these commands are just needed to append the regressions 
preserve 
drop Tlisted_share Tfound_share HHI Tgov_share share_top3
rename Tlisted_ultimate_share Tlisted_share 
rename Tfound_ultimate_share Tfound_share 
rename Tgov_ultimate_share Tgov_share 
rename HHI_ultimate HHI 
rename share_top3_ultimate share_top3 


*ultimate ownership 
reg stay_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share HHI Tdirect_share, robust 
outreg2 using "TablesAndGraphs\regression1", tex append

reg stay_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share Tdirect_share ctrl_voting_share, robust
outreg2 using "TablesAndGraphs\regression1", tex append

reg stay_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share Tdirect_share share_top3, robust
outreg2 using "TablesAndGraphs\regression1", tex append
restore 





*stay_gen and accounting controls...............................................


*direct ownership 
set pformat %5.4f, perm
reg stay_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share HHI KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust 
outreg2 using "TablesAndGraphs\regression1", tex replace

reg stay_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share ctrl_voting_share KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression1", tex append

reg stay_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share share_top3 KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression1", tex append 

*these commands are just needed to append the regressions 
preserve 
drop Tlisted_share Tfound_share HHI Tgov_share share_top3
rename Tlisted_ultimate_share Tlisted_share 
rename Tfound_ultimate_share Tfound_share 
rename Tgov_ultimate_share Tgov_share 
rename HHI_ultimate HHI 
rename share_top3_ultimate share_top3 


*ultimate ownership 
reg stay_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share HHI Tdirect_share KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust 
outreg2 using "TablesAndGraphs\regression1", tex append

reg stay_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share Tdirect_share ctrl_voting_share  KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression1", tex append

reg stay_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share Tdirect_share share_top3  KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression1", tex append
restore 



*ISO----------------------------------------------------------------------------





* no accounting controls........................................................



  
*production direct ownership 
reg iso_production_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share HHI, robust
outreg2 using "TablesAndGraphs\regression2", tex replace dec(2)
 
reg iso_production_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share ctrl_voting_share, robust
outreg2 using "TablesAndGraphs\regression2", tex append dec(2)

reg iso_production_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share share_top3, robust
outreg2 using "TablesAndGraphs\regression2", tex append dec(2)


*environment direct ownership
reg iso_environment_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share HHI, robust
outreg2 using "TablesAndGraphs\regression2", tex append dec(2)
 
reg iso_environment_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share ctrl_voting_share, robust
outreg2 using "TablesAndGraphs\regression2", tex append dec(2) 

reg iso_environment_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share share_top3, robust
outreg2 using "TablesAndGraphs\regression2", tex append dec(2)

*work environment direct ownership
reg iso_workenviro_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share HHI, robust 
outreg2 using "TablesAndGraphs\regression2", tex append dec(2)

reg iso_workenviro_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share ctrl_voting_share, robust
outreg2 using "TablesAndGraphs\regression2", tex append dec(2)

reg iso_workenviro_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share share_top3, robust
outreg2 using "TablesAndGraphs\regression2", tex append dec(2)

*production ultimate ownership
reg iso_production_gen listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share HHI_ultimate, robust
outreg2 using "TablesAndGraphs\regression3", tex replace dec(2)
 
reg iso_production_gen listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share ctrl_voting_share, robust
outreg2 using "TablesAndGraphs\regression3", tex append dec(2) 

reg iso_production_gen listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share share_top3_ultimate, robust
outreg2 using "TablesAndGraphs\regression3", tex append dec(2)

*environment ultimate ownership 
reg iso_environment_gen listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share HHI_ultimate, robust 
outreg2 using "TablesAndGraphs\regression3", tex append dec(2)

reg iso_environment_gen listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share ctrl_voting_share, robust
outreg2 using "TablesAndGraphs\regression3", tex append dec(2)

reg iso_environment_gen listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share share_top3_ultimate, robust
outreg2 using "TablesAndGraphs\regression3", tex append dec(2)


*work environment ultimate ownership 
reg iso_workenviro_gen listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share HHI_ultimate, robust 
outreg2 using "TablesAndGraphs\regression3", tex append dec(2)

reg iso_workenviro_gen listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share ctrl_voting_share, robust
outreg2 using "TablesAndGraphs\regression3", tex append dec(2)

reg iso_workenviro_gen listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share share_top3_ultimate, robust
outreg2 using "TablesAndGraphs\regression3", tex append dec(2)





*accounting controls............................................................



  
*production direct ownership 
reg iso_production_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share HHI KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression2", tex replace dec(2)
 
reg iso_production_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share ctrl_voting_share KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression2", tex append dec(2)

reg iso_production_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share share_top3 KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression2", tex append dec(2)


*environment direct ownership
reg iso_environment_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share HHI KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression2", tex append dec(2)
 
reg iso_environment_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share ctrl_voting_share KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression2", tex append dec(2) 

reg iso_environment_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share share_top3 KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression2", tex append dec(2)

*work environment direct ownership
reg iso_workenviro_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share HHI KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust 
outreg2 using "TablesAndGraphs\regression2", tex append dec(2)

reg iso_workenviro_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share ctrl_voting_share KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression2", tex append dec(2)

reg iso_workenviro_gen listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share share_top3 KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression2", tex append dec(2)

*production ultimate ownership
reg iso_production_gen listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share HHI_ultimate KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression3", tex replace dec(2)
 
reg iso_production_gen listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share ctrl_voting_share KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression3", tex append dec(2) 

reg iso_production_gen listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share share_top3_ultimate KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression3", tex append dec(2)

*environment ultimate ownership 
reg iso_environment_gen listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share HHI_ultimate KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust 
outreg2 using "TablesAndGraphs\regression3", tex append dec(2)

reg iso_environment_gen listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share ctrl_voting_share KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression3", tex append dec(2)

reg iso_environment_gen listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share share_top3_ultimate KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression3", tex append dec(2)


*work environment ultimate ownership 
reg iso_workenviro_gen listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share HHI_ultimate KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust 
outreg2 using "TablesAndGraphs\regression3", tex append dec(2)

reg iso_workenviro_gen listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share ctrl_voting_share KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression3", tex append dec(2)

reg iso_workenviro_gen listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share share_top3_ultimate KLratio_corr leverage_corr slack_corr /*
*/TobinQ_corr assets_growth_corr return_K_corr assets_tot_corr employees_corr, robust
outreg2 using "TablesAndGraphs\regression3", tex append dec(2)






*FIXED EFFECTS******************************************************************

foreach i in stay_gen iso_production_gen iso_environment_gen iso_workenviro_gen {
	display("*****************************************************************")
	display("  `i'  ")
	display("*****************************************************************")
	
	
	*fixed effect of the firm 
	reghdfe `i' listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share HHI, absorb(cvr_firm) 
	reghdfe `i' listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share HHI_ultimate, absorb(cvr_firm)
	
	*fixed effect of the controller 
	reghdfe `i' listed sublisted direct distance natural_person Tgov_share Tlisted_share Tfound_share HHI, absorb(ctrl_owner_id) 
	reghdfe `i' listed sublisted direct distance natural_person Tgov_ultimate_share Tlisted_ultimate_share Tfound_ultimate_share HHI_ultimate, absorb(ctrl_owner_id)
}



log close 





