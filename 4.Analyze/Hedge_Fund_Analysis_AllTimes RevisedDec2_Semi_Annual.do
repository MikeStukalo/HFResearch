set more off

* Destring adv number and use it as a unique identificator
destring fund_id, gen(adv_id)
format adv_id %32.0f
drop fund_id

*Generate different time frames
gen date = ym(year,month)
gen timed = dofm(date) 
gen timeq = qofd(timed)
gen timeh = hofd(timed)
gen timea = yofd(timed)
format timeq %tq
format timeh %th
format timea %ty
drop timed

****************************************************
*SET TIME PERIOD FOR ANALYSIS                      *
****************************************************
****************************************************
*FOR quarter timeq, for annual timea
*FOR quater =3, for annual = 12
****************************************************


gen time=timeh
gen cutmonth=6


****************************************************



* Calculate age
gen age=monthnum-incep

*Change return name
gen mnth_rtrn=ret
gen mnth_exret=exret
gen mnth_gret=gret
drop ret
drop exret
drop gret

*Calculate log returns and alphas
gen log_ret=ln(1+mnth_rtrn/100)
gen log_gret=ln(1+mnth_gret/100)
gen log_exret=ln(1+mnth_exret/100)

gen log_capm=ln(1+exante_a_capm/100)
gen log_ff3=ln(1+exante_a_ff3/100)
gen log_fh7=ln(1+exante_a_fh7/100)


*Calculate returns, alphas, st.dev and Sharpe

*net return
bysort adv_id time: gen count=_n 
bysort adv_id time: egen mcount=max(count) 
egen ret_sum=sum(log_ret) if mcount==cutmonth, by (adv_id time)
gen rtrn=(exp(ret_sum)-1)*100
drop ret_sum
drop count
drop mcount

*gross returns
bysort adv_id time: gen count=_n 
bysort adv_id time: egen mcount=max(count) 
egen gret_sum=sum(log_gret) if mcount==cutmonth, by (adv_id time)
gen grtrn=(exp(gret_sum)-1)*100
drop gret_sum
drop count
drop mcount

*excess net returns
bysort adv_id time: gen count=_n 
bysort adv_id time: egen mcount=max(count)
egen exret_sum=sum(log_exret) if mcount==cutmonth, by (adv_id time)
gen exret=(exp(exret_sum)-1)*100
drop exret_sum
drop count
drop mcount

bysort adv_id time: gen count=_n 
bysort adv_id time: egen mcount=max(count)
egen exante_capm_sum=sum(log_capm) if mcount==cutmonth, by (adv_id time)
gen exante_capm=(exp(exante_capm_sum)-1)*100
drop exante_capm_sum
drop count
drop mcount

bysort adv_id time: gen count=_n 
bysort adv_id time: egen mcount=max(count)
egen exante_ff3_sum=sum(log_ff3) if mcount==cutmonth, by (adv_id time)
gen exante_ff3=(exp(exante_ff3_sum)-1)*100
drop exante_ff3_sum
drop count
drop mcount

bysort adv_id time: gen count=_n 
bysort adv_id time: egen mcount=max(count)
egen exante_fh7_sum=sum(log_fh7) if mcount==cutmonth, by (adv_id time)
gen exante_fh7=(exp(exante_fh7_sum)-1)*100
drop exante_fh7_sum
drop count
drop mcount

bysort adv_id time: gen count=_n
bysort adv_id time: egen mcount=max(count)
egen sd_ret=sd(mnth_rtrn), by (adv_id time)
replace sd_ret=. if mcount!=cutmonth
replace sd_ret=sd_ret*sqrt(cutmonth)
drop count
drop mcount

gen Sharpe=exret/sd_ret

bysort adv_id time: gen count=_n
bysort adv_id time: egen mcount=max(count)
egen sd_resid_capm=sd(resid_capm), by (adv_id time)
replace sd_resid_capm=. if mcount!=cutmonth
replace sd_resid_capm=sd_resid_capm*sqrt(cutmonth)
drop count
drop mcount

bysort adv_id time: gen count=_n
bysort adv_id time: egen mcount=max(count)
egen sd_resid_ff3=sd(resid_ff3), by (adv_id time)
replace sd_resid_ff3=. if mcount!=cutmonth
replace sd_resid_ff3=sd_resid_ff3*sqrt(cutmonth)
drop count
drop mcount

bysort adv_id time: gen count=_n
bysort adv_id time: egen mcount=max(count)
egen sd_resid_fh7=sd(resid_capm), by (adv_id time)
replace sd_resid_fh7=. if mcount!=cutmonth
replace sd_resid_fh7=sd_resid_fh7*sqrt(cutmonth)
drop count
drop mcount

gen Sharpe_capm=exante_capm/sd_resid_capm
gen Sharpe_ff3=exante_ff3/sd_resid_ff3
gen Sharpe_fh7=exante_fh7/sd_resid_fh7

*Identify funds that do not change pct ownership
egen sd_pct=sd(pctowned), by (adv_id)

*Identify first period of reporting
sort fund_name monthnum
by fund_name: gen name_count=_n
replace name_count=. if name_count!=1
sort adv_id monthnum

*Set first report counters for quarters, halves of year and years
bysort adv_id time: egen n_count=max(name_count)

*NEW*Average times reported
tab fund_name, matcell(rep)
svmat rep
mean(rep)
drop rep1
*On average funds reported 5.2 times during 2012-1H2015



********* Data transformation


bysort adv_id time: gen count=_n 
drop if count!=cutmonth
drop count
sort adv_id monthnum

*Set regression
tsset adv_id time

*Leave only one period for funds that do not report change. !!!!USED LAGGED OBSERVATION
sort adv_id time
*drop if sd_pct==0 & l.n_count!=1
drop if sd_pct==0

*Desribe panel
*xtsum rtrn a_capm a_ff3 a_fh7 sd_ret Sharpe modepct maxpct pctowned age mfee ifee lev lockup redem hwm 

*Clean outliers

*scatter rtrn sd_ret 
*scatter Sharpe monthnum
*drop if sd_ret==0
drop if age<0

gen size=ln(aum)
drop if size<0
gen lag_size=l.size

winsor2 rtrn, cuts(1 99) by(time)
winsor2 sd_ret, cuts(1 99) by(time)
winsor2 Sharpe, cuts(1 99) by(time)
winsor2 exante_capm, cuts(1 99) by(time)
winsor2 exante_ff3, cuts(1 99) by(time)
winsor2 exante_fh7, cuts(1 99) by(time)
winsor2 sd_resid_capm, cuts(1 99) by(time)
winsor2 sd_resid_ff3, cuts(1 99) by(time)
winsor2 sd_resid_fh7, cuts(1 99) by(time)
winsor2 Sharpe_capm, cuts(1 99) by(time)
winsor2 Sharpe_ff3, cuts(1 99) by(time)
winsor2 Sharpe_fh7, cuts(1 99) by(time)

*winsor2 gret, cuts(1 99) by(time)

*Create lagged variables
gen lag_pct=l.pctowned
gen chg_pct=pctowned-lag_pct
gen lag_sd_ret_w=l.sd_ret_w
gen chg_sd_ret_w=sd_ret_w-lag_sd_ret_w
gen lag_rtrn_w=l.rtrn_w
gen chg_rtrn_w=rtrn_w-lag_rtrn_w
*egen pctownedcat = cut(pctowned), at(0,15,85,101)
sort lag_pct
*sort pctownedcat
tab style_master, gen(s)
*if gret<rtrn replace gret=rtrn
*sort exante_capm_w
gen lag_exante_capm_w=l.exante_capm_w
*sort exante_ff3_w
gen lag_exante_ff3_w=l.exante_ff3_w
*sort exante_fh7_w
gen lag_exante_fh7_w=l.exante_fh7_w

*Calculate maxpct and modepct
egen maxpct=max(pctowned), by (adv_id time)
egen modepct=mode(pctowned), by (adv_id time)

*Piecewise linear constraint
sort time
foreach var of varlist lag_pct pctowned {
egen rank`var'=rank(`var')
egen count`var'=count(`var')
gen lfs`var'=rank`var'/count`var'
gen rank1`var'=0
gen rank2`var'=0
gen rank3`var'=0
replace rank1`var'=lfs`var' if lfs`var'<=0.2
replace rank1`var'=0.2 if lfs`var'>0.2
replace rank2`var'=lfs`var'-0.2 if lfs`var'<=0.8 & lfs`var'>0.2
replace rank2`var'=0.6 if lfs`var'>0.8
replace rank3`var'=lfs`var'-0.8 if lfs`var'>0.8
}

xtsum rtrn_w exante_capm_w exante_ff3_w exante_fh7_w sd_ret_w sd_resid_capm_w sd_resid_ff3_w sd_resid_fh7_w Sharpe_w modepct maxpct pctowned age mfee ifee lev lockup redem hwm size


* Univariate Sort _ Performance and Risk
forval i=105/110 {

*egen lag_pct`i' = cut(lag_pct), at(0,21,41,61,81,101) icodes, if timeh == `i'
*egen lag_pct`i' if timeh == `i'
*sort lag_pct`i'
*tabstat rtrn_w, s(mean median sd var count range min max) by(lag_pct`i')
*tabstat sd_ret_w, s(mean median sd var count range min max) by(lag_pct`i')
graph twoway (scatter rtrn_w lag_pct) (qfit rtrn_w lag_pct) (lowess rtrn_w lag_pct) 
graph save rtrn_w_lag_pct`i'
graph twoway (scatter sd_ret_w lag_pct) (qfit sd_ret_w lag_pct) (lowess sd_ret_w lag_pct) 
graph save sd_ret_w_lag_pct`i'
graph twoway (scatter Sharpe_w lag_pct) (qfit Sharpe_w lag_pct) (lowess Sharpe_w lag_pct) 
graph save Sharpe_w_lag_pct`i'
}

forval i=105/110 {
egen lag_pct`i' = cut(lag_pct), at(0,21,41,61,81,101) icodes, if time == `i'
sort lag_pct`i'
tabstat rtrn_w, s(mean) by(lag_pct`i')
tabstat sd_ret_w, s(mean) by(lag_pct`i')
tabstat Sharpe_w, s(mean) by(lag_pct`i')
}

forvalues i=105/110 {
sort lag_pct`i'
local t : label origin `i'
display "`t'
graph twoway (scatter rtrn_w lag_pct`i') (qfit rtrn_w lag_pct`i') (lowess rtrn_w lag_pct`i')
graph save graph`t' 
}


sort pctowned
sort size
gen capital_at_risk = pctowned * size
sort capital_at_risk
*gen lag_capital_at_risk = l.capital_at_risk
gen lagpct_sq = lag_pct * lag_pct

*reg sd_ret lag_pct lag_sd_ret hwm size s1-s12 lockup ifee mfee age
*reg Sharpe lag_pct lag_sd_ret hwm size s1-s12 lockup ifee mfee age
*gen lag_rtrn_w = l.rtrn_w
*gen lag_sd_ret_w = l.lag_sd_ret_w

global xlist lag_sd_ret_w invdelta size age mfee lev lockup redem hwm s1-s12

* Compare Linear OLS, FM Full history for Performance, Risk, Alpha and Sharpe

quietly regress rtrn_w lag_pct lagpct_sq lag_rtrn_w $xlist, cluster(adv_id)
eststo i1, title(OLS_rtrn)
quietly xtfmb rtrn_w lag_pct lagpct_sq lag_rtrn_w $xlist
eststo i2, title(FM_rtrn)

quietly regress sd_ret_w lag_pct lagpct_sq lag_rtrn_w $xlist, cluster(adv_id)
eststo i3, title(OLS_Risk)
quietly xtfmb sd_ret_w lag_pct lagpct_sq lag_rtrn_w $xlist
eststo i4, title(FM_Risk)


quietly regress exante_capm_w lag_pct lagpct_sq sd_resid_capm_w $xlist, cluster(adv_id)  
eststo i5, title(OLS_capm)
quietly xtfmb exante_capm_w lag_pct lagpct_sq sd_resid_capm_w $xlist
eststo i6, title(FM_capm)

quietly regress exante_ff3_w lag_pct lagpct_sq sd_resid_ff3_w $xlist, cluster(adv_id)  
eststo i7, title(OLS_ff3)
quietly xtfmb exante_ff3_w  lag_pct lagpct_sq sd_resid_ff3_w $xlist
eststo i8, title(FM_ff3)

quietly regress exante_fh7_w lag_pct lagpct_sq sd_resid_fh7_w $xlist, cluster(adv_id)  
eststo i9, title(OLS_fh7)
quietly xtfmb exante_fh7_w  lag_pct lagpct_sq sd_resid_fh7_w $xlist
eststo i10, title(FM_fh7)

quietly regress Sharpe_w lag_pct lagpct_sq $xlist, cluster(adv_id) 
eststo i11, title(OLS_Sharpe)
quietly xtfmb Sharpe_w lag_pct lagpct_sq $xlist
eststo i12, title(FM_Sharpe)

estout i1 i2 i3 i4 i5 i6 i7 i8, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC)) starlevels(* 0.10 ** 0.05 *** 0.01)
estout i9 i10 i11 i12, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC)) starlevels(* 0.10 ** 0.05 *** 0.01)

* Compare NL OLS, FM Full history for Performance and Risk 
* pctown high, medium, low
mkspline ownlg1 20 ownlg2 80 ownlg3 = lag_pct
xtsum ownlg1 ownlg2 ownlg3
mkspline ownlg1sq 20 ownlg2sq 80 ownlg3sq = lagpct_sq
xtsum ownlg1sq ownlg2sq ownlg3sq

%local xlist2 ownlg1 ownlg2 ownlg3 ownlg1sq ownlg2sq ownlg3sq

quietly regress rtrn_w ownlg1 ownlg2 ownlg3 $xlist
eststo m1, title(OLS_rtrn)
quietly xtfmb rtrn_w ownlg1 ownlg2 ownlg3 $xlist
eststo m2, title(FM_rtrn)
quietly regress a_capm_w ownlg1 ownlg2 ownlg3 $xlist  
eststo m3, title(OLS_a_capm)
quietly xtfmb a_capm_w ownlg1 ownlg2 ownlg3 $xlist
eststo m4, title(FM_a_capm)
quietly regress a_ff3_w ownlg1 ownlg2 ownlg3 $xlist  
eststo m5, title(OLS_a_ff3)
quietly xtfmb a_ff3_w ownlg1 ownlg2 ownlg3 $xlist 
eststo m6, title(FM_a_ff3)
quietly regress a_fh7_w ownlg1 ownlg2 ownlg3 $xlist  
eststo m7, title(OLS_a_fh7)
quietly xtfmb a_fh7_w ownlg1 ownlg2 ownlg3 $xlist 
eststo m8, title(FM_a_fh7) 

quietly regress sd_ret_w ownlg1 ownlg2 ownlg3 $xlist 
eststo m9, title(OLS_Risk)
quietly xtfmb sd_ret_w ownlg1 ownlg2 ownlg3 $xlist 
eststo m10, title(FM_Risk)

quietly regress Sharpe_w ownlg1 ownlg2 ownlg3 $xlist 
eststo m11, title(OLS_Sharpe)
quietly xtfmb Sharpe_w ownlg1 ownlg2 ownlg3 $xlist 
eststo m12, title(FM_Sharpe)

estout m1 m2 m3 m4 m5 m6 m7 m8, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC)) starlevels(* 0.10 ** 0.05 *** 0.01)
estout m9 m10 m11 m12, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC)) starlevels(* 0.10 ** 0.05 *** 0.01)
*Age High
quietly regress rtrn_w ownlg1 ownlg2 ownlg3 $xlist if age>128
eststo l1, title(OLS_rtrn)
quietly xtfmb rtrn_w ownlg1 ownlg2 ownlg3 $xlist if age>128
eststo l2, title(FM_rtrn)
quietly regress a_capm_w ownlg1 ownlg2 ownlg3 $xlist if age>128
eststo l3, title(OLS_a_capm)
quietly xtfmb a_capm_w ownlg1 ownlg2 ownlg3 $xlist if age>128
eststo l4, title(FM_a_capm)
quietly regress a_ff3_w ownlg1 ownlg2 ownlg3 $xlist if age>128
eststo l5, title(OLS_a_ff3)
quietly xtfmb a_ff3_w ownlg1 ownlg2 ownlg3 $xlist if age>128
eststo l6, title(FM_a_ff3)
quietly regress a_fh7_w ownlg1 ownlg2 ownlg3 $xlist if age>128
eststo l7, title(OLS_a_fh7)
quietly xtfmb a_fh7_w ownlg1 ownlg2 ownlg3 $xlist if age>128
eststo l8, title(FM_a_fh7) 

quietly regress sd_ret_w ownlg1 ownlg2 ownlg3 $xlist if age>128
eststo l9, title(OLS_Risk)
quietly xtfmb sd_ret_w ownlg1 ownlg2 ownlg3 $xlist if age>128
eststo l10, title(FM_Risk)

quietly regress Sharpe_w ownlg1 ownlg2 ownlg3 $xlist if age>128
eststo l11, title(OLS_Sharpe)
quietly xtfmb Sharpe_w ownlg1 ownlg2 ownlg3 $xlist if age>128
eststo l12, title(FM_Sharpe)

estout l1 l2 l3 l4 l5 l6 l7 l8, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))
estout l9 l10 l11 l12, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))

*Age low
quietly regress rtrn_w ownlg1 ownlg2 ownlg3 $xlist if age<=128
eststo k1, title(OLS_rtrn)
quietly xtfmb rtrn_w ownlg1 ownlg2 ownlg3 $xlist if age<=128
eststo k2, title(FM_rtrn)
quietly regress a_capm_w ownlg1 ownlg2 ownlg3 $xlist if age<=128
eststo k3, title(OLS_a_capm)
quietly xtfmb a_capm_w ownlg1 ownlg2 ownlg3 $xlist if age<=128
eststo k4, title(FM_a_capm)
quietly regress a_ff3_w ownlg1 ownlg2 ownlg3 $xlist if age<=128
eststo k5, title(OLS_a_ff3)
quietly xtfmb a_ff3_w ownlg1 ownlg2 ownlg3 $xlist if age<=128
eststo k6, title(FM_a_ff3)
quietly regress a_fh7_w ownlg1 ownlg2 ownlg3 $xlist if age<=128
eststo k7, title(OLS_a_fh7)
quietly xtfmb a_fh7_w ownlg1 ownlg2 ownlg3 $xlist if age<=128
eststo k8, title(FM_a_fh7) 

quietly regress sd_ret_w ownlg1 ownlg2 ownlg3 $xlist if age<=128
eststo k9, title(OLS_Risk)
quietly xtfmb sd_ret_w ownlg1 ownlg2 ownlg3 $xlist if age<=128
eststo k10, title(FM_Risk)

quietly regress Sharpe_w ownlg1 ownlg2 ownlg3 $xlist if age<=128
eststo k11, title(OLS_Sharpe)
quietly xtfmb Sharpe_w ownlg1 ownlg2 ownlg3 $xlist if age<=128
eststo k12, title(FM_Sharpe)

estout k1 k2 k3 k4 k5 k6 k7 k8, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))
estout k9 k10 k11 k12, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))

* Size Big
quietly regress rtrn_w ownlg1 ownlg2 ownlg3 $xlist if size>4.466
eststo n1, title(OLS_rtrn)
quietly xtfmb rtrn_w ownlg1 ownlg2 ownlg3 $xlist if size>4.466
eststo n2, title(FM_rtrn)
quietly regress a_capm_w ownlg1 ownlg2 ownlg3 $xlist if size>4.466
eststo n3, title(OLS_a_capm)
quietly xtfmb a_capm_w ownlg1 ownlg2 ownlg3 $xlist if size>4.466
eststo n4, title(FM_a_capm)
quietly regress a_ff3_w ownlg1 ownlg2 ownlg3 $xlist if size>4.466
eststo n5, title(OLS_a_ff3)
quietly xtfmb a_ff3_w ownlg1 ownlg2 ownlg3 $xlist if size>4.466
eststo n6, title(FM_a_ff3)
quietly regress a_fh7_w ownlg1 ownlg2 ownlg3 $xlist if size>4.466
eststo n7, title(OLS_a_fh7)
quietly xtfmb a_fh7_w ownlg1 ownlg2 ownlg3 $xlist if size>4.466
eststo n8, title(FM_a_fh7) 

quietly regress sd_ret_w ownlg1 ownlg2 ownlg3 $xlist if size>4.466
eststo n9, title(OLS_Risk)
quietly xtfmb sd_ret_w ownlg1 ownlg2 ownlg3 $xlist if size>4.466
eststo n10, title(FM_Risk)

quietly regress Sharpe_w ownlg1 ownlg2 ownlg3 $xlist if size>4.466
eststo n11, title(OLS_Sharpe)
quietly xtfmb Sharpe_w ownlg1 ownlg2 ownlg3 $xlist if size>4.466
eststo n12, title(FM_Sharpe)

estout n1 n2 n3 n4 n5 n6 n7 n8, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))
estout n9 n10 n11 n12, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))

*Size small
quietly regress rtrn_w ownlg1 ownlg2 ownlg3 $xlist if size<=4.466
eststo o1, title(OLS_rtrn)
quietly xtfmb rtrn_w ownlg1 ownlg2 ownlg3 $xlist if size<=4.466
eststo o2, title(FM_rtrn)
quietly regress a_capm_w ownlg1 ownlg2 ownlg3 $xlist if size<=4.466
eststo o3, title(OLS_a_capm)
quietly xtfmb a_capm_w ownlg1 ownlg2 ownlg3 $xlist if size<=4.466
eststo o4, title(FM_a_capm)
quietly regress a_ff3_w ownlg1 ownlg2 ownlg3 $xlist if size<=4.466
eststo o5, title(OLS_a_ff3)
quietly xtfmb a_ff3_w ownlg1 ownlg2 ownlg3 $xlist if size<=4.466
eststo o6, title(FM_a_ff3)
quietly regress a_fh7_w ownlg1 ownlg2 ownlg3 $xlist if size<=4.466
eststo o7, title(OLS_a_fh7)
quietly xtfmb a_fh7_w ownlg1 ownlg2 ownlg3 $xlist if size<=4.466
eststo o8, title(FM_a_fh7) 

quietly regress sd_ret_w ownlg1 ownlg2 ownlg3 $xlist if size<=4.466
eststo o9, title(OLS_Risk)
quietly xtfmb sd_ret_w ownlg1 ownlg2 ownlg3 $xlist if size<=4.466
eststo o10, title(FM_Risk)

quietly regress Sharpe_w ownlg1 ownlg2 ownlg3 $xlist if size<=4.466
eststo o11, title(OLS_Sharpe)
quietly xtfmb Sharpe_w ownlg1 ownlg2 ownlg3 $xlist if size<=4.466
eststo o12, title(FM_Sharpe)

estout o1 o2 o3 o4 o5 o6 o7 o8, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))
estout o9 o10 o11 o12, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))

* Compare NL OLS, FM Full history for Performance and Risk 
* pctown top and bottom half 

mkspline pctownlag1 50 pctownlag2 = lag_pct, marginal
xtsum pctownlag1 pctownlag2

quietly regress rtrn_w pctownlag1 pctownlag2 $xlist
eststo p1, title(OLS_rtrn)
quietly xtfmb rtrn_w pctownlag1 pctownlag2 $xlist
eststo p2, title(FM_rtrn)
quietly regress a_capm_w pctownlag1 pctownlag2 $xlist  
eststo p3, title(OLS_a_capm)
quietly xtfmb a_capm_w pctownlag1 pctownlag2 $xlist
eststo p4, title(FM_a_capm)
quietly regress a_ff3_w pctownlag1 pctownlag2 $xlist  
eststo p5, title(OLS_a_ff3)
quietly xtfmb a_ff3_w pctownlag1 pctownlag2 $xlist 
eststo p6, title(FM_a_ff3)
quietly regress a_fh7_w pctownlag1 pctownlag2 $xlist  
eststo p7, title(OLS_a_fh7)
quietly xtfmb a_fh7_w pctownlag1 pctownlag2 $xlist 
eststo p8, title(FM_a_fh7) 

quietly regress sd_ret_w pctownlag1 pctownlag2 $xlist 
eststo p9, title(OLS_Risk)
quietly xtfmb sd_ret_w pctownlag1 pctownlag2 $xlist 
eststo p10, title(FM_Risk)

quietly regress Sharpe_w pctownlag1 pctownlag2 $xlist 
eststo p11, title(OLS_Sharpe)
quietly xtfmb Sharpe_w pctownlag1 pctownlag2 $xlist 
eststo p12, title(FM_Sharpe)

estout p1 p2 p3 p4 p5 p6 p7 p8, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))
estout p9 p10 p11 p12, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))

* Compare NL OLS, FM Full history for Performance and Risk 
* pctown top and bottom half pctile
mkspline pctlag 2 = lag_pct, pctile displayknots
*mkspline pctownlag1 50 pctownlag2 = lag_pct, marginal
*xtsum pctownlag1 pctownlag2

quietly regress rtrn_w pctlag1 pctlag2 $xlist
eststo r1, title(OLS_rtrn)
quietly xtfmb rtrn_w pctlag1 pctlag2 $xlist
eststo r2, title(FM_rtrn)
quietly regress a_capm_w pctlag1 pctlag2 $xlist  
eststo r3, title(OLS_a_capm)
quietly xtfmb a_capm_w pctlag1 pctlag2 $xlist
eststo r4, title(FM_a_capm)
quietly regress a_ff3_w pctlag1 pctlag2 $xlist  
eststo r5, title(OLS_a_ff3)
quietly xtfmb a_ff3_w pctlag1 pctlag2 $xlist 
eststo r6, title(FM_a_ff3)
quietly regress a_fh7_w pctlag1 pctlag2 $xlist  
eststo r7, title(OLS_a_fh7)
quietly xtfmb a_fh7_w pctlag1 pctlag2 $xlist 
eststo r8, title(FM_a_fh7) 

quietly regress sd_ret_w pctlag1 pctlag2 $xlist 
eststo r9, title(OLS_Risk)
quietly xtfmb sd_ret_w pctlag1 pctlag2 $xlist 
eststo r10, title(FM_Risk)

quietly regress Sharpe_w pctlag1 pctlag2 $xlist 
eststo r11, title(OLS_Sharpe)
quietly xtfmb Sharpe_w pctlag1 pctlag2 $xlist 
eststo r12, title(FM_Sharpe)

estout r1 r2 r3 r4 r5 r6 r7 r8, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))
estout r9 r10 r11 r12, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))

*Compare NL OLS, FM Full history for Performance and Risk 
* Age low high
mkspline pctage 2 = age, pctile displayknots
*global xlist1 lag_rtrn_w lag_sd_ret_w lag_size mfee ifee lev lockup redem hwm s1-s12

quietly regress rtrn_w pctlag1 pctlag2 $xlist if age >128
eststo q1, title(OLS_rtrn)
quietly xtfmb rtrn_w pctlag1 pctlag2 $xlist if age >128
eststo q2, title(FM_rtrn)
quietly regress a_capm_w pctlag1 pctlag2 $xlist if age >128
eststo q3, title(OLS_a_capm)
quietly xtfmb a_capm_w pctlag1 pctlag2 $xlist if age >128
eststo q4, title(FM_a_capm)
quietly regress a_ff3_w pctlag1 pctlag2 $xlist if age >128
eststo q5, title(OLS_a_ff3)
quietly xtfmb a_ff3_w pctlag1 pctlag2 $xlist if age >128
eststo q6, title(FM_a_ff3)
quietly regress a_fh7_w pctlag1 pctlag2 $xlist if age >128
eststo q7, title(OLS_a_fh7)
quietly xtfmb a_fh7_w pctlag1 pctlag2 $xlist if age >128
eststo q8, title(FM_a_fh7) 

quietly regress sd_ret_w pctlag1 pctlag2 $xlist if age >128
eststo q9, title(OLS_Risk)
quietly xtfmb sd_ret_w pctlag1 pctlag2 $xlist if age >128
eststo q10, title(FM_Risk)

quietly regress Sharpe_w pctlag1 pctlag2 $xlist if age >128
eststo q11, title(OLS_Sharpe)
quietly xtfmb Sharpe_w pctlag1 pctlag2 $xlist if age >128
eststo q12, title(FM_Sharpe)

estout q1 q2 q3 q4 q5 q6 q7 q8, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))
estout q9 q10 q11 q12, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))

*Age lower than median
quietly regress rtrn_w pctlag1 pctlag2 $xlist if age <=128
eststo s1, title(OLS_rtrn)
quietly xtfmb rtrn_w pctlag1 pctlag2 $xlist if age <=128
eststo s2, title(FM_rtrn)
quietly regress a_capm_w pctlag1 pctlag2 $xlist if age <=128
eststo s3, title(OLS_a_capm)
quietly xtfmb a_capm_w pctlag1 pctlag2 $xlist if age <=128
eststo s4, title(FM_a_capm)
quietly regress a_ff3_w pctlag1 pctlag2 $xlist if age <=128
eststo s5, title(OLS_a_ff3)
quietly xtfmb a_ff3_w pctlag1 pctlag2 $xlist if age <=128
eststo s6, title(FM_a_ff3)
quietly regress a_fh7_w pctlag1 pctlag2 $xlist if age <=128
eststo s7, title(OLS_a_fh7)
quietly xtfmb a_fh7_w pctlag1 pctlag2 $xlist if age <=128
eststo s8, title(FM_a_fh7) 

quietly regress sd_ret_w pctlag1 pctlag2 $xlist if age <=128
eststo s9, title(OLS_Risk)
quietly xtfmb sd_ret_w pctlag1 pctlag2 $xlist if age <=128
eststo s10, title(FM_Risk)

quietly regress Sharpe_w pctlag1 pctlag2 $xlist if age <=128
eststo s11, title(OLS_Sharpe)
quietly xtfmb Sharpe_w pctlag1 pctlag2 $xlist if age <=128
eststo s12, title(FM_Sharpe)

estout s1 s2 s3 s4 s5 s6 s7 s8, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))
estout s9 s10 s11 s12, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))


*Size Big
quietly regress rtrn_w pctlag1 pctlag2 $xlist if size>4.466
eststo a1, title(OLS_rtrn)
quietly xtfmb rtrn_w pctlag1 pctlag2 $xlist if size>4.466
eststo a2, title(FM_rtrn)
quietly regress a_capm_w pctlag1 pctlag2 $xlist if size>4.466
eststo a3, title(OLS_a_capm)
quietly xtfmb a_capm_w pctlag1 pctlag2 $xlist if size>4.466
eststo a4, title(FM_a_capm)
quietly regress a_ff3_w pctlag1 pctlag2 $xlist if size>4.466
eststo a5, title(OLS_a_ff3)
quietly xtfmb a_ff3_w pctlag1 pctlag2 $xlist if size>4.466
eststo a6, title(FM_a_ff3)
quietly regress a_fh7_w pctlag1 pctlag2 $xlist if size>4.466
eststo a7, title(OLS_a_fh7)
quietly xtfmb a_fh7_w pctlag1 pctlag2 $xlist if size>4.466
eststo a8, title(FM_a_fh7) 

quietly regress sd_ret_w pctlag1 pctlag2 $xlist if size>4.466
eststo a9, title(OLS_Risk)
quietly xtfmb sd_ret_w pctlag1 pctlag2 $xlist if size>4.466
eststo a10, title(FM_Risk)

quietly regress Sharpe_w pctlag1 pctlag2 $xlist if size>4.466
eststo a11, title(OLS_Sharpe)
quietly xtfmb Sharpe_w pctlag1 pctlag2 $xlist if size>4.466
eststo a12, title(FM_Sharpe)

estout a1 a2 a3 a4 a5 a6 a7 a8, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))
estout a9 a10 a11 a12, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))

*Size smaller than median
quietly regress rtrn_w pctlag1 pctlag2 $xlist if size<=4.466
eststo b1, title(OLS_rtrn)
quietly xtfmb rtrn_w pctlag1 pctlag2 $xlist if size<=4.466
eststo b2, title(FM_rtrn)
quietly regress a_capm_w pctlag1 pctlag2 $xlist if size<=4.466
eststo b3, title(OLS_a_capm)
quietly xtfmb a_capm_w pctlag1 pctlag2 $xlist if size<=4.466
eststo b4, title(FM_a_capm)
quietly regress a_ff3_w pctlag1 pctlag2 $xlist if size<=4.466
eststo b5, title(OLS_a_ff3)
quietly xtfmb a_ff3_w pctlag1 pctlag2 $xlist if size<=4.466
eststo b6, title(FM_a_ff3)
quietly regress a_fh7_w pctlag1 pctlag2 $xlist if size<=4.466
eststo b7, title(OLS_a_fh7)
quietly xtfmb a_fh7_w pctlag1 pctlag2 $xlist if size<=4.466
eststo b8, title(FM_a_fh7) 

quietly regress sd_ret_w pctlag1 pctlag2 $xlist if size<=4.466
eststo b9, title(OLS_Risk)
quietly xtfmb sd_ret_w pctlag1 pctlag2 $xlist if size<=4.466
eststo b10, title(FM_Risk)

quietly regress Sharpe_w pctlag1 pctlag2 $xlist if size<=4.466
eststo b11, title(OLS_Sharpe)
quietly xtfmb Sharpe_w pctlag1 pctlag2 $xlist if size<=4.466
eststo b12, title(FM_Sharpe)

estout b1 b2 b3 b4 b5 b6 b7 b8, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))
estout b9 b10 b11 b12, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))

* Age Size Interaction
gen age_size_interaction = age * size

global xlist1 lag_rtrn_w lag_sd_ret_w mfee ifee lev lockup redem hwm s1-s12

quietly regress rtrn_w ownlg1 ownlg2 ownlg3 age_size_interaction $xlist1
eststo c1, title(OLS_rtrn)
quietly xtfmb rtrn_w ownlg1 ownlg2 ownlg3 age_size_interaction $xlist1
eststo c2, title(FM_rtrn)
quietly regress a_capm_w ownlg1 ownlg2 ownlg3 age_size_interaction $xlist1  
eststo c3, title(OLS_a_capm)
quietly xtfmb a_capm_w ownlg1 ownlg2 ownlg3 age_size_interaction $xlist1
eststo c4, title(FM_a_capm)
quietly regress a_ff3_w ownlg1 ownlg2 ownlg3 age_size_interaction $xlist1  
eststo c5, title(OLS_a_ff3)
quietly xtfmb a_ff3_w ownlg1 ownlg2 ownlg3 age_size_interaction $xlist1 
eststo c6, title(FM_a_ff3)
quietly regress a_fh7_w ownlg1 ownlg2 ownlg3  age_size_interaction $xlist1  
eststo c7, title(OLS_a_fh7)
quietly xtfmb a_fh7_w ownlg1 ownlg2 ownlg3 age_size_interaction $xlist1 
eststo c8, title(FM_a_fh7) 

quietly regress sd_ret_w ownlg1 ownlg2 ownlg3 age_size_interaction $xlist1 
eststo c9, title(OLS_Risk)
quietly xtfmb sd_ret_w ownlg1 ownlg2 ownlg3 age_size_interaction $xlist1 
eststo c10, title(FM_Risk)

quietly regress Sharpe_w ownlg1 ownlg2 ownlg3 age_size_interaction $xlist1 
eststo c11, title(OLS_Sharpe)
quietly xtfmb Sharpe_w ownlg1 ownlg2 ownlg3 age_size_interaction $xlist1 
eststo c12, title(FM_Sharpe)

estout c1 c2 c3 c4 c5 c6 c7 c8, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))
estout c9 c10 c11 c12, cells(b(star fmt(3)) se(par fmt(2))) legend label varlabels(_cons constant) stats(r2 df_r bic, fmt(3 0 1) label(R-sqr dfres BIC))
