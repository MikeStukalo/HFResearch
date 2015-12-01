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
drop ret
drop exret

*Calculate log returns and alphas
gen log_ret=ln(1+mnth_rtrn/100)
gen log_exret=ln(1+mnth_exret/100)
gen log_capm=ln(1+mnth_capm/100)
gen log_ff3=ln(1+mnth_ff3/100)
gen log_fh7=ln(1+mnth_fh7/100)


*Calculate returns, alphas, st.dev and Sharpe

bysort adv_id time: gen count=_n 
bysort adv_id time: egen mcount=max(count) 
egen ret_sum=sum(log_ret) if mcount==cutmonth, by (adv_id time)
gen rtrn=(exp(ret_sum)-1)*100
drop ret_sum
drop count
drop mcount


bysort adv_id time: gen count=_n 
bysort adv_id time: egen mcount=max(count)
egen exret_sum=sum(log_exret) if mcount==cutmonth, by (adv_id time)
gen exret=(exp(exret_sum)-1)*100
drop exret_sum
drop count
drop mcount

bysort adv_id time: gen count=_n 
bysort adv_id time: egen mcount=max(count)
egen capm_sum=sum(log_capm) if mcount==cutmonth, by (adv_id time)
gen a_capm=(exp(capm_sum)-1)*100
drop capm_sum
drop count
drop mcount

bysort adv_id time: gen count=_n 
bysort adv_id time: egen mcount=max(count)
egen ff3_sum=sum(log_ff3) if mcount==cutmonth, by (adv_id time)
gen a_ff3=(exp(ff3_sum)-1)*100
drop ff3_sum
drop count
drop mcount

bysort adv_id time: gen count=_n 
bysort adv_id time: egen mcount=max(count)
egen fh7_sum=sum(log_fh7) if mcount==cutmonth, by (adv_id time)
gen a_fh7=(exp(fh7_sum)-1)*100
drop fh7_sum
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

*Create lagged variables
gen lag_pct=l.pctowned
gen chg_pct=pctowned-lag_pct
gen lag_sd_ret=l.sd_ret
gen chg_sd_ret=sd_ret-lag_sd_ret
gen lag_rtrn=l.rtrn
gen chg_rtrn=rtrn-lag_rtrn
egen pctownedcat = cut(pctowned), at(0,15,85,101)
sort lag_pct
sort pctownedcat
tab style_master, gen(s)

*Calculate maxpct and modepct
egen maxpct=max(pctowned), by (adv_id time)
egen modepct=mode(pctowned), by (adv_id time)

*Leave only one period for funds that do not report change. !!!!USED LAGGED OBSERVATION
sort adv_id time
drop if sd_pct==0 & l.n_count!=1


*Desribe panel
xtsum rtrn a_capm a_ff3 a_fh7 sd_ret Sharpe modepct maxpct pctowned age mfee ifee lev lockup redem hwm 

*Clean outliers

scatter rtrn sd_ret 
scatter Sharpe monthnum
drop if sd_ret==0
drop if age<0

xtsum rtrn a_capm a_ff3 a_fh7 sd_ret Sharpe modepct maxpct pctowned age mfee ifee lev lockup redem hwm 


*Simple regressions

gen size=ln(aum)
reg sd_ret lag_pct lag_sd_ret hwm size s1-s12 lockup ifee mfee age
reg Sharpe lag_pct lag_sd_ret hwm size s1-s12 lockup ifee mfee age

