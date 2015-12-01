*****************************************************
Compute HF Monthly Delta, Vega
Written by Kevin Mullally, April 2014
Editted by Mikhail Stukalo, November 2014

This program requires the following variabes:
1) Returns (ret_d)
2) Assets Under Management (aum)
3) High Water Mark (highwater)
4) Hurdle Rate (hurdrate)
5) Offshore Dummy (offsh)
6) Incentive fee (incfee)
7) LIBOR rates (taken from Fed Website) 
8) Year/Month variables	(year/month)
9) Fund Identifier (easyid)
*****************************************************;

* Define libraries;
* Library HF contains the base data;

libname raw "C:\Users\Mikhail\Desktop\Funds\Data";
libname d "C:\Users\Mikhail\Desktop\Funds\Delta";
libname l "C:\Users\Mikhail\Desktop\Funds\Rates";


**************************************************************************
First we create returns and contracts databases with matched funds
**************************************************************************;
proc sort data=raw.additional_data out=union_contracts nodupkey; by Fund_id un_id; quit;
proc sort data=union_contracts nodupkey; by Fund_id; quit;

data returns_short; set raw.union_returns; run;
data returns_original; set raw.union_returns_orig (keep=id monthnum ret aum);run;
data returns_original; set returns_original; aum=aum/1000000; run;


* Keep original returns only for matched funds;
proc sql;
create table returns_1
as select distinct a.*, b.un_id
from returns_original as a left join union_contracts as b
on a.id=b.un_id;
quit;

data returns_1; set returns_1; where un_id; run;


proc sql;
create table returns_2
as select distinct a.*, b.id, b.un_id
from returns_short as a left join union_contracts as b
on a.id=b.id;
quit;

data returns_2; set returns_2; where un_id; run;

* add monthnum;
data returns_2; set returns_2; monthnum = 12*(year(date)-1980)+(month(date)-1); run;

* We check if first return is equal to those in another database;
proc sort data=returns_2; by un_id monthnum;quit;
data returns_first; set returns_2; if ret=. then delete;run;
data returns_first;
set returns_first;
by un_id;
first_obs=FIRST.un_id;
run;
data returns_first;
set returns_first;
where first_obs=1;
drop first_obs;
run;

proc sql;
create table NEWvsOld
as select distinct a.*, b.ret as old_ret, b.un_id
from returns_first as a left join returns_1 as b
on a.un_id=b.un_id and a.monthnum=b.monthnum;
quit;

data new_map;
set newvsold;
where abs(ret-old_ret)<0.02;
run;

*Combine old and new;
proc sql;
create table returns_1_1
as select a.monthnum, a.ret, a.aum, a.un_id, b.un_id, b.id
from returns_1 as a left join new_map as b
on a.un_id=b.un_id;
quit;

data returns_1_1; set returns_1_1; where id; run;

proc sql;
create table returns_2_1
as select a.monthnum, a.ret, a.aum, b.un_id, a.id, b.id
from returns_2 as a left join new_map as b
on a.id=b.id;
quit;

data returns; set returns_1_1 returns_2_1; run;
proc sort data=returns nodupkey; by id monthnum; quit;

*add year and month;
data union_returns; set returns; 
date='01Jan1980'd;
retdate= intnx('month',date,monthnum);
year=year(retdate);
month=month(retdate);
drop date;
drop retdate;
run;
 
*Delete working files to free memory;
proc datasets library=work;
   delete newvsold new_map returns returns_1 returns_2 returns_1_1 returns_2_1 returns_first returns_original returns_short;
run;



%let macrodir = C:\Users\Mikhail\Desktop\Funds\Macro\;

options nocenter notes nolabel errors=1 ls=80;
options nomlogic nosymbolgen nodquote nomprint nomacrogen;

* Define macro variables to make code easy to modify;
%let beg_month=1;
%let end_month=12;
%let beg_year=1987;
%let year_before=1986;
%let end_year=2014;
* Data set with return data;
%let data=union_returns;
* Identifying variable;
%let id=id;
* Variables needed;
%let varlist = &id ret_d year month aum highwater hurdrate incfee mgmtfee offsh;
data union_returns; set &data; run;
data union_contracts; set union_contracts; run;
proc sql; create table union_funds as select a.id, a.monthnum, a.ret, a.aum, b.offsh, b.ifee as incfee, b.mfee as mgmtfee, b.hurdrate, b.hwm as highwater 
from union_returns as a left join union_contracts as b on a.id=b.id; quit;
data union_funds; set union_funds; year=int(monthnum/12)+1980; month=mod(monthnum,12)+1; ret_d=ret/100; run;
* Create set to use to manipulate data;
* Only keep relevant variables for faster processing;
data d1;
set union_funds;
where year>=&beg_year;
incfee=incfee/100;
mgmtfee=mgmtfee/100;
if incfee>.5 then incfee=.2;
if mgmtfee>.08 then mgmtfee=.02; 
keep &varlist;
if &id=. or ret_d=. or year=. or month=. then delete;
run;
* Get end of year AUM;
data end_aum;
set d1;
where month=&end_month;
if mgmtfee>.08 then mgmtfee=.02; 
if aum<=0 then delete;
keep &id aum year;
run; 
* Get beginning of year AUM;
data beg_aum;
set d1;
where month=&end_month;
* Adjust the year for diff. calculation (i.e. end of year 1986, becomes beginning of year 1987);
year=year+1;
if aum<=0 then delete;
begyraum=aum;
keep &id year begyraum;
run;
* Merge the sets;
proc sql; create table aum as select * from end_aum as a left join beg_aum as b
on a.&id=b.&id and a.year=b.year; quit;
* Get annual standard deviations, number of returns, average return;
proc sql; create table d2 as select *, count(ret_d) as ctret, std(ret_d) as stdret, mean(ret_d) as avgret
from d1 group by &id, year order by &id, year; quit;
* Clean up datasets to keep memory free;
proc delete data=beg_aum end_aum; run;
* Keep only funds with 12 continuous returns;
data d3; set d2; where ctret=12; drop ctret; run;
proc sort data=d3; by &id year month; run;
* Get buy and hold returns;
%macro lag_rets();
%do i=1 %to 11;
* Lag the returns, 11 lags;
logret_l&i.=lag&i.(logret);
%end;
* Get buy and hold returns;
%do j=1 %to 11;
%let k=%eval(12-&j);
bhret1_&j=(exp(sum(of logret_l&k.-logret_l11))-1);
bhret1_12=(exp(sum(of logret_l1-logret_l11, logret))-1);
%end;
* Get other buy and hold returns;
%do l=2 %to 11;
%let m=%eval(12-&l);
bhret&l._&l.=(exp(logret_l&m.)-1);
%end;
%mend;
data annret; set d3; N=_n_; logret=log(1+ret_d);
* Lag the returns;
%lag_rets();
bhret12_12=(exp(logret)-1);
drop logret_l1-logret_l12;
run;
* Keep only year end observations;
data annret; set annret; where month=12; run;
* Merge datasets to get annual flows;
proc sql;
create table annret1 as select x.*, y.bhret1_1 as ldbhret1_1, y.bhret1_2 as ldbhret1_2, y.bhret1_3 as ldbhret1_3,   y.bhret1_4 as ldbhret1_4, y.bhret1_5 as ldbhret1_5,   
y.bhret1_6 as ldbhret1_6, y.bhret1_7 as ldbhret1_7, y.bhret1_8 as ldbhret1_8, y.bhret1_9 as ldbhret1_9,   y.bhret1_10 as ldbhret1_10, y.bhret1_11 as ldbhret1_11, y.bhret1_12 as ldbhret1_12 
from annret as x left join annret as y on x.&id=y.&id and x.year=(y.year-1);
* Add in the AUM data;
proc sql; create table annret2 as select * from annret1 as a left join aum as b
on a.&id=b.&id and a.year=b.year; quit;
data compdata; set annret2; 
* Get rid of observations with missing variables;
if &id=. or year=. or bhret1_12=. or aum=. or begyraum=. or stdret=. or bhret1_12<-1 or aum<=0 or begyraum<=0 or stdret<=0 then delete;
run;
* Data clean;
proc sql; create table indata as select &id, year, (max(year)-min(year)+1) as countyear, count(year) as sumyear
from compdata group by &id; quit;
data indata1; set indata; 
* Funds with every year's data;
if countyear=sumyear; drop countyear sumyear; run;
data indata2; set indata; 
* Funds with missing data;
if countyear~=sumyear; run;
proc sort data=indata2; by &id year; run;
data indata2; retain delyr; set indata2; lagid=lag(&id); lagyear=lag(year); if (lagyear~=(year-1) and lagid=&id) then flag=1;
if flag=1 then delyr=&id; else if delyr=&id then flag1=1; run;
data indata2; set indata2; if flag=1 or flag1=1 then delete; drop lagid lagyear flag flag1 delyr sumyear countyear; run;
data indata4; *Put together funds with no missing years and initial years of continuous data for funds with missing data later;
set indata1 indata2; run;
* Get full data;
proc sql;
create table indata5 as select * from indata4 as a left join compdata as b
on a.&id=b.&id and a.year=b.year; quit;
proc sql;
create table indata6 as select *, (max(year)-min(year)+1) as countyear, count(year) as sumyear, max(year) as maxyear, min(year) as minyear
from indata5 group by &id; quit;
* Clean up memory;
proc delete data=d1 d2 annret1 annret2 indata indata1 indata2 indata5; run;
data indata6;
set indata6;
* 1) Missing offshore indicator variable: I think we can treat these as onshore;
if offsh=. then offsh=0;
* to take tax outflow into account;
* For offshore, tax=0;
if offsh=1 then tax=0; else tax=0.35;
if offsh=. then tax=.;
* 2) Incentive fees is missing: For missing guys, negative inc fee guys (error in coding) and 
*	more than 100% inc fee guys (error in coding), we assume Incentive fees=median=20%; 
if incfee=. then incfee=0.20;
if incfee<0 then incfee=0.20;
if incfee>1 then incfee=0.20;
* 3) HWM is missing: We treat that it exists; 
if highwater=. then highwater=1;
* 4) Hurdle rate is missing;
* For onshore, assume hurdle rate is there. 
* For offshore, we take it as not being there;
if hurdrate=. and offsh=0 then hurdrate=1;
if hurdrate=. and offsh=1 then hurdrate=0;
* In both cases, we find that HR is not common;
* if hurdrate=. and offsh=0 then hurdrate=0;
* if hurdrate=. and offsh=1 then hurdrate=0;
* THis makes 83% of the funds having HR, so we dont use it;
* if hurdrate=. then hurdrate=1;
* 5) Management fees is missing: For missing guys, negative mgmt fee guys (error in coding) and 
*	more than 100% mgmt fee guys (error in coding), we assume, we assume Management fees=median=1%;
if mgmtfee=. then mgmtfee=0.01;
if mgmtfee<0 then mgmtfee=0.01;
if mgmtfee>1 then mgmtfee=0.01;
run;
* Write a macro to set risk-free rates;
%macro initial;
proc sort data=indata6; by &id year; run;
data temp;
retain xcode&beg_year - xcode&end_year xcodea&beg_year - xcodea&end_year
xtra&beg_year-xtra&end_year xaum&beg_year - xaum&end_year xret&beg_year xret&beg_year - xret&end_year
xret1&beg_year-xret1&end_year xret2&beg_year - xret2&end_year xret3&beg_year - xret3&end_year xret4&beg_year - xret4&end_year
xret5&beg_year - xret5&end_year xret6&beg_year - xret6&end_year xret7&beg_year - xret7&end_year xret8&beg_year-xret8&end_year
xret9&beg_year - xret9&end_year xret10&beg_year - xret10&end_year xret11&beg_year - xret11&end_year xret12&beg_year - xret12&end_year
xmthret1&beg_year - xmthret1&end_year xmthret2&beg_year - xmthret2&end_year xmthret3&beg_year - xmthret3&end_year xmthret4&beg_year - xmthret4&end_year
xmthret5&beg_year - xmthret5&end_year xmthret6&beg_year - xmthret6&end_year xmthret7&beg_year - xmthret7&end_year xmthret8&beg_year - xmthret8&end_year
xmthret9&beg_year - xmthret9&end_year xmthret10&beg_year - xmthret10&end_year xmthret11&beg_year - xmthret11&end_year xmthret12&beg_year - xmthret12&end_year
xstdret&beg_year - xstdret&end_year; 
set indata6;
%do a=&beg_year %to &end_year;
%let b=%eval(&a-1);
%let c=%eval(&a+1);
if year=&a
then 
	do; 
		aum&b=begyraum; 
		aum&a=aum; 
		ret&a=bhret1_12; 
		ret1&a=bhret1_1; 
		ret2&a=bhret1_2; 
		ret3&a=bhret1_3; 
		ret4&a=bhret1_4; 
		ret5&a=bhret1_5; 
		ret6&a=bhret1_6; 
		ret7&a=bhret1_7; 
		ret8&a=bhret1_8; 
		ret9&a=bhret1_9; 
		ret10&a=bhret1_10; 
		ret11&a=bhret1_11; 
		ret12&a=bhret1_12; 

		ret&c=ldbhret1_12; 
		ret1&c=ldbhret1_1; 
		ret2&c=ldbhret1_2; 
		ret3&c=ldbhret1_3; 
		ret4&c=ldbhret1_4; 
		ret5&c=ldbhret1_5; 
		ret6&c=ldbhret1_6; 
		ret7&c=ldbhret1_7; 
		ret8&c=ldbhret1_8; 
		ret9&c=ldbhret1_9; 
		ret10&c=ldbhret1_10; 
		ret11&c=ldbhret1_11; 
		ret12&c=ldbhret1_12; 

		mthret1&a=bhret1_1; 
		mthret2&a=bhret2_2; 
		mthret3&a=bhret3_3; 
		mthret4&a=bhret4_4; 
		mthret5&a=bhret5_5; 
		mthret6&a=bhret6_6; 
		mthret7&a=bhret7_7; 
		mthret8&a=bhret8_8; 
		mthret9&a=bhret9_9; 
		mthret10&a=bhret10_10; 
		mthret11&a=bhret11_11; 
		mthret12&a=bhret12_12; 

		stdret&a=stdret; 

		xcode&a=&id; 
		xaum&a=aum; 
		xret&a=bhret1_12; 
		xret1&a=bhret1_1; 
		xret2&a=bhret1_2; 
		xret3&a=bhret1_3; 
		xret4&a=bhret1_4; 
		xret5&a=bhret1_5; 
		xret6&a=bhret1_6; 
		xret7&a=bhret1_7; 
		xret8&a=bhret1_8; 
		xret9&a=bhret1_9; 
		xret10&a=bhret1_10; 
		xret11&a=bhret1_11; 
		xret12&a=bhret1_12; 

		xmthret1&a=bhret1_1; 
		xmthret2&a=bhret2_2; 
		xmthret3&a=bhret3_3; 
		xmthret4&a=bhret4_4; 
		xmthret5&a=bhret5_5; 
		xmthret6&a=bhret6_6; 
		xmthret7&a=bhret7_7; 
		xmthret8&a=bhret8_8; 
		xmthret9&a=bhret9_9; 
		xmthret10&a=bhret10_10; 
		xmthret11&a=bhret11_11; 
		xmthret12&a=bhret12_12; 

		xstdret&a=stdret; 
		end;
 	else 
		if xcode&a=&id 
		then 
			do; 
			aum&a=xaum&a; 
			ret&a=xret&a; 
			ret1&a=xret1&a; 
			ret2&a=xret2&a; 
			ret3&a=xret3&a; 
			ret4&a=xret4&a; 
			ret5&a=xret5&a; 
			ret6&a=xret6&a; 
			ret7&a=xret7&a; 
			ret8&a=xret8&a; 
			ret9&a=xret9&a; 
			ret10&a=xret10&a; 
			ret11&a=xret11&a; 
			ret12&a=xret12&a; 

			mthret1&a=xmthret1&a; 
			mthret2&a=xmthret2&a; 
			mthret3&a=xmthret3&a; 
			mthret4&a=xmthret4&a; 
			mthret5&a=xmthret5&a; 
			mthret6&a=xmthret6&a; 
			mthret7&a=xmthret7&a; 
			mthret8&a=xmthret8&a; 
			mthret9&a=xmthret9&a; 
			mthret10&a=xmthret10&a; 
			mthret11&a=xmthret11&a; 
			mthret12&a=xmthret12&a; 

			stdret&a=xstdret&a; 
			end;
 %end;
 %do a = &beg_year %to &end_year;
 	%let b=%eval(&a-1);
	if year=minyear and minyear=&a
 	then 
		do; 
		xcodea&a=&id; 
		xtra&a=begyraum; 
		end;
 	else 
		if xcodea&a=&id and year>minyear 
		then aum&b=xtra&a; 
 %end;

drop xcode&beg_year - xcode&end_year xcodea&beg_year - xcodea&end_year
xtra&beg_year-xtra&end_year xaum&beg_year - xaum&end_year xret&beg_year xret&beg_year - xret&end_year
xret1&beg_year-xret1&end_year xret2&beg_year - xret2&end_year xret3&beg_year - xret3&end_year xret4&beg_year - xret4&end_year
xret5&beg_year - xret5&end_year xret6&beg_year - xret6&end_year xret7&beg_year - xret7&end_year xret8&beg_year-xret8&end_year
xret9&beg_year - xret9&end_year xret10&beg_year - xret10&end_year xret11&beg_year - xret11&end_year xret12&beg_year - xret12&end_year
xmthret1&beg_year - xmthret1&end_year xmthret2&beg_year - xmthret2&end_year xmthret3&beg_year - xmthret3&end_year xmthret4&beg_year - xmthret4&end_year
xmthret5&beg_year - xmthret5&end_year xmthret6&beg_year - xmthret6&end_year xmthret7&beg_year - xmthret7&end_year xmthret8&beg_year - xmthret8&end_year
xmthret9&beg_year - xmthret9&end_year xmthret10&beg_year - xmthret10&end_year xmthret11&beg_year - xmthret11&end_year xmthret12&beg_year - xmthret12&end_year
xstdret&beg_year - xstdret&end_year ldbhret1_1 - ldbhret1_12 maxyear countyear sumyear;
run;
%mend;
%initial;
PROC IMPORT OUT= L.liborrates 
            DATAFILE= "C:\Users\Mikhail\Desktop\Funds\Rates\libor.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;
data l.liborrates; set l.liborrates; date=observation_date; value=USD12MD156N; format date date9.;drop USD12MD156N; drop observation_date ; run;

data l.liborrates; set l.liborrates; month=month(date); year=year(date); run;
* Fix LIBOR rates macro;
%macro libor;
%do i=&beg_year %to &end_year;
	%let j=%eval(&i-1);
	if month=12 and year=&j then libor&i=value/100;
	if month=12 and year=&j then rf&i=log(1+(value/100));
	%do k=1 %to 11;
	if month=&k and year=&i then rf&k._&i=log(1+(value/100));
	%end;
%end;
%mend;
* Get the LIBOR rates;
data l.liborrates;
set l.liborrates;
%libor;
run;
* Get LIBOR rates in 1 set;
%macro libor1;
%do i=&beg_year %to &end_year;
data libor&i;
set l.liborrates;
where libor&i~=.;
merge=1;
keep merge libor&i rf&i;
run;
	%do j=1 %to 11;
	data rf&j._&i;
	set l.liborrates;
	where rf&j._&i~=.;
	merge=1;
	keep merge rf&j._&i;
	run;
	%end;
data l&i;
merge rf1_&i rf2_&i rf3_&i rf4_&i rf5_&i rf6_&i rf7_&i rf8_&i rf9_&i rf10_&i rf11_&i libor&i;
by merge; 
run;
proc datasets; delete rf1_&i rf2_&i rf3_&i rf4_&i rf5_&i rf6_&i rf7_&i rf8_&i rf9_&i rf10_&i rf11_&i libor&i; quit;
%end;
data l.libor;
merge l&beg_year - l2014;
by merge;
run;
proc datasets; delete l&beg_year-l2014; quit; 
%mend;
%libor1;
* Add the LIBOR rates to the dataset;
data temp;
set temp;
merge=1;
run;
data temp&year_before;
merge temp l.libor;
by merge;
drop merge;
run;

* Do the calculations;
%macro year(lyr,yr,ldyr);

data temp&yr;
 set temp&lyr;

* 1987 is assumed to be the starting year for everybody. There 
*	were only 2 funds who were born before 1987 anyway;
 if minyear=&yr or (&yr=&beg_year)
 then 
 	do;


* Here we are assuming that the beginning AUM is held by sponsors (outside investors);
	spo&lyr.S=aum&lyr;
 	spo&lyr.X=spo&lyr.S*(1+libor&yr);
	spo&lyr.Sf=spo&lyr.S/aum&lyr;	
	spo&lyr.Xf=spo&lyr.X/aum&lyr;
	man&lyr=0;
	S&lyr=spo&lyr.S;
	X&lyr=spo&lyr.X;
	inv&lyr=aum&lyr;
	stdret&lyr=stdret&yr;
	stdret&lyr.a=stdret&yr*sqrt(12);
	iflow&lyr._&lyr.S=0;
	iflow&lyr._&lyr.X=0;
	inc&yr.iflow&lyr._&lyr=0;

/*
* Here we are assuming that the beginning AUM is held by managers;
	spo&lyr.S=0;
 	spo&lyr.X=spo&lyr.S*(1+libor&yr);
	spo&lyr.Sf=spo&lyr.S/aum&lyr;	
	spo&lyr.Xf=spo&lyr.X/aum&lyr;
	man&lyr=aum&lyr;
	S&lyr=0;
	X&lyr=0;
	inv&lyr=0;
	stdret&lyr=stdret&yr;
	stdret&lyr.a=stdret&yr*sqrt(12);
	iflow&lyr._&lyr.S=0;
	iflow&lyr._&lyr.X=0;
	inc&yr.iflow&lyr._&lyr=0;
*/

	end;

* 1) Computing ANNUAL gross return;
* ---------------------------------;

 convd&yr=0;
* for the very first year=1987, we dont need to do simulation;
* we can get gross returns straightforward fashion;
 if minyear=&yr or &yr=&beg_year
 then 
	do;

* If beg AUM is from investors;
	if ret&yr>libor&yr  then gret&yr=(ret&yr-incfee*libor&yr)/(1-incfee);
 	if ret&yr<=libor&yr then gret&yr=ret&yr;
 	if ret&yr=. or incfee=. then gret&yr=.;
	convd&yr=convd&yr+1;

/*
* If beg AUM is from managers;
	gret&yr=ret&yr;
 	if ret&yr=. or incfee=. then gret&yr=.;
* If beg AUM is from investors;
	convd&yr=convd&yr+1;

*/

	end;


* for the very first year=1987, we dont need to do simulation;
 if minyear<&yr and &yr>&beg_year
 then 
  do;
* this is the minimum gross return required before incentive
*	fees could be earned on all dollar packets.
*	If net return is positive but not greater than the min gross return
*	being computed below, 
*	then greth formula with ret would give a lower value than ret itself!;
* ie., we will get a max gross return that is lower than net return!;
* see Word file where we derive the formula;

  gret&yr.min=spo&lyr.Xf/spo&lyr.Sf;
* this we added to take into account when manager owns 100% at inception. Then the sponsor = 0
*	and we will get the ratio to be missing;
  if spo&lyr.Xf=. and spo&lyr.Sf=. 
  then gret&yr.min=0;

  %do a = &beg_year %to &lyr;
  	gret&yr.min=Max(gret&yr.min,iflow&a._&lyr.Xf/iflow&a._&lyr.Sf);
  %end;
  gret&yr.min=gret&yr.min-1;
  gret&yr.minmax=Max(gret&yr.min,ret&yr);
  gret&yr.hNr=gret&yr.minmax - (spo&lyr.Xf-spo&lyr.Sf)*incfee; 
  %do a = &beg_year %to &lyr;
	gret&yr.hNr=gret&yr.hNr - (iflow&a._&lyr.Xf-iflow&a._&lyr.Sf)*incfee;
  %end;
  gret&yr.hDr=1 - spo&lyr.Sf*incfee;
  %do a = &beg_year %to &lyr;
	gret&yr.hDr=gret&yr.hDr - iflow&a._&lyr.Sf*incfee;
  %end;
* highest possible gross return when ALL of the investors have to pay incentive fees;
  gret&yr.h=gret&yr.hNr/gret&yr.hDr;				
* Lowest possible gross return when NONE of the investors have to pay incentive fees;
  gret&yr.l=ret&yr;

  gret&yr.l1=int(gret&yr.l*10000);
  gret&yr.h1=int(gret&yr.h*10000);
  if ret&yr<=0 
  then 
 	do; 
	gret&yr=ret&yr; 
	convd&yr=convd&yr+1; 
	end;
  if gret&yr.l=gret&yr.h 
  then 
	do; 
	gret&yr=ret&yr; 
	convd&yr=convd&yr+1; 
	end;
  if ret&yr=. 
  then 
	do; 
	gret&yr=.; 
	end;
* the end below corresponds to minyear<yr;
  end;

 if gret&yr.l^=. and gret&yr.h^=. and convd&yr=0 and ret&yr>0 
		and ret&yr^=. and &yr>&beg_year 
 then
  do j&yr=gret&yr.l1 to gret&yr.h1 by 1;
    i&yr=j&yr/10000;
  	ret&yr.t=i&yr - Max((spo&lyr.Sf*(1+i&yr)-spo&lyr.Xf),0)*incfee;
	%do a = &beg_year %to &lyr;
	 	ret&yr.t=ret&yr.t - Max((iflow&a._&lyr.Sf*(1+i&yr)-iflow&a._&lyr.Xf),0)*incfee;
  	%end;
    diff&yr=(ret&yr.t-ret&yr);
    prev&yr=(j&yr-1)/10000;
    prevret&yr.t=prev&yr - Max((spo&lyr.Sf*(1+prev&yr)-spo&lyr.Xf),0)*incfee;
	%do a = &beg_year %to &lyr;
    	prevret&yr.t=prevret&yr.t - Max((iflow&a._&lyr.Sf*(1+prev&yr)-iflow&a._&lyr.Xf),0)*incfee;
    %end;
    prevdiff&yr=(prevret&yr.t-ret&yr);
		* Note that we enter the loop only when the solution is not the 
		*	low point that is why we do not do comparison when j=low point;
    if j&yr=gret&yr.l1
    then
      do;
	  startdiff&yr=diff&yr;
	  startgret&yr=i&yr;
      convd&yr=0;
      end;
		* if we are within bounds and the diff switches sign then we have
		* reached the convergence point;
    if (j&yr>gret&yr.l1 and j&yr<gret&yr.h1) and 
       ((diff&yr<=0 and prevdiff&yr>0) or (diff&yr>=0 and prevdiff&yr<0))
    then
      do;
      convd&yr=convd&yr+1;
      gret&yr=i&yr; 
	  end;
		* some times, it is almost converging towards the end and we are 
		*	trying to capture this;
    if j&yr=gret&yr.h1 and convd&yr=0 and abs(diff&yr)<0.0001
    then
      do;
      convd&yr=convd&yr+1;
      gret&yr=i&yr; 
 	  end;
		* sometimes, the difference starts diverging from the start and
		*	if the diff at teh beginning was small enough, then the 
		*	smallest value is the gross return - but we want to run
		*	thro the entire range before doing that ;
    if j&yr=gret&yr.h1 and convd&yr=0 and abs(startdiff&yr)<0.0001
    then
      do;
      convd&yr=convd&yr+1;
      gret&yr=startgret&yr; 
 	  end;
    if j&yr=gret&yr.h1 and convd&yr=0 and abs(diff&yr)>0.0001 and abs(startdiff&yr)>0.0001
    then 
      do;
	  gret&yr=.;
      end;
* end corresponds to convergence loop;
  end;

* 1a) Computing MONTHLY gross return;
* ----------------------------------;

 %do m = 1 %to 12;
 convd&m.&yr=0;

* for the very first year=1987, we dont need to do simulation;
* we can get gross returns straightforward fashion;
 if minyear=&yr or &yr=&beg_year
 then  
	do;

* If beg AUM is from investors;
	if ret&m.&yr>libor&yr  then gret&m.&yr=(ret&m.&yr-incfee*libor&yr)/(1-incfee);
 	if ret&m.&yr<=libor&yr then gret&m.&yr=ret&m.&yr;
 	if ret&m.&yr=. or incfee=. then gret&m.&yr=.;
	convd&m.&yr=convd&m.&yr+1;

/*
* If beg AUM is from managers;
	gret&m.&yr=ret&m.&yr;
 	if ret&m.&yr=. or incfee=. then gret&m.&yr=.;
* If beg AUM is from investors;
	convd&m.&yr=convd&m.&yr+1;
*/

	end;


* if (code=2189) and year=&yr
* then put code year gret&m.&yr convd&m.&yr;

 if minyear<&yr and &yr>&beg_year
 then 
  do;
* if (code=2189) and year=&yr
* then put code year gret&m.&yr convd&m.&yr;
* this is the minimum gross return required before incentive
*	fees could be earned on all dollar packets.
*	If net return is positive but not greater than the min gross return
*	being computed below, 
*	then greth formula with ret would give a lower value than ret itself!;
* ie., we will get a max gross return that is lower than net return!;

  gret&m.&yr.min=spo&lyr.Xf/spo&lyr.Sf;
* this we added to take into account when manager owns 100% at inception. Then the sponsor = 0
*	and we will get the ratio to be missing;
  if spo&lyr.Xf=. and spo&lyr.Sf=. 
  then gret&m.&yr.min=0;

  %do a = &beg_year %to &lyr;
  	gret&m.&yr.min=Max(gret&m.&yr.min,iflow&a._&lyr.Xf/iflow&a._&lyr.Sf);
  %end;
  gret&m.&yr.min=gret&m.&yr.min-1;
  gret&m.&yr.minmax=Max(gret&m.&yr.min,ret&m.&yr);
  gret&m.&yr.hNr=gret&m.&yr.minmax - (spo&lyr.Xf-spo&lyr.Sf)*incfee; 
  %do a = &beg_year %to &lyr;
	gret&m.&yr.hNr=gret&m.&yr.hNr - (iflow&a._&lyr.Xf-iflow&a._&lyr.Sf)*incfee;
  %end;
  gret&m.&yr.hDr=1 - spo&lyr.Sf*incfee;
  %do a = &beg_year %to &lyr;
	gret&m.&yr.hDr=gret&m.&yr.hDr - iflow&a._&lyr.Sf*incfee;
  %end;

* highest possible gross return when ALL of the investors have to pay incentive fees;
  gret&m.&yr.h=gret&m.&yr.hNr/gret&m.&yr.hDr;				
* Lowest possible gross return when NONE of the investors have to pay incentive fees;
  gret&m.&yr.l=ret&m.&yr;

  gret&m.&yr.l1=int(gret&m.&yr.l*10000);
  gret&m.&yr.h1=int(gret&m.&yr.h*10000);
  if ret&m.&yr<=0 
  then 
 	do; 
	gret&m.&yr=ret&m.&yr; 
	convd&m.&yr=convd&m.&yr+1; 
	end;
  if gret&m.&yr.l=gret&m.&yr.h 
  then 
	do; 
	gret&m.&yr=ret&m.&yr; 
	convd&m.&yr=convd&m.&yr+1; 
	end;
  if ret&m.&yr=. 
  then 
	do;
	gret&m.&yr=.; 
	end;

* the end below corresponds to minyear<yr;
  end;

 if gret&m.&yr.l^=. and gret&m.&yr.h^=. and convd&m.&yr=0 and ret&m.&yr>0 
	and ret&m.&yr^=. and &yr>&beg_year
 then
  do j&m.&yr=gret&m.&yr.l1 to gret&m.&yr.h1 by 1;
    i&m.&yr=j&m.&yr/10000;
  	ret&m.&yr.t=i&m.&yr - Max((spo&lyr.Sf*(1+i&m.&yr)-spo&lyr.Xf),0)*incfee;
	%do a = 1987 %to &lyr;
		ret&m.&yr.t=ret&m.&yr.t - Max((iflow&a._&lyr.Sf*(1+i&m.&yr)-iflow&a._&lyr.Xf),0)*incfee;
  	%end;
    diff&m.&yr=(ret&m.&yr.t-ret&m.&yr);
    prev&m.&yr=(j&m.&yr-1)/10000;
    prevret&m.&yr.t=prev&m.&yr - Max((spo&lyr.Sf*(1+prev&m.&yr)-spo&lyr.Xf),0)*incfee;
	%do a = &beg_year %to &lyr;
    	prevret&m.&yr.t=prevret&m.&yr.t - Max((iflow&a._&lyr.Sf*(1+prev&m.&yr)-iflow&a._&lyr.Xf),0)*incfee;
    %end;
    prevdiff&m.&yr=(prevret&m.&yr.t-ret&m.&yr);
	* Note that we enter the loop only when the solution is not the 
	*	low point that is why we do not do comparison when j=low point;
    if j&m.&yr=gret&m.&yr.l1
    then
    	do;
		startdiff&m.&yr=diff&m.&yr;
		startgret&m.&yr=i&m.&yr;
      	convd&m.&yr=0;
		* if (code=2157) and year=&yr
		* then put code year startdiff&m.&yr startgret&m.&yr ret&yr i&m.&yr ret&yr.t diff&yr prev&m.&yr prevret&m.&yr.t prevdiff&m.&yr convd&yr ;
	    end;
		* if we are within bounds and the diff&m. switches sign then we have
		* reached the convergence point;
    if (j&m.&yr>gret&m.&yr.l1 and j&m.&yr<gret&m.&yr.h1) and 
       ((diff&m.&yr<=0 and prevdiff&m.&yr>0) or (diff&m.&yr>=0 and prevdiff&m.&yr<0))
    then
      	do;
      	convd&m.&yr=convd&m.&yr+1;
      	gret&m.&yr=i&m.&yr; 
		end;
		* some times, it is almost converging towards the end and we are 
		*	trying to capture this;
    if j&m.&yr=gret&m.&yr.h1 and convd&m.&yr=0 and abs(diff&m.&yr)<0.0001
    then
    	do;
      	convd&m.&yr=convd&m.&yr+1;
      	gret&m.&yr=i&m.&yr; 
 		end;
		* sometimes, the difference starts diverging from the start and
		*	if the diff at teh beginning was small enough, then the 
		*	smallest value is the gross return - but we want to run
		*	thro the entire range before doing that ;
    if j&m.&yr=gret&m.&yr.h1 and convd&m.&yr=0 and abs(startdiff&m.&yr)<0.0001
    then
    	do;
      	convd&m.&yr=convd&m.&yr+1;
      	gret&m.&yr=startgret&m.&yr; 
 		end;
    if j&m.&yr=gret&m.&yr.h1 and convd&m.&yr=0 and abs(diff&m.&yr)>0.0001 and abs(startdiff&m.&yr)>0.0001
    then 
    	do;
		gret&m.&yr=.;
 		end;
* end corresponds to convergence loop;
  end;

* This is the end of m=1 to 12;
%end;

* We only get the cumulative returns from the iterative process, and we have to back out
*	the monthly gross returns;
 mthgret1&yr=gret1&yr; 
 mthgret2&yr=(1+gret2&yr)/(1+gret1&yr)-1; 
 mthgret3&yr=(1+gret3&yr)/(1+gret2&yr)-1; 
 mthgret4&yr=(1+gret4&yr)/(1+gret3&yr)-1; 
 mthgret5&yr=(1+gret5&yr)/(1+gret4&yr)-1; 
 mthgret6&yr=(1+gret6&yr)/(1+gret5&yr)-1; 
 mthgret7&yr=(1+gret7&yr)/(1+gret6&yr)-1; 
 mthgret8&yr=(1+gret8&yr)/(1+gret7&yr)-1; 
 mthgret9&yr=(1+gret9&yr)/(1+gret8&yr)-1; 
 mthgret10&yr=(1+gret10&yr)/(1+gret9&yr)-1; 
 mthgret11&yr=(1+gret11&yr)/(1+gret10&yr)-1; 
 mthgret12&yr=(1+gret12&yr)/(1+gret11&yr)-1; 

* Gross return obtained using monthly routing and gross return obtained using annual routine.
*	This is a good check to see if our monthly routine is correct. so it should
*	match perfectly;
  diffgret&yr=abs(gret&yr-gret12&yr);

* 2) Computing incentive fees based on gross returns;
* Also setting the temporary S and X for various components;
* --------------------------------------------------;

 spo&yr.St1=spo&lyr.S*(1+gret&yr);
 inc&yr.spo&lyr=Max((spo&yr.St1-spo&lyr.X),0)*incfee;
 spo&yr.St=spo&yr.St1-inc&yr.spo&lyr;

 if spo&yr.St1>spo&lyr.X and spo&yr.St1^=. and spo&lyr.X^=.
 then spo&yr.Xt=spo&yr.St*(1+libor&ldyr);

 if spo&yr.St1<=spo&lyr.X and highwater=1 and spo&yr.St1^=. and spo&lyr.X^=. and highwater^=.
 then spo&yr.Xt=spo&lyr.X*(1+libor&ldyr);

 if spo&yr.St1<=spo&lyr.X and highwater=0 and spo&yr.St1^=. and spo&lyr.X^=. and highwater^=.
 then spo&yr.Xt=spo&yr.St*(1+libor&ldyr);

 if spo&yr.St1=. or spo&lyr.X=. or highwater=.
 then 
    do;
    inc&yr.spo&lyr.=.;
    spo&yr.St=.;
    spo&yr.Xt=.;
    end;

 %do a = &beg_year %to &lyr;
	iflow&a._&yr.St1=iflow&a._&lyr.S*(1+gret&yr);
 	inc&yr.iflow&a._&lyr=Max((iflow&a._&yr.St1-iflow&a._&lyr.X),0)*incfee;
 	iflow&a._&yr.St=iflow&a._&yr.St1-inc&yr.iflow&a._&lyr;

	if iflow&a._&yr.St1>iflow&a._&lyr.X and iflow&a._&yr.St1^=. and iflow&a._&lyr.X^=.
 	then iflow&a._&yr.Xt=iflow&a._&yr.St*(1+libor&ldyr);

	if iflow&a._&yr.St1<=iflow&a._&lyr.X and highwater=1 and iflow&a._&yr.St1^=. and iflow&a._&lyr.X^=. and highwater^=.
 	then iflow&a._&yr.Xt=iflow&a._&lyr.X*(1+libor&ldyr);

	if iflow&a._&yr.St1<=iflow&a._&lyr.X and highwater=0 and iflow&a._&yr.St1^=. and iflow&a._&lyr.X^=. and highwater^=.
 	then iflow&a._&yr.Xt=iflow&a._&yr.St*(1+libor&ldyr);

 	if iflow&a._&yr.St1=. or iflow&a._&lyr.X=. or highwater=.
 	then
    do;
    inc&yr.iflow&a._&lyr.=.;
    iflow&a._&yr.St=.;
    iflow&a._&yr.Xt=.;
    end;
  %end;

	inc&yr=inc&yr.spo&lyr;
  %do a = &beg_year %to &lyr;
		if &yr>&beg_year then inc&yr=inc&yr+inc&yr.iflow&a._&lyr;
  %end;

* 3) Compute total flows, which includes incentives fees reinvested;
* -----------------------------------------------------------------;

*	When using net returns, flows includes incentives fees reinvested;
* For example assume that aum(t-1) = 100 and that investors didnt bring in
*       any new flow that year. Also, assume net ret=4% (reported in the database).
*       thus gross ret=5% (assuming inc fee=20%). Hence, AUM=105 in the database
*       we calculate flows = 105 - 100*1.04 = 1. This is the incentive fee
*       the manager earned. so we need to take this into account;
* When net returns are positive, gross>net and hence flow00>flow00g;
* When net returns are negative, gross=net and hence flow00=flow00g;

* investor flows + sponsor flows + incentive fees;
 flow&yr=aum&yr-aum&lyr*(1+ret&yr);
* investor flows + sponsor flows ;
 flow&yr.g=aum&yr-aum&lyr*(1+gret&yr);

 inc&yr.a=flow&yr-flow&yr.g;
 if abs(inc&yr)<1e-6 and inc&yr^=. then inc&yr=0;
 if abs(inc&yr.a)<1e-6 and inc&yr.a^=. then inc&yr.a=0;
 
* Incentives will not exactly match because gross return computation
* is not exact but rounded to the closest 0.01%;
 diffinc&yr=abs(inc&yr.a-inc&yr);

* 4) Compute net flow (from sponsors and investors) after taking;
* into account inc fees earned by manager;
* --------------------------------------------------------------;
 tax&yr=inc&yr*tax;
 iflow&yr.ta=flow&yr-(inc&yr-tax&yr);
* fractional flow to be used in regressions;

* 5) Modifying first-estimate of flow to take any new outflows
* into account using FIFO rule;
* When flow is positive, we dont have to do anything. We have to do
* adjustment only when flow is negative;
* Hurdle rate depends only on returns and not on flows;
* --------------------------------------------------------------;

/*
* since we are assuming 1987 is the starting point for everybody;
 if &yr=1987
 then man&yr.t=inc&yr-tax&yr;
 else man&yr.t=man&lyr*(1+gret&yr)+inc&yr-tax&yr;
*/
* We use to have the above before but the IF condition is not required since manlyr=0 
*	if we assume that initial AUM = sponsor;
*	If not manlyr=aumlyr. so we dont need the IF condition at all at any time;
 man&yr.t=man&lyr*(1+gret&yr)+inc&yr-tax&yr;

* The flow computed with gross returns can give problems becuase
*	of rounding-off errror. So we are backing out flow as the balance
*	of the AUM from the spot prices of other packets of flow;
* Also this ensure that none of the St2 variables below are less
*	than AUM. Otherwise, we had to check each of the St2 variable
*	against AUM;

 iflow&yr.t = aum&yr - (spo&yr.St+man&yr.t);
 %do a = &beg_year %to &lyr;
  	iflow&yr.t = iflow&yr.t - iflow&a._&yr.St;
 %end;
 
 fracflow&yr=iflow&yr.t/(aum&lyr*(1+gret&yr));
 diffiflow&yr.t=abs(iflow&yr.ta-iflow&yr.t);

* for the sake of being amenable to macro usage, we are naming 
* the variable as follows;

 iflow&year_before._&yr.St2=iflow&yr.t;
 %do a = &beg_year %to &lyr;
 	%let b=%eval(&a-1);
  iflow&a._&yr.St2=iflow&b._&yr.St2+iflow&a._&yr.St; 
 %end;

	spo&yr.St2=iflow&lyr._&yr.St2+spo&yr.St;
  man&yr.t2=spo&yr.St2+man&yr.t;

* Remember there are 3 possible spot prices: St, St2, and 0;
* For the manager, it cant be zero but AUM is the minimum;
* If previous sum > 0 and current sum > 0 then spot=St;
* If previous sum < 0 and current sum > 0 then spot=St2;
* If current sum < 0 (if current < 0 does not depend on previous) then spot=0;
* In the case of investor flows and sponsor,
*	the previous depends on minyear;
* When minyear=1988, the previous is iflow87t ;
* When minyear<1988, the previous is iflow87_88t ;
* In case of manager, the previous is always sponsor and does not
*	depend on minyear;

 if iflow&yr.t>0 and iflow&yr.t^=.
 then iflow&yr._&yr.S=iflow&yr.t;
 if iflow&yr.t<=0 and iflow&yr.t^=.
 then	iflow&yr._&yr.S=0;

 %do a = &beg_year %to &lyr;
    %let b=%eval(&a-1);
		if (minyear<=&a and iflow&b._&yr.St2>0  and iflow&a._&yr.St2>0) and 
		 minyear^=.    and iflow&b._&yr.St2^=. and iflow&a._&yr.St2^=.
 		then iflow&a._&yr.S=iflow&a._&yr.St;
 		if (minyear<=&a and iflow&b._&yr.St2<=0 and iflow&a._&yr.St2>0) and 
		 minyear^=.    and iflow&b._&yr.St2^=. and iflow&a._&yr.St2^=.
		then iflow&a._&yr.S=iflow&a._&yr.St2;
 		if ((minyear<=&a and iflow&a._&yr.St2<=0) or minyear>&a) and 
		 minyear^=.    and iflow&a._&yr.St2^=.
		then iflow&a._&yr.S=0;
	%end;

 if ((minyear<&yr and iflow&lyr._&yr.St2>0 and spo&yr.St2>0) or 
	   (minyear=&yr and iflow&yr.t>0      and spo&yr.St2>0)) and 
			minyear^=.   and iflow&lyr._&yr.St2^=. and spo&yr.St2^=. and iflow&yr.t^=.
 then spo&yr.S=spo&yr.St;
 if ((minyear<&yr and iflow&lyr._&yr.St2<=0 and spo&yr.St2>0) or 
		 (minyear=&yr and iflow&yr.t<=0      and spo&yr.St2>0)) and 
		  minyear^=.   and iflow&lyr._&yr.St2^=. and spo&yr.St2^=. and iflow&yr.t^=.
 then spo&yr.S=spo&yr.St2;
 if spo&yr.St2<=0 and spo&yr.St2^=.
 then spo&yr.S=0;

 if spo&yr.St2>0 and man&yr.t2>0 and 
		spo&yr.St2^=. and man&yr.t2^=.
 then man&yr=man&yr.t;
 if spo&yr.St2<=0 and man&yr.t2>0 and 
		spo&yr.St2^=. and man&yr.t2^=.
 then man&yr=man&yr.t2;
 if man&yr.t2<=0 and 
		spo&yr.St2^=. and man&yr.t2^=.
 then 
 		do;
	  man&yr=aum&yr;
		flagman&yr=1;
    end;

* X is always new S times the multiplication factor except for latest flow;
* Also, Manager has no S;
 %do a = &beg_year %to &lyr;
 		iflow&a._&yr.X=iflow&a._&yr.S*(iflow&a._&yr.Xt/iflow&a._&yr.St);
		* when both are zero, we need to set X=0;
 		if iflow&a._&yr.Xt=0 and iflow&a._&yr.St=0
 		then iflow&a._&yr.X=0;
 %end;
* For latest flow, there was no St and Xt. Its X has never been set
*	before. That is why it is slightly different for the latest flow;
* The following is correct, but in delta6.sas, we had done this
*	statement only for 87 and 88. Afterwardsm it was wrong. So in order
* to purely check the numbers with delta6.lst, I would run the macro
*	with the right statement for 86 and 87 but use the wrong
*	statement for other years;
* right;
 iflow&yr._&yr.X=iflow&yr._&yr.S*(1+libor&ldyr);
* wrong;
* iflow&yr._&yr.X=iflow&yr._&yr.S*(1+libor&yr);
 spo&yr.X=spo&yr.S*(spo&yr.Xt/spo&yr.St);
 if spo&yr.Xt=0 and spo&yr.St=0
 then spo&yr.X=0;

* investor money is the sum of S of all previous inflows flows;
*	this should be equal to the balance of AUM afer taking 
*	manager and sponsors money;

 inv&yr=0;
 %do a = &beg_year %to &yr;
 	inv&yr= inv&yr+iflow&a._&yr.S;
 %end;

 man&yr.a=aum&yr-spo&yr.S-inv&yr;
 if abs(inv&yr)<1e-6 and inv&yr^=. then inv&yr=0;
 if abs(man&yr.a)<1e-6 and man&yr.a^=. then man&yr.a=0;
 diffman&yr=abs(man&yr.a-man&yr);
 
 if abs(man&yr)<1e-6 and man&yr^=. then man&yr=0;
 
 if abs(spo&yr.S)<1e-6 and spo&yr.S^=. then spo&yr.S=0;
 if abs(spo&yr.X)<1e-6 and spo&yr.X^=. then spo&yr.X=0;

 %do a = 1987 %to &yr;
	if abs(iflow&a._&yr.S)<1e-6 and iflow&a._&yr.S^=. then iflow&a._&yr.S=0;
 	if abs(iflow&a._&yr.X)<1e-6 and iflow&a._&yr.X^=. then iflow&a._&yr.X=0;
 %end;

* 6) Checking for underwater options;
* ----------------------------------;
 X&yr=spo&yr.X;
 S&yr=spo&yr.S;
 %do a = &beg_year %to &yr;
 	X&yr=X&yr+iflow&a._&yr.X;
 %end;
 %do a = &beg_year %to &yr;
 	S&yr=S&yr+iflow&a._&yr.S;
 %end;
 uw&yr=S&yr-X&yr;
 uw&yr.p=uw&yr/S&yr;  
 uw&yr.px=uw&yr/X&yr;  
*	when a fund earned high return, and the managers incentive
*	fee was greater than closing aum. therefore S99=X99=0 which
* made the ratio=. Both being zero implies that only the manager 
*	money is left. therefore no underwater options;
 if X&yr=0 and S&yr=0 then do; uw&yr.p=0; uw&yr.px=0; end;
 if abs(uw&yr)<1e-6 and uw&yr^=. then uw&yr=0;
 if abs(uw&yr.p)<1e-6 and uw&yr.p^=. then uw&yr.p=0;
 if abs(uw&yr.px)<1e-6 and uw&yr.px^=. then uw&yr.px=0;

* Before flows come in and before resetting but after incentive fees have been paid;
* Remember we cannot just take beginning-period exercise and compare
*	with current spot since there might have been outflows, which will
* make Xprice less than Sprice even though there might have been 
* positive returns. This is why we are taking Syr
* But then we dont want to include current year flows, so we net it off;
* Still the following is correct only if flows in the year is positive but not otherwise;
* Bcos if investor flow (iflowyrt) is negative then iflowyr_yrS=0. and we take out oldest inflow and 
*	the corresponding X (there is no X corresponding to iflowyrt when it is negative). 
*	This is becuase we dont know how many of past investor packets is extinguished by iflow87t
*	The net effect is reflected in Syr and Xyr. It is 
*	nearly impossible to factor this in our calculations easily;
* Xprior&yr=(X&yr-iflow&yr._&yr.X)/(1+libor&ldyr);
* Sprior&yr=S&yr-iflow&yr._&yr.S;
* uwprior&yr=Sprior&yr-Xprior&yr;
* uwprior&yr.p=uwprior&yr/Sprior&yr;
* uwprior&yr.px=uwprior&yr/Xprior&yr;
* if Xprior&yr=0 and Sprior&yr=0 then uwprior&yr.p=0;
* if abs(uwprior&yr)<1e-6 and uwprior&yr^=. then uwprior&yr=0;
* if abs(uwprior&yr.p)<1e-6 and uwprior&yr.p^=. then uwprior&yr.p=0;
* if abs(uwprior&yr.px)<1e-6 and uwprior&yr.px^=. then uwprior&yr.px=0;

* After flows come in but before resetting;
* Note uw1prioryr will be exactly equal to uwprioryr since
*	by the exercise price of latest flow = (1+libor)* Spot price
*	That is,iflowyr._yr.X=(1+libor)*iflowyr._yr.S;
 Xprior&yr=X&yr/(1+libor&ldyr);
 Sprior&yr=S&yr;
 uwprior&yr=Sprior&yr-Xprior&yr;
 uwprior&yr.p=uwprior&yr/Sprior&yr;
 uwprior&yr.px=uwprior&yr/Xprior&yr;
 if Xprior&yr=0 and Sprior&yr=0 then do; uwprior&yr.p=0; uwprior&yr.px=0; end;
 if abs(uwprior&yr)<1e-6 and uwprior&yr^=. then uwprior&yr=0;
 if abs(uwprior&yr.p)<1e-6 and uwprior&yr.p^=. then uwprior&yr.p=0;
 if abs(uwprior&yr.px)<1e-6 and uwprior&yr.px^=. then uwprior&yr.px=0;

* 6.1) Monthly underwaterness;

%do m=1 %to 12;

 S&m.&yr=S&lyr*(1+gret&m.&yr);
 uw&m.&yr=S&m.&yr-X&lyr;
 uw&m.&yr.p=uw&m.&yr/S&m.&yr;  
 uw&m.&yr.px=uw&m.&yr/X&lyr;  
 if X&lyr=0 and S&m.&yr=0 then do; uw&m.&yr.p=0; uw&m.&yr.px=0; end;
 if abs(uw&m.&yr)<1e-6 and uw&m.&yr^=. then uw&m.&yr=0;
 if abs(uw&m.&yr.p)<1e-6 and uw&m.&yr.p^=. then uw&m.&yr.p=0;
 if abs(uw&m.&yr.px)<1e-6 and uw&m.&yr.px^=. then uw&m.&yr.px=0;

%end;

* 7) Distribution of AUM across investor clienteles;
* -------------------------------------------------;

* The packets of dollars we have to keep track are
*       man00, spo00, iflow99_00 and iflow00_00;

 man&yr.f=man&yr/aum&yr;
 spo&yr.Sf=spo&yr.S/aum&yr;
 %do a = &beg_year %to &yr;
 	iflow&a._&yr.Sf=iflow&a._&yr.S/aum&yr;
 %end;
 inv&yr.f=inv&yr/aum&yr;

* We have to keep track of all exercise prices too;
 spo&yr.Xf=spo&yr.X/aum&yr;
 %do a = &beg_year %to &yr;
 	iflow&a._&yr.Xf=iflow&a._&yr.X/aum&yr;
 %end;

* bounded to be between 0 and 1;
 if abs(man&yr.f-1)<1e-6 and man&yr.f^=. then man&yr.f=1;
 if abs(man&yr.f)  <1e-6 and man&yr.f^=. then man&yr.f=0;
 if abs(spo&yr.Sf-1)<1e-6 and spo&yr.Sf^=. then spo&yr.Sf=1;
 if abs(spo&yr.Sf)  <1e-6 and spo&yr.Sf^=. then spo&yr.Sf=0;
 if abs(inv&yr.f-1)<1e-6 and inv&yr.f^=. then inv&yr.f=1;
 if abs(inv&yr.f)  <1e-6 and inv&yr.f^=. then inv&yr.f=0;

 %do a = &beg_year %to &yr;
 	if abs(iflow&a._&yr.Sf-1)<1e-6 and iflow&a._&yr.Sf^=. then iflow&a._&yr.Sf=1;
 %end;
 %do a = &beg_year %to &yr;
 	if abs(iflow&a._&yr.Sf)  <1e-6 and iflow&a._&yr.Sf^=. then iflow&a._&yr.Sf=0;
 %end;
*	exercise price need not be necessarily be less than 1;
 %do a = &beg_year %to &yr;
 	if abs(iflow&a._&yr.Xf-0)<1e-6 and iflow&a._&yr.Xf^=. then iflow&a._&yr.Xf=0;
 %end;

* 8) Value, Delta, and Vega computation;
* -------------------------------------;
* for robustness, we may allow time=2 or 3 since half-life is only 2.5 years;
 time=1;
* Stdret should be annualised standard deviation;
 stdret&yr.a=stdret&yr*sqrt(12);

* Managers ownership is like shareholdings 
*	effectively provides only delta and not vega;
 man&yr.del=man&yr*0.01;

* rf should be look ahead. hence rf2000 should be there in 1999 calc,
*	rf1999 in 2000 calc and so on;
 spo&yr.Z=(log(spo&yr.S/spo&yr.X)+time*(rf&ldyr+0.5*(stdret&yr.a**2)))
				/(stdret&yr.a*(time**0.5));
 spo&yr.val=(spo&yr.S*cdf('normal',spo&yr.Z)
					-spo&yr.X*exp(-1*rf&ldyr*time)
					*cdf('normal',spo&yr.Z-stdret&yr.a*(time**0.5)))*incfee;
 spo&yr.del=spo&yr.S*cdf('normal',spo&yr.Z)*0.01*incfee;
 spo&yr.veg=spo&yr.S*pdf('normal',spo&yr.Z)*0.01*(time**0.5)*incfee;
* When flow=0, we assign S and X to be zero. Then S/X in Z is undefined
*       and we end end up getting all values to be missing, which is wrong;
 if spo&yr.S=0
 then do; spo&yr.val=0; spo&yr.del=0; spo&yr.veg=0; end;

 %do a = &beg_year %to &yr;
 	iflow&a._&yr.Z=(log(iflow&a._&yr.S/iflow&a._&yr.X)+time*(rf&ldyr+0.5*(stdret&yr.a**2)))
							/(stdret&yr.a*(time**0.5));
 	iflow&a._&yr.val=(iflow&a._&yr.S*cdf('normal',iflow&a._&yr.Z)
							-iflow&a._&yr.X*exp(-1*rf&ldyr*time)
							*cdf('normal',iflow&a._&yr.Z-stdret&yr.a*(time**0.5)))*incfee;
 	iflow&a._&yr.del=iflow&a._&yr.S*cdf('normal',iflow&a._&yr.Z)*0.01*incfee;
 	iflow&a._&yr.veg=iflow&a._&yr.S*pdf('normal',iflow&a._&yr.Z)*0.01*(time**0.5)*incfee;
* When flow=0, we assign S and X to be zero. Then S/X in Z is undefined
*       and we end end up getting all values to be missing, which is wrong;
 	if iflow&a._&yr.S=0
 	then do; iflow&a._&yr.val=0; iflow&a._&yr.del=0; iflow&a._&yr.veg=0; end;
 %end;

 if abs(man&yr.del)<1e-6 and man&yr.del^=. then man&yr.del=0;
 if abs(spo&yr.val)<1e-6 and spo&yr.val^=. then spo&yr.val=0;
 if abs(spo&yr.del)<1e-6 and spo&yr.del^=. then spo&yr.del=0;
 if abs(spo&yr.veg)<1e-6 and spo&yr.veg^=. then spo&yr.veg=0;

 %do a = &beg_year %to &yr;
	if abs(iflow&a._&yr.val)<1e-6 and iflow&a._&yr.val^=. then iflow&a._&yr.val=0;
 	if abs(iflow&a._&yr.del)<1e-6 and iflow&a._&yr.del^=. then iflow&a._&yr.del=0;
 	if abs(iflow&a._&yr.veg)<1e-6 and iflow&a._&yr.veg^=. then iflow&a._&yr.veg=0;
 %end;

 inv&yr.val = 0;
 inv&yr.del = 0;
 inv&yr.veg = 0;

 %do a = &beg_year %to &yr;
 	inv&yr.val = inv&yr.val + iflow&a._&yr.val;
 	inv&yr.del = inv&yr.del + iflow&a._&yr.del;
 	inv&yr.veg = inv&yr.veg + iflow&a._&yr.veg;
 %end;

 man&yr.val=man&yr;

 value&yr = man&yr.val + spo&yr.val + inv&yr.val;
 delta&yr = man&yr.del + spo&yr.del + inv&yr.del;
 vega&yr  =            spo&yr.veg + inv&yr.veg;

* 8.1) Value, Delta, and Vega computation for sub-periods;
* -------------------------------------------------------;
 
%do m = 1 %to 11;

 time&m=(12-&m)/12;

 man&m.&yr=man&lyr*(1+gret&m.&yr);
 inv&m.&yr=inv&lyr*(1+gret&m.&yr);

* Managers ownership is like shareholdings 
*	effectively provides only delta and not vega;
 man&m.&yr.del=man&lyr*(1+gret&m.&yr)*0.01;

* Note rf1_yr is 1-year LIBOR as of Jan-end of year=yr;
 spo&m.&yr.S=spo&lyr.S*(1+gret&m.&yr);
 spo&m.&yr.Z=(log(spo&m.&yr.S/spo&lyr.X)+time&m.*(rf1_&yr+0.5*(stdret&lyr.a**2)))
				/(stdret&lyr.a*(time&m.**0.5));
 spo&m.&yr.val=(spo&m.&yr.S*cdf('normal',spo&m.&yr.Z)
					-spo&lyr.X*exp(-1*rf1_&yr*time&m.)
					*cdf('normal',spo&m.&yr.Z-stdret&lyr.a*(time&m.**0.5)))*incfee;
 spo&m.&yr.del=spo&m.&yr.S*cdf('normal',spo&m.&yr.Z)*0.01*incfee;
 spo&m.&yr.veg=spo&m.&yr.S*pdf('normal',spo&m.&yr.Z)*0.01*(time&m.**0.5)*incfee;
* When flow=0, we assign S and X to be zero. Then S/X in Z is undefined
*       and we end end up getting all values to be missing, which is wrong;
 if spo&m.&yr.S=0
 then do; spo&m.&yr.val=0; spo&m.&yr.del=0; spo&m.&yr.veg=0; end;

* The index has to be up to lyr becuase in Jan, we can only have flow upto lyr;
 %do a = &beg_year %to &lyr;
	iflow&m.&a._&yr.S=iflow&a._&lyr.S*(1+gret&m.&yr);
	iflow&m.&a._&yr.Z=(log(iflow&m.&a._&yr.S/iflow&a._&lyr.X)+time&m.*(rf1_&yr+0.5*(stdret&lyr.a**2)))
							/(stdret&lyr.a*(time&m.**0.5));
 	iflow&m.&a._&yr.val=(iflow&m.&a._&yr.S*cdf('normal',iflow&m.&a._&yr.Z)
							-iflow&a._&lyr.X*exp(-1*rf1_&yr*time&m.)
							*cdf('normal',iflow&m.&a._&yr.Z-stdret&lyr.a*(time&m.**0.5)))*incfee;
 	iflow&m.&a._&yr.del=iflow&m.&a._&yr.S*cdf('normal',iflow&m.&a._&yr.Z)*0.01*incfee;
 	iflow&m.&a._&yr.veg=iflow&m.&a._&yr.S*pdf('normal',iflow&m.&a._&yr.Z)*0.01*(time&m.**0.5)*incfee;
* When flow=0, we assign S and X to be zero. Then S/X in Z is undefined
*       and we end end up getting all values to be missing, which is wrong;
 	if iflow&m.&a._&yr.S=0
 	then do; iflow&m.&a._&yr.val=0; iflow&m.&a._&yr.del=0; iflow&m.&a._&yr.veg=0; end;
 %end;

 if abs(man&m.&yr.del)<1e-6 and man&m.&yr.del^=. then man&m.&yr.del=0;
 if abs(spo&m.&yr.val)<1e-6 and spo&m.&yr.val^=. then spo&m.&yr.val=0;
 if abs(spo&m.&yr.del)<1e-6 and spo&m.&yr.del^=. then spo&m.&yr.del=0;
 if abs(spo&m.&yr.veg)<1e-6 and spo&m.&yr.veg^=. then spo&m.&yr.veg=0;

 %do a = &beg_year %to &lyr;
	if abs(iflow&m.&a._&yr.val)<1e-6 and iflow&m.&a._&yr.val^=. then iflow&m.&a._&yr.val=0;
 	if abs(iflow&m.&a._&yr.del)<1e-6 and iflow&m.&a._&yr.del^=. then iflow&m.&a._&yr.del=0;
 	if abs(iflow&m.&a._&yr.veg)<1e-6 and iflow&m.&a._&yr.veg^=. then iflow&m.&a._&yr.veg=0;
 %end;

 inv&m.&yr.val = 0;
 inv&m.&yr.del = 0;
 inv&m.&yr.veg = 0;

 %do a = &beg_year %to &lyr;
 	inv&m.&yr.val = inv&m.&yr.val + iflow&m.&a._&yr.val;
 	inv&m.&yr.del = inv&m.&yr.del + iflow&m.&a._&yr.del;
 	inv&m.&yr.veg = inv&m.&yr.veg + iflow&m.&a._&yr.veg;
 %end;

 man&m.&yr.val=man&lyr*(1+gret&m.&yr);

 value&m.&yr = man&m.&yr.val + spo&m.&yr.val + inv&m.&yr.val;
 delta&m.&yr = man&m.&yr.del + spo&m.&yr.del + inv&m.&yr.del;
 vega&m.&yr  =                 spo&m.&yr.veg + inv&m.&yr.veg;

* for month=1 to 11;
%end;

* 9) Setting certain values to zero for the years before the fund
*	the fund was born. Otherwise, we will have missing values;
* --------------------------------------------------------------;
 if year>&yr and minyear>&yr
 then
 		do;
		man&yr=0;
		spo&yr.S=0;
		spo&yr.X=0;
 		%do a = 1987 %to &yr;
			iflow&a._&yr.S=0;
			iflow&a._&yr.X=0;
		%end;
		end;

* 10) Creating flags - to identify if there are problems;
* ------------------------------------------------------;
 if inc&yr<0 and inc&yr^=. and year=&yr
 then flag&yr.01=1;

 if diffinc&yr^=0 and inc&yr^=. and inc&yr.a^=. and year=&yr
 then flag&yr.02=1;

 if diffman&yr^=0 and man&yr^=. and man&yr.a^=. and year=&yr
 then flag&yr.03=1;

* managers wealth less than AUM;
 if iflow&yr.t<=0 and spo&yr.St2<=0 and man&yr.t2<=0 and 
		iflow&yr.t^=. and spo&yr.St2^=. and man&yr.t2^=. and year=&yr
 then flag&yr.04=1;

 if fracflow&yr<-1 and fracflow&yr^=. and year=&yr
 then flag&yr.05=1;

* S and X cannot be less than 0;
 if ((inv&yr<0 and inv&yr^=.) or (man&yr<0 and man&yr^=.) or 
		(spo&yr.S<0 and spo&yr.S^=.) or	(spo&yr.X<0 and spo&yr.X^=.)) 
		and year=&yr
 then flag&yr.06=1;
 %do a = &beg_year %to &yr;
  if ((iflow&a._&yr.S<0 and iflow&a._&yr.S^=.) or (iflow&a._&yr.X<0 and iflow&a._&yr.X^=.)) 
			and year=&yr
	then flag&yr.06=1;
 %end;

 if ((inv&yr.f<0 and inv&yr.f^=.) or (man&yr.f<0 and man&yr.f^=.) or 
				(spo&yr.Sf<0 and spo&yr.Sf^=.) or	(spo&yr.Xf<0 and spo&yr.Xf^=.))
				and year=&yr
 then flag&yr.07=1;
 %do a = &beg_year %to &yr;
	if ((iflow&a._&yr.Sf<0 and iflow&a._&yr.Sf^=.) or (iflow&a._&yr.Xf<0 and iflow&a._&yr.Xf^=.))
				and year=&yr
 	then flag&yr.07=1;
 %end;

* X can end up being greater than 1. so dont include X below;
 if ((inv&yr.f>1 and inv&yr.f^=.) or (man&yr.f>1 and man&yr.f^=.) or 
		(spo&yr.Sf>1 and spo&yr.Sf^=.)) and year=&yr
 then flag&yr.08=1;
 %do a = &beg_year %to &yr;
	if ((iflow&a._&yr.Sf>1 and iflow&a._&yr.Sf^=.)) and year=&yr
 	then flag&yr.08=1;
 %end;

* S cannot be greater than X;
 if ((spo&yr.S>spo&yr.X and spo&yr.S^=. and spo&yr.X^=.)) and year=&yr
 then flag&yr.09=1;
 %do a = &beg_year %to &yr;
 	if (iflow&a._&yr.S>iflow&a._&yr.X and iflow&a._&yr.S^=. and 
			iflow&a._&yr.X^=.) and year=&yr
 	then flag&yr.09=1;
 %end;

* Delta cannot be greater than be 0.01 of S;
 if (spo&yr.del>(0.01*spo&yr.S) and spo&yr.del^=. and spo&yr.S^=.)
		and year=&yr
 then flag&yr.10=1;
 %do a = &beg_year %to &yr;
 	if (iflow&a._&yr.del>(0.01*iflow&a._&yr.S) and iflow&a._&yr.del^=. and 
			iflow&a._&yr.S^=.) and year=&yr
 	then flag&yr.10=1;
 %end;

 if ((man&yr<0 and man&yr^=.) or (man&yr.del<0 and man&yr.del^=.)) 
	and year=&yr
 then flag&yr.11=1;

 if ((spo&yr.val<0 and spo&yr.val^=.) or (spo&yr.del<0 and spo&yr.del^=.) or 
				(spo&yr.veg<0 and spo&yr.veg^=.)) and year=&yr
 then flag&yr.12=1;

 %do a = &beg_year %to &yr;
 	if ((iflow&a._&yr.val<0 and iflow&a._&yr.val^=.) or 
			(iflow&a._&yr.del<0 and iflow&a._&yr.del^=.) or 
			(iflow&a._&yr.veg<0 and iflow&a._&yr.veg^=.)) and year=&yr
 	then flag&yr.13=1;
 %end;

 if ((value&yr<0 and value&yr^=.) or (delta&yr<0 and delta&yr^=.) or 
				(vega&yr<0 and vega&yr^=.)) and year=&yr
 then flag&yr.14=1;

 if diffiflow&yr.t^=0 and diffiflow&yr.t^=. and year=&yr 
 then flag&yr.15=1;

 if S&yr>X&yr  and S&yr^=. and X&yr^=. and year=&yr
 then flag&yr.16=1;

 if (int(ret&yr*10000)/10000)>(int(gret&yr*10000)/10000) 
 and ret&yr^=. and gret&yr^=. and year=&yr 
 then flag&yr.17=1;

 if convd&yr=0 and convd&yr^=. and ret&yr^=. and gret&yr^=. and year=&yr
 then flag&yr.18=1;

 if gret&yr.h<gret&yr.l and gret&yr.h^=. and gret&yr.l^=. and year=&yr
 then flag&yr.19=1;

 if minyear=&yr and inc&yr.iflow&lyr._&lyr>0 and 
		inc&yr.iflow&lyr._&lyr^=. and year=&yr 
 then flag&yr.20=1;


 %do m = 1 %to 11;
 %let n=%eval(20+&m);
 %let p=%eval(31+&m);
 %let q=%eval(42+&m);
 %let r=%eval(53+&m);
 %let s=%eval(64+&m);
 %let t=%eval(75+&m);

* MONTHLY FLAGS; 
* Note that for monthly, we will have flows only till previous year to go by;
*	Therefore index a will go only upto lyr;
* S and X cannot be less than 0;
 if ((inv&m.&yr<0 and inv&m.&yr^=.) or (man&m.&yr<0 and man&m.&yr^=.) or 
		(spo&m.&yr.S<0 and spo&m.&yr.S^=.) or (spo&lyr.X<0 and spo&lyr.X^=.)) 
		and year=&yr
 then flag&yr.&n.=1;
 %do a = &beg_year %to &lyr;
  if ((iflow&m.&a._&yr.S<0 and iflow&m.&a._&yr.S^=.) or (iflow&a._&lyr.X<0 and iflow&a._&lyr.X^=.)) 
			and year=&yr
	then flag&yr.&n.=1;
 %end;

* Delta cannot be greater than be 0.01 of S;
 if (spo&m.&yr.del>(0.01*spo&m.&yr.S) and spo&m.&yr.del^=. and spo&m.&yr.S^=.)
		and year=&yr
 then flag&yr.&p.=1;
 %do a = &beg_year %to &lyr;
 	if (iflow&m.&a._&yr.del>(0.01*iflow&m.&a._&yr.S) and iflow&m.&a._&yr.del^=. and 
			iflow&m.&a._&yr.S^=.) and year=&yr
 	then flag&yr.&p.=1;
 %end;

 if ((man&m.&yr<0 and man&m.&yr^=.) or (man&m.&yr.del<0 and man&m.&yr.del^=.)) 
	and year=&yr
 then flag&yr.&q.=1;

 if ((spo&m.&yr.val<0 and spo&m.&yr.val^=.) or (spo&m.&yr.del<0 and spo&m.&yr.del^=.) or 
				(spo&m.&yr.veg<0 and spo&m.&yr.veg^=.)) and year=&yr
 then flag&yr.&r.=1;

 %do a = &beg_year %to &lyr;
 	if ((iflow&m.&a._&yr.val<0 and iflow&m.&a._&yr.val^=.) or 
			(iflow&m.&a._&yr.del<0 and iflow&m.&a._&yr.del^=.) or 
			(iflow&m.&a._&yr.veg<0 and iflow&m.&a._&yr.veg^=.)) and year=&yr
 	then flag&yr.&s.=1;
 %end;

 if ((value&m.&yr<0 and value&m.&yr^=.) or (delta&m.&yr<0 and delta&m.&yr^=.) or 
				(vega&m.&yr<0 and vega&m.&yr^=.)) and year=&yr
 then flag&yr.&t.=1;

* for the m=1 to 11 loop;
%end;

* Flags for returns;
* gross returns cannot be less than net return if net return is positive;

 %do m = 1 %to 12;
 %let n=%eval(86+&m);
 %let p=%eval(98+&m);
 %let q=%eval(110+&m);
 %let r=%eval(122+&m);

* Since the monthly returns are getting backed out, there will be slight differences 
*	(in probably the 15th decimal), and hence we are taing the absolute below;
 diffmthret&m.&yr=abs(mthgret&m.&yr-mthret&m.&yr);
 diffcumret&m.&yr=abs(gret&m.&yr-ret&m.&yr);

 if mthret&m.&yr>0 and mthgret&m.&yr<mthret&m.&yr and diffmthret&m.&yr>0.001 and mthgret&m.&yr^=. and mthret&m.&yr^=.
 then flag&yr.&n=1;
 if mthret&m.&yr<=0 and mthgret&m.&yr>mthret&m.&yr and diffmthret&m.&yr>0.001 and mthgret&m.&yr^=. and mthret&m.&yr^=.
 then flag&yr.&p=1;
* Note that flag122 and flag134 are effectively taking care of annual returns;
 if ret&m.&yr>0 and gret&m.&yr<ret&m.&yr and diffcumret&m.&yr>0.001 and gret&m.&yr^=. and ret&m.&yr^=.
 then flag&yr.&q=1;
 if ret&m.&yr<=0 and gret&m.&yr>ret&m.&yr and diffcumret&m.&yr>0.001 and gret&m.&yr^=. and ret&m.&yr^=.
 then flag&yr.&r=1;
 %end;

run;
/*
proc print data=temp1987;
* var code year mthret11987 mthgret11987 flag198787;
 var code year aum1987 aum1986 ret1987 inc1987 tax1987 iflow1987ta iflow1987t aum1987 spo1987St man1987t man1986 man1987;
where year=1987;
run;
*/
/*
proc print data=temp&yr;
where year=&yr and uw1&yr^=.;
*where inv11988val=0 and spo11988val^=. and year=1988;
*where code=2915;
*where flag198888=1 and year=1988;
*var code year minyear libor1988 mthret111988 gret111988 ret11988 gret11988 flag198887 gret1988 ret1988;
*var code year minyear bhret1_1 bhret2_2 bhret1_2 libor1988 mthret111988 gret111988 mthret221988 gret221988 ret11988 gret11988 ret21988 gret21988 flag198887 gret1988 ret1988 incfee;
var code year time1 man1&yr man&lyr gret1&yr inv1&yr inv&lyr 
 man1&yr.del spo1&yr.S spo&lyr.S spo1&yr.Z spo&lyr.X
rf1_&yr stdret&lyr.a spo1&yr.val spo1&yr.S spo1&yr.Z spo&lyr.X rf1_&yr spo1&yr.del
 spo1&yr.veg;
*var code year time1 man11988 man&lyr gret1&yr inv1&yr inv&lyr 
 man1&yr.del spo1&yr.S spo&lyr.S spo1&yr.Z spo&lyr.X
rf1_&yr stdret&lyr.a spo1&yr.val spo1&yr.S spo1&yr.Z spo&lyr.X rf1_&yr spo1&yr.del
 spo1&yr.veg;
run;
*/
/*
proc print data=temp1987;
*var code year X1986 X1987 Xprior1987;
var code year S1987 S121987 Sprior1987 inc1987 tax1987 flow1987 iflow1987t iflow1987_1987S libor1987;
where S121987^=. and Sprior1987^=. and S121987>Sprior1987 and year=1987 ;
run;
*/
*where year=1988 and flag198888=1;
*var code year minyear 
mthret11988 mthret21988 mthret31988  mthret41988  mthret51988  mthret61988  mthret71988  mthret81988  mthret91988  mthret101988  mthret111988 mthret121988
ret11988 ret21988 ret31988  ret41988  ret51988  ret61988  ret71988  ret81988  ret91988  ret101988  ret111988 ret121988
mthgret11988 mthgret21988 mthgret31988 mthgret41988 mthgret51988 mthgret61988 mthgret71988 mthgret81988 mthgret91988 mthgret101988 mthgret111988 mthgret121988 
gret11988 gret21988 gret31988 gret41988 gret51988 gret61988 gret71988 gret81988 gret91988 gret101988 gret111988 gret121988 
libor1988 incfee 
convd111988 gret111988 gret111988min gret111988minmax gret111988l gret111988hNr 
gret111988hDr gret111988h gret111988l1 gret111988h1 prev111988 
startgret111988 diff111988 startdiff111988 prevdiff111988 prev111988 prevret111988t;
*where inv11988val=0 and spo11988val^=. and year=1988;
*where code=2915;
*where flag198888=1 and year=1988;
*var code year minyear libor1988 mthret111988 gret111988 ret11988 gret11988 flag198887 gret1988 ret1988;
*var code year bhret1_1 bhret2_2 bhret1_2 libor1988 mthret111987 gret111987 
	mthret221987 gret221987 ret11987 gret11987 ret21987 gret21987 gret21987 flag198788 
	gret1987 ret1987 incfee;
*var bhret1_1 bhret1_2 bhret1_3 bhret1_4 bhret1_5 bhret1_6 bhret1_7 bhret1_8 bhret1_9 bhret1_10 bhret1_11 bhret1_12 
bhret1_1 bhret2_2 bhret3_3 bhret4_4 bhret5_5 bhret6_6 bhret7_7 bhret8_8 bhret9_9 bhret10_10 bhret11_11 bhret12_12 ;
*var code year time1 man1&yr man&lyr gret1&yr inv1&yr inv&lyr 
 man1&yr.del spo1&yr.S spo&lyr.S spo1&yr.Z spo&lyr.X
rf1_&yr stdret&lyr.a spo1&yr.val spo1&yr.S spo1&yr.Z spo&lyr.X rf1_&yr spo1&yr.del
 spo1&yr.veg;
*var code year time1 man11988 man&lyr gret1&yr inv1&yr inv&lyr 
 man1&yr.del spo1&yr.S spo&lyr.S spo1&yr.Z spo&lyr.X
rf1_&yr stdret&lyr.a spo1&yr.val spo1&yr.S spo1&yr.Z spo&lyr.X rf1_&yr spo1&yr.del
 spo1&yr.veg;


* Flags 02, 03 and 15 wont be zero. so look at proc means below. 
*		As long as means are small enough, we are happy;
* For 1987, there are no prior years flows, so flags 65-75 will not be created;
%if &yr=&beg_year
%then
%do;
*proc means data=temp&yr;
* var flag&yr.01-flag&yr.64 flag&yr.76-flag&yr.99 flag&yr.100-flag&yr.134 ;
* where year=&yr;
* title "&yr flag 2,3, and 15 wont be zero";
*run;
%end;
%else
%do;
*proc means data=temp&yr;
* var flag&yr.01-flag&yr.99 flag&yr.100-flag&yr.134 ;
* where year=&yr;
* title "&yr flag 2,3, and 15 wont be zero";
*run;
%end;
*proc means data=temp&yr;
* var diffinc&yr;
* where year=&yr and flag&yr.02=1;
* title "&yr diffinc";
*run;
*proc means data=temp&yr;
* var diffman&yr;
* where year=&yr and flag&yr.03=1;
* title "&yr diffman";
*run;
*proc means data=temp&yr;
 *var diffiflow&yr.t;
 *where year=&yr and flag&yr.15=1;
 *title "&yr diffiflowt";
*run;
*proc means data=temp&yr;
 *var diffgret&yr;
 *where year=&yr ;
 *title "&yr difference in annual gross return and compounded monthly gross return";
*run;
* shows how many people had convergence - all of them should converge; 
* Sometimes, it should converge only once. That is mean=1;
*	if it converges at 2 places, we can let it go;
*proc means data=temp&yr n mean median min max ;
 *where year=&yr;
 *var convd&yr convd1&yr convd2&yr convd3&yr convd4&yr convd5&yr convd6&yr 
	 convd7&yr convd8&yr convd9&yr convd10&yr convd11&yr convd12&yr 

	 ret1&yr gret1&yr mthret1&yr mthgret1&yr
	 man1&yr.val spo1&yr.val inv1&yr.val value1&yr 
	 man1&yr.del spo1&yr.del inv1&yr.del delta1&yr 
	 spo1&yr.veg inv1&yr.veg vega1&yr
	 uw1&yr uw1&yr.p uw1&yr.px 
	 ret2&yr gret2&yr mthret2&yr mthgret2&yr
	 man2&yr.val spo2&yr.val inv2&yr.val value2&yr 
	 man2&yr.del spo2&yr.del inv2&yr.del delta2&yr 
	 spo2&yr.veg inv2&yr.veg vega2&yr
	 uw2&yr uw2&yr.p uw2&yr.px
	 ret3&yr gret3&yr mthret3&yr mthgret3&yr
	 man3&yr.val spo3&yr.val inv3&yr.val value3&yr 
	 man3&yr.del spo3&yr.del inv3&yr.del delta3&yr 
	 spo3&yr.veg inv3&yr.veg vega3&yr
	 uw3&yr uw3&yr.p uw3&yr.px
	 ret4&yr gret4&yr mthret4&yr mthgret4&yr
	 man4&yr.val spo4&yr.val inv4&yr.val value4&yr 
	 man4&yr.del spo4&yr.del inv4&yr.del delta4&yr 
	 spo4&yr.veg inv4&yr.veg vega4&yr
	 uw4&yr uw4&yr.p uw4&yr.px
	 ret5&yr gret5&yr mthret5&yr mthgret5&yr
	 man5&yr.val spo5&yr.val inv5&yr.val value5&yr 
	 man5&yr.del spo5&yr.del inv5&yr.del delta5&yr 
	 spo5&yr.veg inv5&yr.veg vega5&yr
	 uw5&yr uw5&yr.p uw5&yr.px
	 ret6&yr gret6&yr mthret6&yr mthgret6&yr
	 man6&yr.val spo6&yr.val inv6&yr.val value6&yr 
	 man6&yr.del spo6&yr.del inv6&yr.del delta6&yr 
	 spo6&yr.veg inv6&yr.veg vega6&yr
	 uw6&yr uw6&yr.p uw6&yr.px
	 ret7&yr gret7&yr mthret7&yr mthgret7&yr
	 man7&yr.val spo7&yr.val inv7&yr.val value7&yr 
	 man7&yr.del spo7&yr.del inv7&yr.del delta7&yr 
	 spo7&yr.veg inv7&yr.veg vega7&yr
	 uw7&yr uw7&yr.p uw7&yr.px
	 ret8&yr gret8&yr mthret8&yr mthgret8&yr
	 man8&yr.val spo8&yr.val inv8&yr.val value8&yr 
	 man8&yr.del spo8&yr.del inv8&yr.del delta8&yr 
	 spo8&yr.veg inv8&yr.veg vega8&yr
	 uw8&yr uw8&yr.p uw8&yr.px
	 ret9&yr gret9&yr mthret9&yr mthgret9&yr
	 man9&yr.val spo9&yr.val inv9&yr.val value9&yr 
	 man9&yr.del spo9&yr.del inv9&yr.del delta9&yr 
	 spo9&yr.veg inv9&yr.veg vega9&yr
	 uw9&yr uw9&yr.p uw9&yr.px
	 ret10&yr gret10&yr mthret10&yr mthgret10&yr
	 man10&yr.val spo10&yr.val inv10&yr.val value10&yr 
	 man10&yr.del spo10&yr.del inv10&yr.del delta10&yr 
 	 spo10&yr.veg inv10&yr.veg vega10&yr
	 uw10&yr uw10&yr.p uw10&yr.px
	 ret11&yr gret11&yr mthret11&yr mthgret11&yr
	 man11&yr.val spo11&yr.val inv11&yr.val value11&yr 
	 man11&yr.del spo11&yr.del inv11&yr.del delta11&yr 
	 spo11&yr.veg inv11&yr.veg vega11&yr
	 uw11&yr uw11&yr.p uw11&yr.px
	 ret12&yr gret12&yr mthret12&yr mthgret12&yr
	 man&yr.val spo&yr.val inv&yr.val value&yr 
	 man&yr.del spo&yr.del inv&yr.del delta&yr 
	 spo&yr.veg inv&yr.veg vega&yr
	 uw12&yr uw12&yr.p uw12&yr.px

	 libor&ldyr uw&yr uw&yr.p uw&yr.px uwprior&yr uwprior&yr.p uwprior&yr.px
 	 ret&yr gret&yr iflow&yr.t fracflow&yr 
	 man&yr spo&yr.S inv&yr aum&yr 
	 man&yr.f spo&yr.Sf inv&yr.f;

 *title "&yr summary stats";
*run;

* We have temporarily closed this out to check if our program is running;

 proc delete data=temp&lyr;
 run;

data temp&yr;
 set temp&yr;
 drop gret&yr.min gret&yr.minmax gret&yr.l gret&yr.hNr
	gret&yr.hNr	gret&yr.hDr	gret&yr.h gret&yr.l1 gret&yr.h1 startgret&yr diffgret&yr 
    i&yr j&yr ret&yr.t diff&yr prev&yr prevret&yr.t prevdiff&yr startdiff&yr
	diffinc&yr diffman&yr diffiflow&yr.t convd&yr spo&yr.Z
	ret1&yr.t ret2&yr.t ret3&yr.t ret4&yr.t ret5&yr.t ret6&yr.t ret7&yr.t ret8&yr.t ret9&yr.t ret10&yr.t ret11&yr.t ret12&yr.t 
	convd1&yr convd2&yr convd3&yr convd4&yr convd5&yr convd6&yr convd7&yr convd8&yr convd9&yr convd10&yr convd11&yr convd12&yr
	ret1&yr ret2&yr ret3&yr ret4&yr ret5&yr ret6&yr ret7&yr ret8&yr ret9&yr ret10&yr ret11&yr ret12&yr
	spo1&yr.S spo2&yr.S spo3&yr.S spo4&yr.S spo5&yr.S spo6&yr.S spo7&yr.S spo8&yr.S spo9&yr.S spo10&yr.S spo11&yr.S 
	spo1&yr.Z spo2&yr.Z spo3&yr.Z spo4&yr.Z spo5&yr.Z spo6&yr.Z spo7&yr.Z spo8&yr.Z spo9&yr.Z spo10&yr.Z spo11&yr.Z 
	S1&yr S2&yr S3&yr S4&yr S5&yr S6&yr S7&yr S8&yr S9&yr S10&yr S11&yr S12&yr
	rf1_&yr rf2_&yr rf3_&yr rf4_&yr rf5_&yr rf6_&yr rf7_&yr rf8_&yr rf9_&yr rf10_&yr rf11_&yr 
	mthret1&yr mthret2&yr mthret3&yr mthret4&yr mthret5&yr mthret6&yr mthret7&yr mthret8&yr mthret9&yr mthret10&yr mthret11&yr mthret12&yr 
	gret1&yr.min gret2&yr.min gret3&yr.min gret4&yr.min gret5&yr.min gret6&yr.min gret7&yr.min gret8&yr.min gret9&yr.min gret10&yr.min gret11&yr.min gret12&yr.min 
	gret1&yr.minmax gret2&yr.minmax gret3&yr.minmax gret4&yr.minmax gret5&yr.minmax gret6&yr.minmax gret7&yr.minmax gret8&yr.minmax gret9&yr.minmax gret10&yr.minmax gret11&yr.minmax gret12&yr.minmax 
	gret1&yr.l gret2&yr.l gret3&yr.l gret4&yr.l gret5&yr.l gret6&yr.l gret7&yr.l gret8&yr.l gret9&yr.l gret10&yr.l gret11&yr.l gret12&yr.l 
	gret1&yr.hNr gret2&yr.hNr gret3&yr.hNr gret4&yr.hNr gret5&yr.hNr gret6&yr.hNr gret7&yr.hNr gret8&yr.hNr gret9&yr.hNr gret10&yr.hNr gret11&yr.hNr gret12&yr.hNr 
	gret1&yr.hDr gret2&yr.hDr gret3&yr.hDr gret4&yr.hDr gret5&yr.hDr gret6&yr.hDr gret7&yr.hDr gret8&yr.hDr gret9&yr.hDr gret10&yr.hDr gret11&yr.hDr gret12&yr.hDr 
	gret1&yr.h gret2&yr.h gret3&yr.h gret4&yr.h gret5&yr.h gret6&yr.h gret7&yr.h gret8&yr.h gret9&yr.h gret10&yr.h gret11&yr.h gret12&yr.h 
	gret1&yr.l1 gret2&yr.l1 gret3&yr.l1 gret4&yr.l1 gret5&yr.l1 gret6&yr.l1 gret7&yr.l1 gret8&yr.l1 gret9&yr.l1 gret10&yr.l1 gret11&yr.l1 gret12&yr.l1 
	gret1&yr.h1 gret2&yr.h1 gret3&yr.h1 gret4&yr.h1 gret5&yr.h1 gret6&yr.h1 gret7&yr.h1 gret8&yr.h1 gret9&yr.h1 gret10&yr.h1 gret11&yr.h1 gret12&yr.h1 
	prev1&yr prev2&yr prev3&yr prev4&yr prev5&yr prev6&yr prev7&yr prev8&yr prev9&yr prev10&yr prev11&yr prev12&yr 
	startgret1&yr startgret2&yr startgret3&yr startgret4&yr startgret5&yr startgret6&yr startgret7&yr startgret8&yr startgret9&yr startgret10&yr startgret10&yr startgret12&yr 
	diff1&yr diff2&yr diff3&yr diff4&yr diff5&yr diff6&yr diff7&yr diff8&yr diff9&yr diff10&yr diff11&yr diff12&yr 
	startdiff1&yr startdiff2&yr startdiff3&yr startdiff4&yr startdiff5&yr startdiff6&yr startdiff7&yr startdiff8&yr startdiff9&yr startdiff10&yr startdiff11&yr startdiff12&yr 
	prevdiff1&yr prevdiff2&yr prevdiff3&yr prevdiff4&yr prevdiff5&yr prevdiff6&yr prevdiff7&yr prevdiff8&yr prevdiff9&yr prevdiff10&yr prevdiff11&yr prevdiff12&yr 
	prev1&yr prev2&yr prev3&yr prev4&yr prev5&yr prev6&yr prev7&yr prev8&yr prev9&yr prev10&yr prev11&yr prev12&yr 
	prevret1&yr.t prevret2&yr.t prevret3&yr.t prevret4&yr.t prevret5&yr.t prevret6&yr.t prevret7&yr.t prevret8&yr.t prevret9&yr.t prevret10&yr.t prevret11&yr.t prevret12&yr.t
	i1&yr i2&yr i3&yr i4&yr i5&yr i6&yr i7&yr i8&yr i9&yr i10&yr i11&yr i12&yr 
	j1&yr j2&yr j3&yr j4&yr j5&yr j6&yr j7&yr j8&yr j9&yr j10&yr j11&yr j12&yr 
	diffmthret1&yr diffmthret2&yr diffmthret3&yr diffmthret4&yr diffmthret5&yr diffmthret6&yr diffmthret7&yr diffmthret8&yr diffmthret9&yr diffmthret10&yr diffmthret11&yr diffmthret12&yr 
	diffcumret1&yr diffcumret2&yr diffcumret3&yr diffcumret4&yr diffcumret5&yr diffcumret6&yr diffcumret7&yr diffcumret8&yr diffcumret9&yr diffcumret10&yr diffcumret11&yr diffcumret12&yr ;
 %do a = &beg_year %to &lyr;
 drop iflow1&a._&yr.S iflow1&a._&yr.Z iflow2&a._&yr.S iflow2&a._&yr.Z
	  iflow3&a._&yr.S iflow3&a._&yr.Z iflow4&a._&yr.S iflow4&a._&yr.Z
	  iflow5&a._&yr.S iflow5&a._&yr.Z iflow6&a._&yr.S iflow6&a._&yr.Z
	  iflow7&a._&yr.S iflow7&a._&yr.Z iflow8&a._&yr.S iflow8&a._&yr.Z
	  iflow9&a._&yr.S iflow9&a._&yr.Z iflow10&a._&yr.S iflow10&a._&yr.Z
	  iflow11&a._&yr.S iflow11&a._&yr.Z 
	  iflow&a._&yr.Z;
 %end;
 if &yr=&beg_year 
 then do; drop flag&yr.01-flag&yr.64 flag&yr.76-flag&yr.99 flag&yr.100-flag&yr.134; end;
 else do; drop flag&yr.01-flag&yr.99 flag&yr.100-flag&yr.134; end;


/*

* Other variables that could be dropped?;

	gret1&yr gret2&yr gret3&yr gret4&yr gret5&yr gret6&yr gret7&yr gret8&yr gret9&yr gret10&yr gret11&yr gret12&yr 
	spo&yr.St1 inc&yr.spo&lyr spo&yr.St spo&yr.St1 spo&yr.Xt
	iflow&a._&yr.St1 inc&yr.iflow&a._&lyr 
	inc&yr.a tax&yr iflow&yr.ta man&yr.t iflow&yr.t diffiflow&yr.t
	iflow1986_&yr.St2 iflow&a._&yr.St2 spo&yr.St2 man&yr.t2 iflow&yr.t 
	spo&yr.St2 flagman&yr iflow&a._&yr.Xt spo&yr.Xt man&yr.a ;

*/
run;


%mend;
* Final dataset;
* =============;
%macro assign;
 %do i=&beg_year %to &end_year;
 %let j=%eval(&i+1);
 if year=&i 
 then
 	do;

	man1val=man1&i.val; 
	spo1val=spo1&i.val; 
	inv1val=inv1&i.val; 
	value1=value1&i;
	man1del=man1&i.del;
	spo1del=spo1&i.del; 
	inv1del=inv1&i.del;
	delta1=delta1&i;
	spo1veg=spo1&i.veg; 
	inv1veg=inv1&i.veg;
	vega1=vega1&i;
	gret1=gret1&i;	
	mthgret1=mthgret1&i;

	man2val=man2&i.val; 
	spo2val=spo2&i.val; 
	inv2val=inv2&i.val; 
	value2=value2&i;
	man2del=man2&i.del;
	spo2del=spo2&i.del; 
	inv2del=inv2&i.del;
	delta2=delta2&i;
	spo2veg=spo2&i.veg; 
	inv2veg=inv2&i.veg;
	vega2=vega2&i;
	gret2=gret2&i;
	mthgret2=mthgret2&i;

	man3val=man3&i.val; 
	spo3val=spo3&i.val; 
	inv3val=inv3&i.val; 
	value3=value3&i;
	man3del=man3&i.del;
	spo3del=spo3&i.del; 
	inv3del=inv3&i.del;
	delta3=delta3&i;
	spo3veg=spo3&i.veg; 
	inv3veg=inv3&i.veg;
	vega3=vega3&i;
	gret3=gret3&i;
	mthgret3=mthgret3&i;

	man4val=man4&i.val; 
	spo4val=spo4&i.val; 
	inv4val=inv4&i.val; 
	value4=value4&i;
	man4del=man4&i.del;
	spo4del=spo4&i.del; 
	inv4del=inv4&i.del;
	delta4=delta4&i;
	spo4veg=spo4&i.veg; 
	inv4veg=inv4&i.veg;
	vega4=vega4&i;
	gret4=gret4&i;
	mthgret4=mthgret4&i;

	man5val=man5&i.val; 
	spo5val=spo5&i.val; 
	inv5val=inv5&i.val; 
	value5=value5&i;
	man5del=man5&i.del;
	spo5del=spo5&i.del; 
	inv5del=inv5&i.del;
	delta5=delta5&i;
	spo5veg=spo5&i.veg; 
	inv5veg=inv5&i.veg;
	vega5=vega5&i;
	gret5=gret5&i;
	mthgret5=mthgret5&i;

	man6val=man6&i.val; 
	spo6val=spo6&i.val; 
	inv6val=inv6&i.val; 
	value6=value6&i;
	man6del=man6&i.del;
	spo6del=spo6&i.del; 
	inv6del=inv6&i.del;
	delta6=delta6&i;
	spo6veg=spo6&i.veg; 
	inv6veg=inv6&i.veg;
	vega6=vega6&i;
	gret6=gret6&i;
	mthgret6=mthgret6&i;

	man7val=man7&i.val; 
	spo7val=spo7&i.val; 
	inv7val=inv7&i.val; 
	value7=value7&i;
	man7del=man7&i.del;
	spo7del=spo7&i.del; 
	inv7del=inv7&i.del;
	delta7=delta7&i;
	spo7veg=spo7&i.veg; 
	inv7veg=inv7&i.veg;
	vega7=vega7&i;
	gret7=gret7&i;
	mthgret7=mthgret7&i;

	man8val=man8&i.val; 
	spo8val=spo8&i.val; 
	inv8val=inv8&i.val; 
	value8=value8&i;
	man8del=man8&i.del;
	spo8del=spo8&i.del; 
	inv8del=inv8&i.del;
	delta8=delta8&i;
	spo8veg=spo8&i.veg; 
	inv8veg=inv8&i.veg;
	vega8=vega8&i;
	gret8=gret8&i;
	mthgret8=mthgret8&i;

	man9val=man9&i.val; 
	spo9val=spo9&i.val; 
	inv9val=inv9&i.val; 
	value9=value9&i;
	man9del=man9&i.del;
	spo9del=spo9&i.del; 
	inv9del=inv9&i.del;
	delta9=delta9&i;
	spo9veg=spo9&i.veg; 
	inv9veg=inv9&i.veg;
	vega9=vega9&i;
	gret9=gret9&i;
	mthgret9=mthgret9&i;

	man10val=man10&i.val; 
	spo10val=spo10&i.val; 
	inv10val=inv10&i.val; 
	value10=value10&i;
	man10del=man10&i.del;
	spo10del=spo10&i.del; 
	inv10del=inv10&i.del;
	delta10=delta10&i;
	spo10veg=spo10&i.veg; 
	inv10veg=inv10&i.veg;
	vega10=vega10&i;
	gret10=gret10&i;
	mthgret10=mthgret10&i;

	man11val=man11&i.val; 
	spo11val=spo11&i.val; 
	inv11val=inv11&i.val; 
	value11=value11&i;
	man11del=man11&i.del;
	spo11del=spo11&i.del; 
	inv11del=inv11&i.del;
	delta11=delta11&i;
	spo11veg=spo11&i.veg; 
	inv11veg=inv11&i.veg;
	vega11=vega11&i;
	gret11=gret11&i;
	mthgret11=mthgret11&i;
	mthgret12=mthgret12&i;

	iflow=iflow&i.t;
	fracflow=fracflow&i;

* We are later on comparing gross return with what you should have earned in year t before
*	inc fees can be paid out. Note Libor87 is the risk-free rate as of Dec86. THerefore
*	we compare gross_ret87 with Libor87;
	hurdle=libor&i;
	ldhurdle=libor&j;
	inc=inc&i;
	man=man&i;
	spo=spo&i.S;
	inv=inv&i;
	manf=man&i.f;
	spof=spo&i.Sf;
	invf=inv&i.f;
	manval=man&i.val;
	spoval=spo&i.val;
	invval=inv&i.val;
	value=value&i;	
	mandel=man&i.del;
	spodel=spo&i.del;
	invdel=inv&i.del;
	delta=delta&i;
	spoveg=spo&i.veg;
	invveg=inv&i.veg;
	vega=vega&i;
	gret=gret&i;
		end;

 	drop gret&i iflow&i.t fracflow&i inc&i man&i spo&i.S
	inv&i man&i.f spo&i.Sf inv&i.f man&i.val spo&i.val inv&i.val
	man&i.del spo&i.del inv&i.del spo&i.veg inv&i.veg  libor&j 
	aum&i ret&i stdret&i rf&i 
	spo&i.S spo&i.X spo&i.Sf spo&i.Xf
	inc&i flow&i flow&i.g inc&i.a iflow&i.ta man&i.t 
	iflow&i.t fracflow&i spo&i.St2 man&i.t2 iflow&i._&i.S
	man&i flagman&i inv&i man&i.a X&i S&i	
	man&i.f spo&i.Sf inv&i.f

	gret1&i man1&i.val spo1&i.val inv1&i.val value1&i 
	man1&i.del spo1&i.del inv1&i.del delta1&i 
	spo1&i.veg inv1&i.veg vega1&i mthgret1&i

	gret2&i man2&i.val spo2&i.val inv2&i.val value2&i 
	man2&i.del spo2&i.del inv2&i.del delta2&i 
	spo2&i.veg inv2&i.veg vega2&i mthgret2&i

	gret3&i man3&i.val spo3&i.val inv3&i.val value3&i 
	man3&i.del spo3&i.del inv3&i.del delta3&i 
	spo3&i.veg inv3&i.veg vega3&i mthgret3&i

	gret4&i man4&i.val spo4&i.val inv4&i.val value4&i 
	man4&i.del spo4&i.del inv4&i.del delta4&i 
	spo4&i.veg inv4&i.veg vega4&i mthgret4&i

	gret5&i man5&i.val spo5&i.val inv5&i.val value5&i 
	man5&i.del spo5&i.del inv5&i.del delta5&i 
	spo5&i.veg inv5&i.veg vega5&i mthgret5&i

	gret6&i man6&i.val spo6&i.val inv6&i.val value6&i 
	man6&i.del spo6&i.del inv6&i.del delta6&i 
	spo6&i.veg inv6&i.veg vega6&i mthgret6&i

	gret7&i man7&i.val spo7&i.val inv7&i.val value7&i 
	man7&i.del spo7&i.del inv7&i.del delta7&i 
	spo7&i.veg inv7&i.veg vega7&i mthgret7&i

	gret8&i man8&i.val spo8&i.val inv8&i.val value8&i 
	man8&i.del spo8&i.del inv8&i.del delta8&i 
	spo8&i.veg inv8&i.veg vega8&i mthgret8&i

	gret9&i man9&i.val spo9&i.val inv9&i.val value9&i 
	man9&i.del spo9&i.del inv9&i.del delta9&i 
	spo9&i.veg inv9&i.veg vega9&i mthgret9&i

	gret10&i man10&i.val spo10&i.val inv10&i.val value10&i 
	man10&i.del spo10&i.del inv10&i.del delta10&i 
	spo10&i.veg inv10&i.veg vega10&i mthgret10&i

	gret11&i man11&i.val spo11&i.val inv11&i.val value11&i 
	man11&i.del spo11&i.del inv11&i.del delta11&i 
	spo11&i.veg inv11&i.veg vega11&i mthgret11&i mthgret12&i

	gret12&i;
 %end;
%mend;

options nonotes nosource2 dkrOCond=noWarning;
filename myfile 'c:\temp\mylog2.log';
proc printto log=myfile;
run;

* year(lagyear, year, leadyear);
%year(1986,1987,1988);
%year(1987,1988,1989);
%year(1988,1989,1990);
%year(1989,1990,1991);
%year(1990,1991,1992);
%year(1991,1992,1993);
%year(1992,1993,1994);
%year(1993,1994,1995);
%year(1994,1995,1996);
%year(1995,1996,1997);
%year(1996,1997,1998);
%year(1997,1998,1999);
%year(1998,1999,2000);
%year(1999,2000,2001);
%year(2000,2001,2002);
%year(2001,2002,2003);
%year(2002,2003,2004);
%year(2003,2004,2005);
%year(2004,2005,2006);
%year(2005,2006,2007);
%year(2006,2007,2008);
%year(2007,2008,2009);
%year(2008,2009,2010);
%year(2009,2010,2011);
%year(2010,2011,2012);
%year(2011,2012,2013);
%year(2012,2013,2014);
%year(2013,2014,2015);
options notes source2 dkrOCond=noWarning;
proc printto log;
run;
data final;
 set temp2014;
 where year>=&beg_year;

 %assign;

 keep &id year 
man1val spo1val inv1val value1 
  	man1del spo1del inv1del delta1 
	spo1veg inv1veg vega1 gret1	mthgret1
	man2val spo2val inv2val value2 
  	man2del spo2del inv2del delta2 
	spo2veg inv2veg vega2 gret2	mthgret2
	man3val spo3val inv3val value3 
  	man3del spo3del inv3del delta3 
	spo3veg inv3veg vega3 gret3	mthgret3
	man4val spo4val inv4val value4 
  	man4del spo4del inv4del delta4 
	spo4veg inv4veg vega4 gret4	mthgret4
	man5val spo5val inv5val value5 
  	man5del spo5del inv5del delta5 
	spo5veg inv5veg vega5 gret5	mthgret5
	man6val spo6val inv6val value6 
  	man6del spo6del inv6del delta6 
	spo6veg inv6veg vega6 gret6	mthgret6
	man7val spo7val inv7val value7 
  	man7del spo7del inv7del delta7 
	spo7veg inv7veg vega7 gret7	mthgret7
	man8val spo8val inv8val value8 
  	man8del spo8del inv8del delta8 
	spo8veg inv8veg vega8 gret8	mthgret8
	man9val spo9val inv9val value9 
  	man9del spo9del inv9del delta9 
	spo9veg inv9veg vega9 gret9	mthgret9
	man10val spo10val inv10val value10 
  	man10del spo10del inv10del delta10 
	spo10veg inv10veg vega10 gret10	mthgret10
	man11val spo11val inv11val value11 
  	man11del spo11del inv11del delta11 
	spo11veg inv11veg vega11 gret11	mthgret11 mthgret12
 	manval spoval invval value 
	mandel spodel invdel delta 
	spoveg invveg vega gret

	inc hurdle ldhurdle
	incfee highwater hurdrate offsh mgmtfee
	gret iflow fracflow 
	man spo inv aum 
	manf spof invf;

run;
data d.final;
set final;
optdelta=spodel+invdel;
run;
* Get the data into month-year observations;
data monthlydelta;
set final;
month=12;
invdelta=invdel;
keep id year month gret delta vega optdelta mandel invdelta mthgret12;
run;
data monthlydelta; set monthlydelta; rename mthgret12=mnthgret; run;
%macro monthdata();
%do i=1 %to 11;
data month&i;
set final;
month=&i;
invdelta&i=inv&i.del;
keep id year month gret&i delta&i vega&i invdelta&i man&i.del mthgret&i;
run;
data month&i;
set month&i;
rename gret&i=gret;
rename delta&i=delta;
rename vega&i=vega;
rename invdelta&i=invdelta;
rename man&i.del=mandel;
rename mthgret&i=mnthgret;
run;
proc append base=monthlydelta data=month&i force;
run;
proc delete data=month&i; run;
%end;
%mend;
%monthdata();
data monthlydelta;
set monthlydelta;
monthnum=12*(year-1980)+(month-1);
run;
proc sort data=monthlydelta;
by id monthnum;
run;

data d.delta;
set monthlydelta;
run;
