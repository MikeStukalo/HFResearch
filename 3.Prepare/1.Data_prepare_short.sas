*******************************************************************************
This code creates annual databases for regression analysis.
The code is created by Mikhail Stukalo. July 2015
*******************************************************************************;

*******************************************************************************
Input files: 	union_names - mapping file between ADV and 
		adv_base-  database created from SEC_7B1
		Union_returns - Union returns database
*******************************************************************************;


* Set library path. !!!!!!!Check the path!!!!!!!!!!!!;

libname data 'c:\Users\Mikhail\Desktop\Funds\Data\';


* This part adds calendar values to months in Union return database;
data union_returns;
set data.union_returns;
retdate=date;
drop date;
run;

data union_returns;
set union_returns;
format retdate date9.;
run;

* We breakup dates by years and mmonths;
data union_returns;
set union_returns;
year= year(retdate);
month= month(retdate);
run;

proc sort data=union_returns;
by id year month;
quit;


* We breakup dates by years and months in ADV file;
data adv_base;
set data.adv_base;
keep Fund_id Fund_name DateSubmitted PctOwned;
run;

data adv_base;
set adv_base;
year= year(DateSubmitted);
month= month(DateSubmitted);
run;
* Match returns to matched ids;

proc sql;
create table temp1
as select distinct a.*,b.* 
from union_returns as a left join data.union_names (keep=fund_id id) as b
on a.id=b.id;
quit;

data temp1;
set temp1;
where Fund_id;
run;

proc sort data=temp1;
by Fund_id year month id;
quit;


* Match ADV with matched names and returns;

proc sql;
create table temp2
as select distinct a.*,b.* 
from temp1 as a left join adv_base as b
on a.fund_id=b.fund_id and a.year=b.year and a.month=b.month;
quit;

proc sort data=temp2;
by Fund_id year month id;
quit;

*clean up the dataset;
data hf_database;
set temp2;
keep fund_id id fund_name year month pctowned ret aum retdate;
run;

data hf_database;
set hf_database;
fund_id=tranwrd(fund_id,'805-','');
run;

data hf_database;
set hf_database;
format fund_id;
informat fund_id;
run;
proc sort data=hf_database;
by fund_id year month id; *thus we will keep as many data from the same database for consistency. May recode further via count to keep the longest data;
quit;


proc sort data=hf_database nodupkey;
by fund_id year month;
quit;


*Populate pctowned with previous values;
proc sort data=hf_database;by fund_id year month;run;
data temp3;
    do until(last.fund_id);
      set hf_database;
      by fund_id;
      if not missing(pctowned) then do;
        p = pctowned;
      end; 
      if missing(pctowned) then do;
        pctowned=p;
      end; 
      output;
    end;
  drop p;
  run;

proc sort data=temp3;by fund_id year month;run;

*Populate backwards;
proc sort data=temp3;by fund_id year decending month; run;
data temp4;
    do until(last.fund_id);
      set temp3;
      by fund_id;
      if not missing(pctowned) then do;
        p = pctowned;
      end; 
      if missing(pctowned) then do;
        pctowned=p;
      end; 
      output;
    end;
  drop p;
  run;

proc sort data=temp4;by fund_id year month;run;


*we add monthnum;
data temp4;
set temp4;
monthnum = 12*(year(retdate)-1980)+(month(retdate)-1);
drop retdate;
run;

*Create base file for the database;
data base_db;set temp4;if year=>2012;run;
data base_db; retain Fund_id id Fund_Name year month monthnum pctowned ret aum; set base_db; run;


********************Calculate alphas*************************;

*Import data to Work directory;
data FF_factors;set data.ff_factors; if year>=2000;run; *Fama-French fators;
data FH_factors;set data.fh_factors; if year>=2000;run; *Fung-Hsieh fators;

data factors; 
merge ff_factors fh_factors;
by year month;
run;

data factors; 
set factors;
mktrf=mkt_rf;
monthnum_fact= 12*(year(date)-1980)+(month(date)-1);
drop mkt_rf;
run;

*Leave only relevant columns for returns analysis;
data temp5; set temp4; keep Fund_id id year month monthnum ret aum; run;
*Start from 2009;
data temp5;set temp5; if year>=2009;run;


proc sql;
create table returns_temp1
as select distinct a.*,b.rf,b.monthnum_fact as monthnum, a.ret-b.rf as exret
from temp5 as a left join factors as b
on a.monthnum=b.monthnum_fact;quit;

proc sort data=returns_temp1; by fund_id monthnum; quit;
 
*join with factors;
proc sql;
create table returns_temp2
as select distinct a.*,b.*
from returns_temp1 as a left join factors as b
on a.monthnum=b.monthnum_fact;
quit;

*create 24 month window;
proc sql;
create table returns24
as select distinct a.*,b.*
from returns_temp1 (drop=exret) as a left join returns_temp2 (drop=rf date year month ret monthnum) as b
on a.fund_id=b.fund_id and 0<=a.monthnum-b.monthnum_fact<=23;
quit;


proc sort data=returns24; by fund_id monthnum monthnum_fact; quit;
data returns24;set returns24; if year=>2009 and monthnum<=425;run;


**Getting beta estimates first and calculate monthly alphas;
%let CAPM=mktrf;
%let FF3=mktrf SMB HML;
%let FH7=PTFSBD PTFSFX PTFSCOM FRTCM10 BAAMTSY SNPMRF SCMLC;

ods graphics off;
proc reg data=returns24 outest=capm tableout ADJRSQ noprint;
model exRet=&CAPM;
by fund_id monthnum;
quit;
proc reg data=returns24 outest=ff3 tableout ADJRSQ noprint;
model exRet=&FF3;
by fund_id monthnum;
quit;
proc reg data=returns24 outest=fh7 tableout ADJRSQ noprint;
model exRet=&FH7;
by fund_id monthnum;
quit;

data model1;set capm(keep=fund_id _MODEL_ monthnum _ADJRSQ_ _TYPE_ Intercept &CAPM _EDF_) ;if _MODEL_="MODEL1" and _TYPE_="PARMS" and _EDF_>=22;rename _ADJRSQ_=R2_CAPM;rename Intercept=exante_a_CAPM;rename MKTRF=MKTRF_CAPM;drop _MODEL_;attrib _all_ label=' '; run;
data model2;set ff3(keep=fund_id _MODEL_ monthnum _ADJRSQ_ _TYPE_ Intercept &FF3 _EDF_) ;if _MODEL_="MODEL1" and _TYPE_="PARMS" and _EDF_>=20;rename _ADJRSQ_=R2_FF3;rename Intercept=exante_a_FF3;rename MKTRF=MKTRF_FF3; rename SMB=SMB_FF3; rename HML=HML_FF3;drop _MODEL_;attrib _all_ label=' '; run;
data model3;set fh7(keep=fund_id _MODEL_ monthnum _ADJRSQ_ _TYPE_ Intercept &FH7 _EDF_) ;if _MODEL_="MODEL1" and _TYPE_="PARMS" and _EDF_>=16;rename _ADJRSQ_=R2_FH7;rename Intercept=exante_a_FH7;rename PTFSBD=PTFSBD_FH7;rename PTFSFX=PTFSFX_FH7;rename PTFSCOM=PTFSCOM_FH7;rename FRTCM10=FRTCM10_FH7;rename BAAMTSY=BAAMTSY_FH7;rename SNPMRF=SNPMRF_FH7;rename SCMLC=SCMLC_FH7;drop _MODEL_;attrib _all_ label=' '; run;

data modelest;merge model1 model2 model3;by fund_id monthnum;run;
data modelest;set modelest;rename monthnum=mn;run;
* Calculate out of sample residual ;
*Join return and factor data with regression coefficients;
proc sql;
create table alpha1
as select distinct a.*,b.*
from returns_temp2 as a left join modelest as b
on a.fund_id=b.fund_id and a.monthnum=b.mn+1;
quit;

proc sort data=alpha1; by fund_id year month; quit;
data alpha1; set alpha1; if year>=2009 and monthnum<=425; run;

data alpha_m;set alpha1(where=(_TYPE_="PARMS"));
resid_CAPM=exret-exante_a_CAPM-mktrf_CAPM*mktrf;
resid_FF3=exret-exante_a_FF3-mktrf_FF3*mktrf-SMB_FF3*SMB-HML_FF3*HML;
resid_FH7=exret-exante_a_FH7-PTFSBD_FH7*PTFSBD-PTFSFX_FH7*PTFSFX-PTFSCOM_FH7*PTFSCOM-FRTCM10_FH7*FRTCM10-BAAMTSY_FH7*BAAMTSY-SNPMRF_FH7*SNPMRF-SCMLC_FH7*SCMLC;
keep fund_id year month monthnum ret rf exret exante_a_CAPM exante_a_FF3 exante_a_FH7 resid_CAPM resid_FF3 resid_FH7;
run;

*add alpha to base_db;
proc sql;
create table base_db_1
as select distinct a.*, b.*
from base_db as a left join alpha_m (keep=fund_id year month monthnum exret exante_a_CAPM exante_a_FF3 exante_a_FH7 resid_CAPM resid_FF3 resid_FH7)  as b
on a.fund_id=b.fund_id and a.monthnum=b.monthnum;
quit;


* Add to database;
data data.base_db_4;
set base_db_1;
run;
proc sort data=data.base_db_4; by Fund_id monthnum;quit;



