 
* Set library path. !!!!!!!Check the path!!!!!!!!!!!!;

libname data 'c:\Users\Mikhail\Desktop\Funds\Data\';

*Combine TASS name matches;

PROC IMPORT OUT= WORK.tass_match1 
            DATAFILE= "C:\Users\Mikhail\Desktop\Funds\Results\TASS\clean.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.tass_match2 
            DATAFILE= "C:\Users\Mikhail\Desktop\Funds\Results\TASS\match7.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.tass_match3 
            DATAFILE= "C:\Users\Mikhail\Desktop\Funds\Results\TASS\match6.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.tass_match4 
            DATAFILE= "C:\Users\Mikhail\Desktop\Funds\Results\TASS\match5.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data tass;
set tass_match2 tass_match3 tass_match4;
run;


data tass_clean;
set tass;
drop Fund_Name;
run;

data tass_perf;
set tass_match1;
drop Fund_Name;
run;

*Combine HFR name matches;

PROC IMPORT OUT= WORK.HFR_match1 
            DATAFILE= "C:\Users\Mikhail\Desktop\Funds\Results\HFR\clean.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.HFR_match2 
            DATAFILE= "C:\Users\Mikhail\Desktop\Funds\Results\HFR\match7.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.HFR_match3 
            DATAFILE= "C:\Users\Mikhail\Desktop\Funds\Results\HFR\match6.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.HFR_match4 
            DATAFILE= "C:\Users\Mikhail\Desktop\Funds\Results\HFR\match5.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data HFR;
set HFR_match2 HFR_match3 HFR_match4;
run;


data HFR_clean;
set HFR;
drop Fund_Name;
run;

data HFR_perf;
set HFR_match1;
drop Fund_Name;
run;

PROC EXPORT DATA= WORK.HFR_clean 
            OUTFILE= "C:\Users\Mikhail\Desktop\Funds\Results\HFR\HFR_clean_names.csv" 
            DBMS=CSV REPLACE; *Check folder!!!!!!;
     PUTNAMES=YES;
RUN;
*Combine EH name matches;

PROC IMPORT OUT= WORK.EH_match1 
            DATAFILE= "C:\Users\Mikhail\Desktop\Funds\Results\EH\clean.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.EH_match2 
            DATAFILE= "C:\Users\Mikhail\Desktop\Funds\Results\EH\match7.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.EH_match3 
            DATAFILE= "C:\Users\Mikhail\Desktop\Funds\Results\EH\match6.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.EH_match4 
            DATAFILE= "C:\Users\Mikhail\Desktop\Funds\Results\EH\match5.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data EH;
set EH_match2 EH_match3 EH_match4;
run;


data EH_clean;
set EH;
drop Fund_Name;
run;

data EH_perf;
set EH_match1;
drop Fund_Name;
run;

PROC EXPORT DATA= WORK.EH_clean 
            OUTFILE= "C:\Users\Mikhail\Desktop\Funds\Results\EH\EH_clean_names.csv" 
            DBMS=CSV REPLACE; *Check folder!!!!!!;
     PUTNAMES=YES;
RUN;
*Combine MS name matches;

PROC IMPORT OUT= WORK.MS_match1 
            DATAFILE= "C:\Users\Mikhail\Desktop\Funds\Results\MS\clean.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.MS_match2 
            DATAFILE= "C:\Users\Mikhail\Desktop\Funds\Results\MS\match7.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.MS_match3 
            DATAFILE= "C:\Users\Mikhail\Desktop\Funds\Results\MS\match6.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.MS_match4 
            DATAFILE= "C:\Users\Mikhail\Desktop\Funds\Results\MS\match5.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data MS;
set MS_match2 MS_match3 MS_match4;
run;


data MS_clean;
set MS;
drop Fund_Name;
run;

data MS_perf;
set MS_match1;
drop Fund_Name;
run;


*************** FOR IMPERFECT MATCHES*******************
* Get rid of exact duplicates;
proc sort data=hfr_clean nodupkey;
by Fund_id id;
quit;

proc sort data=ms_clean nodupkey;
by Fund_id id;
quit;
proc sort data=tass_clean nodupkey;
by Fund_id id;
quit;
proc sort data=eh_clean nodupkey;
by Fund_id id;
quit;

*Put counters for duplicate observations (adv and commercial database);
proc sort data=hfr_clean;
by fund_id;
quit;

data hfr_clean; 
set hfr_clean; 
by fund_id; 
dbl_adv+1; 
if first.fund_id then dbl_adv=1; 
run;
proc sort data=hfr_clean;
by id;
quit;
data hfr_clean; 
set hfr_clean; 
by id; 
dbl_base+1; 
if first.id then dbl_base=1; 
run;
proc sort data=ms_clean;
by fund_id;
quit;
data ms_clean; 
set ms_clean; 
by fund_id; 
dbl_adv+1; 
if first.fund_id then dbl_adv=1; 
run;
proc sort data=ms_clean;
by id;
quit;
data ms_clean; 
set ms_clean; 
by id; 
dbl_base+1; 
if first.id then dbl_base=1; 
run;
proc sort data=eh_clean;
by fund_id;
quit;
data eh_clean; 
set eh_clean; 
by fund_id; 
dbl_adv+1; 
if first.fund_id then dbl_adv=1; 
run;
proc sort data=eh_clean;
by id;
quit;
data eh_clean; 
set eh_clean; 
by id; 
dbl_base+1; 
if first.id then dbl_base=1; 
run;
proc sort data=tass_clean;
by fund_id;
quit;
data tass_clean; 
set tass_clean; 
by fund_id; 
dbl_adv+1; 
if first.fund_id then dbl_adv=1; 
run;
proc sort data=tass_clean;
by id;
quit;
data tass_clean; 
set tass_clean; 
by id; 
dbl_base+1; 
if first.id then dbl_base=1; 
run;

*Create one database of name matches;
data data.union_names_imperf;
set TASS_clean HFR_clean EH_clean MS_clean;
run;

* Keep those that do not have multiple observations;
proc sql;
create table union_names_impf_nodup as
  select *
   from data.union_names_imperf
    group by fund_id
     having max(dbl_adv)=1 ;
quit;
proc sql;
create table union_names_impf_nodup1 as
  select *
   from union_names_impf_nodup
    group by id
     having max(dbl_base)=1 ;
quit;

*************** FOR PERFECT MATCHES*******************

* Get rid of exact duplicates;
proc sort data=hfr_perf nodupkey;
by Fund_id id;
quit;

proc sort data=ms_perf nodupkey;
by Fund_id id;
quit;
proc sort data=tass_perf nodupkey;
by Fund_id id;
quit;
proc sort data=eh_perf nodupkey;
by Fund_id id;
quit;

*Put counters for duplicate observations (adv and commercial database);
proc sort data=hfr_perf;
by fund_id;
quit;

data hfr_perf; 
set hfr_perf; 
by fund_id; 
dbl_adv+1; 
if first.fund_id then dbl_adv=1; 
run;
proc sort data=hfr_perf;
by id;
quit;
data hfr_perf; 
set hfr_perf; 
by id; 
dbl_base+1; 
if first.id then dbl_base=1; 
run;
proc sort data=ms_perf;
by fund_id;
quit;
data ms_perf; 
set ms_perf; 
by fund_id; 
dbl_adv+1; 
if first.fund_id then dbl_adv=1; 
run;
proc sort data=ms_perf;
by id;
quit;
data ms_perf; 
set ms_perf; 
by id; 
dbl_base+1; 
if first.id then dbl_base=1; 
run;
proc sort data=eh_perf;
by fund_id;
quit;
data eh_perf; 
set eh_perf; 
by fund_id; 
dbl_adv+1; 
if first.fund_id then dbl_adv=1; 
run;
proc sort data=eh_perf;
by id;
quit;
data eh_perf; 
set eh_perf; 
by id; 
dbl_base+1; 
if first.id then dbl_base=1; 
run;
proc sort data=tass_perf;
by fund_id;
quit;
data tass_perf; 
set tass_perf; 
by fund_id; 
dbl_adv+1; 
if first.fund_id then dbl_adv=1; 
run;
proc sort data=tass_perf;
by id;
quit;
data tass_perf; 
set tass_perf; 
by id; 
dbl_base+1; 
if first.id then dbl_base=1; 
run;

*Create one database of name matches;
data data.union_names_perf;
set TASS_perf HFR_perf EH_perf MS_perf;
run;


********* CREATE ONE DATABASE************;
data data.union_names;
set data.union_names_perf Union_names_impf_nodup1;
run;
