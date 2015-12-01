*******************************************************************************
This code creates reverse mapping from matched names to union_database.
The code is created by Mikhail Stukalo. July 2015
*******************************************************************************;

*******************************************************************************
Input files: 	union_names - mapping file between ADV and 4 databases
				fund_ids - map of union_database to commercial database
				union_contracts1 - original union names file

*******************************************************************************;
* Set library path. !!!!!!!Check the path!!!!!!!!!!!!;

libname data 'c:\Users\Mikhail\Desktop\Funds\Data\';

*Break matched names by databases;

data names;
set data.union_names;
keep Fund_id id ADV_name;
run;

data hfr_match;
set names;
if id<=100000;
orig_id=id;
run;

data tass_match;
set names;
if id>=100001 & id<=300000;
orig_id=id-100000;
run;

data EH_match;
set names;
if id>=300001 & id<=500000;
orig_id=id-300000;
run;

* For morningstar we need a more complicated procedure;
data MS_match_temp;
set names;
if id>=500001;
run;

PROC IMPORT OUT= WORK.MS_temp 
            DATAFILE= "C:\Users\Mikhail\Desktop\Funds\RAW\MS_Names.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data ms_temp;
set ms_temp;
keep id SecId;
run;

proc sql;
create table ms_match
as select distinct a.*, b.*
from ms_match_temp as a left join ms_temp as b
on a.id=b.id;
quit;

data ms_match;
set ms_match;
orig_id=SecId;
drop SecId;
run;

*Crete union maps by commercial databases;
data union_map;
set data.funds_ids;
run;

data union_tass;
set union_map;
un_id=id;
drop id;
keep un_id tid1 tid2 tid3 tid4 tid5 tid6 tid7 tid8 tid9 tid10;
run;

data union_HFR;
set union_map;
un_id=id;
drop id;
keep un_id hid1 hid2 hid3 hid4 hid5 hid6 hid7 hid8 hid9 hid10;
run;

data union_EH;
set union_map;
un_id=id;
drop id;
keep un_id eid1 eid2 eid3 eid4 eid5 eid6 eid7 eid8 eid9 eid10;
run;

data union_MS;
set union_map;
un_id=id;
drop id;
keep un_id secid1 secid2 secid3 secid4 secid5 secid6 secid7 secid8 secid9 secid10;
run;

* Create combined maps;
*Tass;
proc sql;
create table tass
as select distinct a.*, b.*
from tass_match as a left join union_tass as b
on a.orig_id=b.tid1 or a.orig_id=b.tid2 or a.orig_id=b.tid3 or
a.orig_id=b.tid4 or a.orig_id=b.tid5 or a.orig_id=b.tid6 or a.orig_id=b.tid7 or
a.orig_id=b.tid8 or a.orig_id=b.tid9 or a.orig_id=b.tid10;
quit;

proc sort data=tass nodupkey;
by Fund_id un_id;
quit;

data tass;
set tass;
keep Fund_id id orig_id un_id;
run;

*EH;

proc sql;
create table EH
as select distinct a.*, b.*
from EH_match as a left join union_EH as b
on a.orig_id=b.eid1 or a.orig_id=b.eid2 or a.orig_id=b.eid3 or
a.orig_id=b.eid4 or a.orig_id=b.eid5 or a.orig_id=b.eid6 or a.orig_id=b.eid7 or
a.orig_id=b.eid8 or a.orig_id=b.eid9 or a.orig_id=b.eid10;
quit;

proc sort data=EH nodupkey;
by Fund_id un_id;
quit;

data EH;
set EH;
keep Fund_id id orig_id un_id;
run;

*MS;

proc sql;
create table MS
as select distinct a.*, b.*
from MS_match as a left join union_MS as b
on a.orig_id=b.secid1 or a.orig_id=b.secid2 or a.orig_id=b.secid3 or
a.orig_id=b.secid4 or a.orig_id=b.secid5 or a.orig_id=b.secid6 or a.orig_id=b.secid7 or
a.orig_id=b.secid8 or a.orig_id=b.secid9 or a.orig_id=b.secid10;
quit;

proc sort data=MS nodupkey;
by Fund_id un_id;
quit;

data MS;
set MS;
keep Fund_id id orig_id un_id;
run;

*HFR;

proc sql;
create table HFR
as select distinct a.*, b.*
from HFR_match as a left join union_HFR as b
on a.orig_id=b.hid1 or a.orig_id=b.hid2 or a.orig_id=b.hid3 or
a.orig_id=b.hid4 or a.orig_id=b.hid5 or a.orig_id=b.hid6 or a.orig_id=b.hid7 or
a.orig_id=b.hid8 or a.orig_id=b.hid9 or a.orig_id=b.hid10;
quit;

proc sort data=HFR nodupkey;
by Fund_id un_id;
quit;

data HFR;
set HFR;
keep Fund_id id orig_id un_id;
run;


*Combine all maps;
data reverse_map;
set tass (keep=Fund_id un_id) hfr (keep=Fund_id un_id) ms (keep=Fund_id un_id) eh (keep=Fund_id un_id);
run;

data reverse_map;
set reverse_map;
where un_id;
run;

proc sort data=reverse_map nodupkey;
by Fund_id;
quit;

* add data from original union_names file;
proc sql;
create table additional_data
as select distinct a.*, b.*
from reverse_map as a left join data.union_contracts1 as b
on a.un_id=b.id;
quit;

data additional_data;
set additional_data;
drop id;
run;

proc sql;
create table data.additional_data
as select distinct a.*, b.id
from additional_data as a left join names as b
on a.Fund_id=b.Fund_id;
quit;
