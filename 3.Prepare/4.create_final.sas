*******************************************************************************
This code combines database into one STATA file.
The code is created by Mikhail Stukalo. July 2015
*******************************************************************************;

*******************************************************************************
Input files: 	base_db_4 - database with returns, alphas, etc 
		        additional_data - result of reverse mapping to the original union database
				delta - delta calculations
*******************************************************************************;

libname data 'c:\Users\Mikhail\Desktop\Funds\Data\';
libname d 'c:\Users\Mikhail\Desktop\Funds\delta\';

* Get database of returns;
data db;
set data.base_db_4;
run;

* Get additional data;

data add;
set data.additional_data;
run;

* Clean Fund_id;
data add;
set add;
fund_id=tranwrd(fund_id,'805-','');
run;

*Keep relevant additional data;
data add1;
set add;
keep Fund_id un_id Name companyname style_master offsh incep curr usd mfee ifee lev lockup redem hwm;
run;

* Create final table;
proc sql;
create table full_database
as select a.*, b.*
from db as a left join add1 as b
on a.Fund_id=b.Fund_id;
quit;

proc sort data=full_database;
by fund_id monthnum;
quit;

*Add delta;
proc sql;
create table full_database_delta
as select a.*, b.invdelta, b.gret
from full_database as a left join d.delta as b
on a.id=b.id and a.monthnum=b.monthnum;
quit;


*Reorder the columns;
data full_db_final;
   retain Fund_id un_id id Name companyname Fund_Name offsh curr usd year month monthnum pctowned aum invdelta ret gret rf exret
a_CAPM int_CAPM resid_CAPM a_FF3 int_FF3 resid_FF3 a_FF4 int_FF4 resid_FF4 a_FH7 int_FH7 resid_FH7 style_master incep mfee ifee lev lockup 
redem hwm;
   set full_database_delta;
  run;

  proc sort data=full_db_final nodupkey; by Fund_id monthnum; quit;



*Export data for STATA;

PROC EXPORT DATA= full_db_final 
            OUTFILE= "C:\Users\Mikhail\Desktop\Funds\Results\STATA\hf_database_full.dta" 
            DBMS=STATA REPLACE;
RUN;



