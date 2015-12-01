set more off

* Get rid off duplicates
sort adv_id monthnum 
quietly by adv_id monthnum:  gen dup = cond(_N==1,0,_n)
tabulate dup
drop if dup>1
drop dup

*Convert into panel data
xtset adv_id monthnum

*calcuate sd of pctowned
egen sd_pct=sd(pctowned), by (adv_id)

*find those that change
gen d_grp1=(sd_pct!=0) 

*Tabulate
xttab d_grp1
