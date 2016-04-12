libname lib 'C:\Users\redme\Dropbox\research\paper2\uk\further';
libname my 'C:\Users\redme\Dropbox\research\paper2\uk\Organizeb4writing';
options MSTORED SASMSTORE=STOREMAC;
LIBNAME STOREMAC 'C:\Users\redme\Dropbox\Notes\STOREMAC';

/*****event data*****/
/*****save event file*****/
/*****variable definition*****
lstday: first-stage listing day
t1b: second-stage issuance day
announcmenet: second-stage issuance announcement day
section: coarse industry, based on 2-digit sic number
sic: security industy code
p: second-stage offering price
shares: second-stage offering share number
proceed: calculated as ln(shares*p)
*****end of variable definition*****/
data my.offering(keep=sedol company isin t1b announcement section sic year proceed shares lstday p);
set lib.reg1;
where yd<=5;
run;

data q;
set lib.underwriter(keep=off_prestigious announcement company);
run;
%merge(data1=my.offering,data2=q,data3=my.offering,var1=announcement,var2=company ,if=if in1 and in2 );
 

/*****merge with stock-level data*****/
/*****variable definition*****
mkt_rt: market return at event day, calculated as [mkt_p-lag(mkt_p)]/lag(mkt_p)
healthy: 1 if pre-event year operating income>0, sales>0 and net income >0
merket_level: market price at event day;
market return: 1 day market return
prc: unadjusted share price
cap: market capitalization calculated as ln(prc*so/100000000)
so: shares outstanding
return std: volatility of price
tv: trading volume
baspread: ask-bid
quote: 2(ask-bid)/(ask+bid)
nsci_3m:previous 3 month index return in %
*****end of variable definition*****/
proc sort data=lib.adjusted_price;
by isin date;
run;
 
proc expand data=lib.adjusted_price out=rt method=none;
by isin ;
convert adjprc=lprc/transform=(lag 1);
convert adjprc=l5prc/transform=(lag 5);
convert date=ldate/transform=(lead 1);
run;
data rt;
set rt;
rt=(adjprc-lprc)/lprc;
stm=(adjprc-l5prc)/l5prc;
year=year(date);
run;
%merge(data1=rt,data2=lib.ipoprice,data3=rt,var1=isin,var2=date);
/*
proc sort data=lib.nsci;
by date;
run;
 
proc expand data=lib.nsci out=nsci method=none;
convert mkt_level=lmkt_level/transform=(lag 66);
run;
data  nsci;
set nsci;
nsci_3m=( mkt_level-lmkt_level)/lmkt_level*100;
run;
data lib.index;
set nsci(keep=date nsci_3m);
run;*/
%merge(data1=rt,data2=lib.nsci,data3=rt,var1=date);
%merge(data1=rt,data2=lib.tv,data3=rt,var1=date,var2=isin);
%merge(data1=rt,data2=lib.ds_so,data3=rt,var1=date,var2=isin);
%merge(data1=rt,data2=lib.baspread,data3=rt,var1=date,var2=isin);
%merge(data1=rt,data2=lib.quote,data3=rt,var1=date,var2=isin);
%merge(data1=rt,data2=lib.index,data3=rt,var1=date);
data healthy;
set lib.accounting(keep=year isin healthy);
where healthy ne .;
year=year-1;
run;
%merge(data1=rt,data2=healthy,data3=rt,var1=year,var2=isin,if=if in1);
data oc;
set lib.Ownership_concentration;
if oc='' then delete;
year=year-1;
run;
%merge(data1=rt,data2=oc,data3=rt,var1=year,var2=isin,if=if in1 );

data rt;
set rt(drop=time lprc);
cap=log(prc*so/100000000);
turnover=tv/so;
run;
 /**
 SAS Macro to add a prefix to some or all variables in a 
 data set...
 to be used like this...
 %prefixvars(inpdsn,prefix,outdsn,excludevars=);
 inpdsn - input dataset name libname.dsnname
 prefix - prefix that you want to assign
 outdsn - output dataset name libname.dsnname
 excludevars - vars that you do not want to rename with the 
 prefix
 **/

 %macro prefixvars(inpdsn,prefix,outdsn,event,date,excludevars=); 

 /* split the excludevars into individual macro var names 
 for later use*/
 %let num=1;
 %let excludevar=%scan(%upcase(&excludevars),&num,' ');
 %let excludevar&num=&excludevar;

 %do %while(&excludevar ne);
 %let num=%eval(&num + 1);
 %let excludevar=%scan(%upcase(&excludevars),&num,' ');
 %let excludevar&num=&excludevar;
 %end;
 %let numkeyvars=%eval(&num - 1); /* this is number of 
 variables given in the exclude vars */


 %let dsid=%sysfunc(open(&inpdsn)); /* open the dataset 
 and get the handle 
 */ 

 %let numvars=%sysfunc(attrn(&dsid,nvars)); /* get the 
 number of variables 
 */ 

 data &outdsn; 
 set &inpdsn(rename=( 
 /*rename all the variables that are not in the 
 excludevars= 
 */ 

 %do i = 1 %to &numvars;
 %let flag=N; 
 %let var&i=%sysfunc(varname(&dsid,&i)); 
 %do j=1 %to &numkeyvars;
 %if %upcase(&&var&i) eq &&excludevar&j %then 
 %let flag=Y; 
 %end; 
 %if &flag eq N %then %do; 
 &&var&i=&prefix&&var&i %end; 
 %end;
&date=&event)); 


 %let rc=%sysfunc(close(&dsid)); 
 run; 
 *%merge(data1=my.offering,data2=&outdsn,data3=&outdsn,var1=&event,var2=isin,if=if in1);
 %mend ; 



 %prefixvars(work.rt,off_,work.rt1,t1b,date,excludevars=isin date ) ;
 %prefixvars(work.rt,lst_,work.rt2,lstday,date,excludevars=isin date) ;
 %prefixvars(work.rt,ann_,work.rt3,announcement,ldate,excludevars=isin ldate ) ;
proc sort data=rt1;
by isin  off_ldate;
run;
  
proc expand data=rt1 out=rt1 method=none;
by isin ;
convert off_mkt_level=off_mkt_level_average/transform=(movave 22);
convert off_turnover=off_turnover_average/transform=(movave 22);
run;
proc expand data=rt1 out=rt1 method=none;
by isin ;
convert off_mkt_level=off_mkt_level_m1m/transform=(lag 23);
convert off_mkt_level_average=off_mkt_level_ave1m/transform=(lag 23);
convert off_turnover=off_turnover_m1m/transform=(lag 23);
convert off_turnover_average=off_turnover_ave1m/transform=(lag 23);
run;
proc sort data=rt2;
by isin descending lstday;
run;
  
proc expand data=rt2 out=rt2 method=none;
by isin ;
convert lst_turnover=lst_turnoverm1m/transform=(movave 22);
run;
%merge(data1=my.offering,data2=rt1,data3=rt1,var1=t1b,var2=isin,if=if in1);
%merge(data1=my.offering,data2=rt2,data3=rt2,var1=lstday,var2=isin,if=if in1);
%merge(data1=my.offering,data2=rt3,data3=rt3,var1=announcement,var2=isin,if=if in1);
 data rt1;
 set rt1(drop=off_ldate );
 run;
 data rt2;
 set rt2(drop=lst_ldate );
 run;
%merge(data1=rt1,data2=rt2,data3=event,var1=isin,if=if in1 and in2);
%merge(data1=event,data2=rt3,data3=event,var1=isin,if=if in1 and in2);

data event; 
set event;
ann_yd=datdif(lstday,ann_date,'act/act')/365;
off_yd=datdif(lstday,t1b,'act/act')/365;
irt=(off_prc-p)/p*100;
up=(ann_prc-p)/p*100;
run;

data my.event(drop=year);
set event;
run;


/*****lending before offering*****/
%sort(data1=my.event,var1=sedol,var2=isin,var3=t1b);
%sort(data1=lib.totlending,var1=sedol,var2=isin,var3=date);

proc sql;
create table LD
as select
a.*,b.Lendable_quantity,b.Total_Balance_Quantity, b.DNS as off_DNS ,B.DIPS as off_dips,
B.dimv as off_dimv,B.DPS as off_dps,B.DSS as off_dss,B.BO_Inventory_Value_Concentration as off_inv_con,
B.BO_On_Loan_Value_Concentration_R as off_ol_con,B.Broker_Demand_Value_Concentratio as off_dem_con, 
b.date ,  datdif(b.date,a.t1b,'act/act') as distance,b.Active_Agents+b.inactive_agents as off_agent,
b.active_brokers as off_broker
from  my.event as a, lib.totlending as b 
where a.sedol=b.sedol and a.isin=b.isin and  b.date<=a.t1b;
quit;

%sort(data1=ld,var1=sedol,var2=distance);
%merge(data1=lib.ds_so,data2=ld,data3=ld,var1=isin,var2=date,if=if in2);
data ld1(where=(distance<31));
set ld;
lendable=lendable_quantity/so*100;
balance=Total_Balance_Quantity/so*100;
run;
proc means data=ld1 noprint;
by isin;
var lendable balance off_dns off_dips off_dimv off_dps off_dss off_inv_con off_ol_con off_dem_con off_agent off_broker;
output out=ll(keep=isin off_lendable off_balance off_dns off_dips off_dimv
off_dps off_dss off_inv_con off_ol_con off_dem_con off_agent off_broker) mean=off_lendable off_balance off_dns off_dips off_dimv
off_dps off_dss off_inv_con off_ol_con off_dem_con off_agent off_broker;
run;
%merge(data1=my.event,data2=ll,data3=my.event,var1=isin,if=if in1);

data my.event;
set my.event;
if off_lendable='' then off_lendable=0;
if off_Balance='' then off_balance=0;
if off_inv_con='' then off_inv_con=0;
if off_ol_con='' then off_ol_con=0;
if off_dem_con='' then off_dem_con=0;
if off_agent='' then off_agent=0;
if off_broker='' then off_broker=0;
proceed=log((p*shares+1)/100000000);

run; 

/*****lending before announcement*****/
proc sql;
create table LD
as select
a.*,b.Lendable_quantity,b.Total_Balance_Quantity,b.DNS as ann_dns,B.DIPS as ann_dips,B.DIMv as ann_dimv,
B.DPS as ann_dps,B.DSS as ann_dss,B.BO_Inventory_Value_Concentration as ann_inv_con,
B.BO_On_Loan_Value_Concentration_R as ann_ol_con,B.Broker_Demand_Value_Concentratio as ann_dem_con,
b.date ,  datdif(b.date,a.announcement,'act/act') as distance,b.Active_Agents+b.inactive_agents as ann_agent,
b.active_brokers as ann_broker
from  my.event as a, lib.totlending as b 
where a.sedol=b.sedol and a.isin=b.isin and b.date<=a.announcement ;
quit;

%sort(data1=ld,var1=sedol,var2=distance);
%merge(data1=lib.ds_so,data2=ld,data3=ld,var1=isin,var2=date,if=if in2);
data ld1(where=(distance<31));
set ld;
lendable=lendable_quantity/so*100;
balance=Total_Balance_Quantity/so*100;
run;
proc means data=ld1 noprint;
by isin;
var lendable balance ann_dns ann_dips ann_dimv ann_dps ann_dss ann_inv_con ann_ol_con ann_dem_con ann_agent ann_broker;
output out=ll(keep=isin ann_lendable ann_balance ann_dns ann_dips ann_dimv ann_dps ann_dss ann_inv_con ann_ol_con ann_dem_con ann_agent ann_broker)
mean=ann_lendable ann_balance ann_dns ann_dips ann_dimv ann_dps ann_dss ann_inv_con ann_ol_con ann_dem_con ann_agent ann_broker;
run;
%merge(data1=my.event,data2=ll,data3=my.event,var1=isin,if=if in1);
data my.event;
set my.event;
lst_turnover=(lst_tv/lst_so)*100;
ann_turnover=(ann_tv/ann_so)*100;
off_turnover=(off_tv/off_so)*100;
if ann_lendable='' then ann_lendable=0;
if ann_Balance='' then ann_balance=0;
if ann_inv_con='' then ann_inv_con=0;
if ann_ol_con='' then ann_ol_con=0;
if ann_dem_con='' then ann_dem_con=0;
if ann_agent='' then ann_agent=0;
if ann_broker='' then ann_broker=0;
if off_lendable>0 then off_ldb=1;
else off_ldb=0;
if off_balance>0 then off_ol=1;
else off_ol=0;
if ann_lendable>0 then ann_ldb=1;
else ann_ldb=0;
if ann_balance>0 then ann_ol=1;
else ann_ol=0;
/*if announcement='' and off_lendable>0 then  ann_ldb=1;
if announcement='' and off_balance>0 then  ann_ol=1;
*/
run;

%merge(data1=lib.excess,data2=my.event,data3=my.event,var1=isin,if=if in2);
%merge(data1=lib.press,data2=my.event,data3=my.event,var1=isin,if=if in2);
/*

data my.event;
set my.event;
off_prestigious=9;
run;
data q;
set my.event(keep=announcement off_prestigious company t1b);
run;
%sort(data1=q,var1=company);

data lib.underwriter;
set q;
run;
*/

 data my.event;
 set my.event;
 if isin='GB00B759CR16' then off_healthy=1;
 if isin='GB00B759CR16' then ann_healthy=1;
 if isin='DK0016245202' then off_healthy=0;
 if isin='DK0016245202' then ann_healthy=0;
 if isin='US71922A1034' then ann_healthy=0;
 if isin='US71922A1034' then off_healthy=0;
 if isin='JE00B3PVQ001' then ann_healthy=0;
 if isin='JE00B3PVQ001' then off_healthy=0;
 if isin='GB00B4KDJB03' then ann_healthy=0;
 if isin='GB00B4KDJB03' then off_healthy=0;
 if isin='GB00B3FBWW43' then ann_healthy=1;
 if isin='GB00B3FBWW43' then off_healthy=1;
 if isin='SG9999007926' then ann_healthy=0;
 if isin='SG9999007926' then off_healthy=0;
 if isin='GG00B1KBGQ78' then ann_healthy=1;
 if isin='GG00B1KBGQ78' then off_healthy=1;
 if isin='AU0000XINAA9' then ann_healthy=0;
 if isin='AU0000XINAA9' then off_healthy=0;
  if isin='AU0000XINAD3' then ann_healthy=0;
 if isin='AU0000XINAD3' then off_healthy=0;
run;

/*****long-run performance*****/
data rt;
set rt;
return=rt-mkt_rt;
run;

%macro longrun;
%sort(data1=rt,var1=isin,var2=date);
data control ;
set rt;
rename date=t1b;
count +1;
by isin;
if first.isin then count =1;
run;

data count;
set control(keep=isin t1b count);
run;

%merge(data1=my.event,data2=count,data3=div1,var1=isin,var2=t1b,if=if in1);

proc sql;
create table startend
as select
a.*, b.t1b as startdate ,b.count as startdays
from div1 as a left join control as b
on a.isin=b.isin and b.count=a.count+6;
quit;

proc sql;
create table startend1
as select
a.*, b.t1b as enddate ,b.count as enddays
from startend as a left join control as b
on a.isin=b.isin and b.count=a.count+765;
quit;

data startend1;
set startend1;
if enddate='' then enddate=mdy(12,31,2013);
run;

proc sql;
create table file
as select
a.t1b,a.enddate,a.startdate,a.isin, b.t1b as gdate, b.count as gdays ,b.return,b.mkt_rt,b.rt
from startend1 as a left join control as b
on a.isin=b.isin and b.t1b>=startdate and b.t1b<=enddate;
quit;

proc sql;
 create table crets as
 select isin,  (exp(sum(log(1+rt)))-1)- (exp(sum(log(1+mkt_rt)))-1) as bhar , sum(return) as car, count(isin) as count
 from file 
group by isin;
quit;
%mend;
%longrun;
/*data event;
set my.event(drop=cret vol);
run;*/
%merge(data1=crets,data2= my.event,data3= EVENT,var1=isin,if=if  in2);
 

/*****long-run performance*****/
 

%macro std;
%sort(data1=rt,var1=isin,var2=date);
data control ;
set rt;
rename date=t1b;
count +1;
by isin;
if first.isin then count =1;
run;

data count;
set control(keep=isin t1b count);
run;

%merge(data1=my.event,data2=count,data3=div1,var1=isin,var2=t1b,if=if in1);

proc sql;
create table startend
as select
a.*, b.t1b as startdate ,b.count as startdays
from div1 as a left join control as b
on a.isin=b.isin and b.count=a.count+6;
quit;

proc sql;
create table startend1
as select
a.*, b.t1b as enddate ,b.count as enddays
from startend as a left join control as b
on a.isin=b.isin and b.count=a.count+260;
quit;

data startend1;
set startend1;
if enddate='' then delete;
run;

proc sql;
create table file
as select
a.t1b,a.enddate,a.startdate,a.isin, b.t1b as gdate, b.count as gdays ,b.rt
from startend1 as a left join control as b
on a.isin=b.isin and b.t1b>=startdate and b.t1b<=enddate;
quit;

proc sql;
 create table std as
 select isin,  std(rt)   as vol 
 from file
group by isin;
quit;
%mend;
%std;
%merge(data1=std,data2= event,data3= EVENT,var1=isin,if=if  in2);
data my.event;
set event;
run;









/**********************************************************************
Purpose: Get compound returns for one stock between two dates, 
          using CRSP daily or monthly file. This utility is basically
          the same as the Fortran function in CRSP for compounding
          returns.

Author : Noah Stoffman (http://kelley.iu.edu/nstoffma)
Date   : 04/21/2004

Input
 id    : 'cusip', permno, or 'ticker'
 idtype: c=cusip, p=permno, t=ticker
 begdt : beginning date
 enddt : ending date
 freq  : d=daily, m=monthly

Note: cusip or ticker have to be in single quotes,
but everything else should not have any quotes.

Output
 The table work.crets is returned, with one
  observation and 7 variables. For details, see
   < http://forum.wharton.upenn.edu/forums/index.php?showtopic=103 >
 Since ticker symbols are not unique, the company which is used is
  for calculations is reported if idtype=t.
***********************************************************************/

/*%macro cret(id,idtype,begdt,enddt,freq);

%if       &idtype = c %then %let what=cusip;
%else %if &idtype = p %then %let what=permno;
%else %if &idtype = t %then %do;
 proc sql;
  select distinct comnam, permno into :name, :id from crsp.&freq.sfnames where ticker=&id
   and st_date <= &enddt;
 quit;
 %let what=permno;
 %put *** Using %sysfunc(trim(&name)) ***;
%end;

proc sql;
 create table crets as
 select distinct &what, exp(sum(log(1+ret))) - 1 as cret "Compound return",
  min(ret) as minret "Minimum return", max(ret) as maxret "Maximum return",
  n(ret) as n_periods "Number of periods", nmiss(ret) as n_miss "Number missing", 
  sum(ret=.P) as n_dot_p "Number with .P"
 from crsp.&freq.sf (keep=&what date ret)
 where &what=&id
 and &begdt <= date <= &enddt;
quit;

%global cret;

data _null_;
 set crets;
 call symput('cret',trim(left(put(cret,9.5))));
run;

%mend cret;




 

/*****match firms*****/
/* split the dataset in 3 parts */
/*
data off_ldb off_ol ann_ldb ann_ol other;
set my.event;
if off_ol=1 then output off_ol;
if off_ldb=1 then output off_ldb;
if ann_ol=1 then output ann_ol;
if ann_ldb=1 then output ann_ldb;
if off_ol=0 and off_ldb=0 and  ann_ol=0 and ann_ldb=0 then output other;
run;

/* Find the firms comparable to offldb */
/*
%macro matching(var,prefix,n);
proc sql;
create table like&prefix&var as
select O.*,
 A.isin as Aisin, abs(O.lst_cap-A.lst_cap) as capDiff
from other as O /* inner join, &prefix&var as A */
  /* on  O.lst_Year between  A.lst_Year - 1 and A.lst_Year+1 /*  and  O.section=A.section  
order by Aisin;/*

proc means data=like&prefix&var  noprint;
by  Aisin;
output out=like&prefix&var idgroup(min(capDiff) out[&n] (capDiff isin)=);
run;

data like&prefix&var(keep= isin aisin capDiff);
set like&prefix&var;
array C{&n} isin_:;array d{&n} capDiff_:;
do order = 1 to &n;
 isin = C{order};
capDiff= d{order};
output;
 end;
run;

proc sql;
create table s as
 select *,count(*) as n
  from like&prefix&var
   group by aisin
    order by  n,aisin;
quit;

data close&prefix&var;
if _n_ eq 1 then do;
 if 0 then  set s; 
  declare hash ha();
   ha.definekey('isin');
   ha.definedone();
end;
 set s; 
 by n aisin ;
 retain found;
 if first.aisin then found=0;
 if not found and ha.check() ne 0 then do;found=1;ha.add();output;end;
run;
%mend;
%matching(ldb,off_,10);
%matching(ol,off_,10);
%matching(ldb,ann_,10);
%matching(ol,ann_,10);


/*****YEAH¬¬¬*****/

/*
data off_oc;
set oc;
rename year=off_year;
rename oc=off_oc;
run;
data ann_oc;
set oc;
rename year=ann_year;
rename oc=ann_oc;
run;

/*****get firm isin for offldb*****/
/*
%macro firm(var,prefix);
data aisin( drop=isin rename=(aisin=isin))  isin( drop=aisin);
set close&prefix&var(keep=aisin isin);
run;

%merge(data1=aisin,data2=isin,data3=my.&prefix&var._firms,var1=isin);

%sort(data1=my.&prefix&var._firms,var1=isin,if=noduprecs);
%merge(data1=my.&prefix&var._firms,data2=my.event,data3=my.&prefix&var._firms,var1=isin,if=if in1 and in2);
data my.&prefix&var._firms;
set my.&prefix&var._firms;
recession=0;
if mdy(05,01,2008) le t1b le mdy(01,01,2010) then recession=1;
if mdy(08,01,2010) le t1b le mdy(02,01,2012) then recession=1;
run;
%merge(data1=my.&prefix&var._firms,data2=&prefix.oc,data3=my.&prefix&var._firms,var1=&prefix.year,var2=isin,if=if in1);
%mend;
%firm(ldb,off_);
%firm(ol,off_);
%firm(ldb,ann_);
%firm(ol,ann_);
%merge(data1=lib.excess,data2=my.ann_ldb_firms,data3=my.ann_ldb_firms,var1=isin,if=if in2);
%merge(data1=lib.press,data2=my.ann_ldb_firms,data3=my.ann_ldb_firms,var1=isin,if=if in2);
%merge(data1=lib.excess,data2=my.ann_ol_firms,data3=my.ann_ol_firms,var1=isin,if=if in2);
%merge(data1=lib.press,data2=my.ann_ol_firms,data3=my.ann_ol_firms,var1=isin,if=if in2);
%macro export(var,prefix);
PROC EXPORT DATA=my.&prefix&var._firms
OUTFILE="C:\research\PAPER2\uk\Organizeb4writing\STATA\&prefix&var..dta"
DBMS=dta REPLACE; 
run;%mend;
%export(ldb,off_);
%export(ol,off_);
%export(ldb,ann_);
%export(ol,ann_);

data check;
set lib.buysidelending;
if isin='GB0031249856' then output;
run;
*/

%merge(data1=lib.bookvalue,data2=lib.adjusted_price,data3=matching,var1=date,var2=isin,if=if in1 and in2);
data matching;
set matching;
bm=bv/adjprc;
run;
%sort(data1=matching,var1=isin,var2=date);
proc expand data=matching  out=matching  method=none;
by  isin ;
convert date=t1b/transform=(lag 1);
run;
%merge(data1=matching,data2=my.event,data3=matching,var1=t1b,var2=isin,if=if in2);
data matching;
set matching;
if isin='AU0000XINAA9' then bm=0.1144615385;
if isin='DK0060005684' then bm=2.5835652174;
if isin='GB0034380393' then bm=0.00844;
if isin='GB00B2PF7L39' then bm=0.0126410256;
if isin='GB00B3FBWW43' then bm=0.0051162791;
run;
libname mt 'C:\research\PAPER2\uk\further\longrun';
data mt.matching(drop=date);
set matching;
run;
data mt.cap;
set mt.daily;
cap=(prccd/AJEXDI)*cshoc/1000000 ;
run;
data mt.annual;
set mt.annual;
rename datadate=date;
run;
data mt.daily;
set mt.daily;
rename datadate=date;
run;
data mt.daily;
set mt.daily;
rename tisin=isin;
run;
*Create Book Equity(BE) measure from Compustat (definition from Daniel and Titman (JF, 2006;
data comp_extract; set mt.annual 
   (where=(fyr>0 and at>0));
   if missing(SEQ)=0 then she=SEQ;else
   if missing(CEQ)=0 and missing(PSTK)=0 then she=CEQ+PSTK;else
   if missing(AT)=0 and missing(LT)=0 and missing(MIB)=0 then she=AT-(LT+MIB);else she=.;
  * if missing(PSTKRV)=0 then BE0=she-PSTKRV;*else if missing(PSTKL)=0 then BE0=she-PSTKL;
   if missing(PSTK)=0 then BE0=she-PSTK; else BE0=.;
   * Converts fiscal year into calendar year data;
 	if (1<=fyr<=5) then date_fyend=intnx('month',mdy(fyr,1,fyear+1),0,'end');
	else if (6<=fyr<=12) then date_fyend=intnx('month',mdy(fyr,1,fyear),0,'end');
  	calyear=year(date_fyend);
  	format date_fyend date9.;
	* Accounting data since calendar year 't-1';
  * if (year(date_fyend) >= year(&bdate) - 1) and (year(date_fyend) <=year(&edate) + 1);
   keep isin calyear fyr SEQ BE0 CEQ AT PSTK date_fyend LT indfmt consol datafmt popsrc date TXDITC;
run;
*Create Book to Market (BM) ratios at December;
proc sql;
   create table BM0	(where=(BM>0))
   as select a.isin, a.calyear,  c.date, a.BE0/(abs(c.prccd /c.AJEXDI )*(c.CSHOC/1000000)) as BM
   from comp_extract as a, 	
		mt.daily (where=( month(date)=12))as c
	where a.isin=c.isin  
   and a.calyear = year(c.date) and (abs(c.prccd /c.AJEXDI )*(c.CSHOC/1000000))>0;
quit;

data bm;
set bm0;
year=year(date);
run;
proc sort data=bm;
by isin descending date;
run;
data bm1;
set bm;
lyear=lag(year);
if lyear ne year then output ;
run;
data mt.bm;
set bm1(keep=date isin bm);
where isin NE '';
run;
data mt.cap;
set mt.cap;
rename tisin=isin;
run;
%merge(data1=mt.bm,data2=mt.cap,data3=mt.target,var1=isin,var2=date,if=if in1 and in2);
data mt.target(keep=isin cap bm year ipodate);
set mt.target;
year=year(date);
run;

data mt.firmbm(keep=isin t1b  year cap bm) mt.firmcap(keep=isin year t1b  cap bm);
set mt.matching;
cap=exp(off_cap);
year=year(t1b);
if bm<0 then bm='';
if bm=''  then output  mt.firmcap;
if bm ne '' and bm>0  then output mt.firmbm;
run;


/*****match firms*****/

/* Find the firm comparable to offldb */
 proc sql;
create table likebm  as
select O.*,
 A.isin as Aisin, abs(O.cap-A.cap) as capDiff,abs(O.bm-A.bm) as bmDiff , ((2*(O.cap-A.cap)/(O.cap+A.cap))**2+
(2*(O.bm-A.bm)/(O.bm+A.bm))**2) as argm, a.t1b as t1b
from mt.target as O   inner join mt.firmbm as A  
  on  O.Year=A.Year and O.ipodate<A.t1b
order by Aisin; 

proc means data=likebm  noprint;
by  Aisin;
output out=likebm idgroup(min(argm /*capDiff bmDiff*/ ) out[10] (/*capDiff bmDiff*/ argm ipodate isin )=);
run;

data likebm(keep= isin aisin ipodate argm /*capDiff bmDiff*/);
set likebm;
array C{10} isin_:;array d{10} argm_:;/*array d{10} capDiff_:;array e{10} bmDiff_:;*/array f{10} ipodate_:; 
do order = 1 to 10;
 isin = C{order};
 argm=d{order};
/*capDiff= d{order};
bmDiff=e{order};
*/ipodate=f{order};
output;
 end;
run;

 
/* Find the firms comparable to offldb */
 proc sql;
create table likecap  as
select O.*,
 A.isin as Aisin, abs(O.cap-A.cap) as capDiff, abs(O.bm-A.bm) as bmDiff,(O.cap-A.cap)/(O.cap+A.cap) as argm,a.t1b as t1b
from mt.target as O   inner join mt.firmcap as A  
  on  O.Year=A.Year and O.ipodate<A.t1b
order by Aisin; 

proc means data=likecap  noprint;
by  Aisin;
output out=likecap idgroup(min( argm/* capDiff*/  ) out[10] (argm /*capDiff*/ ipodate isin)=);
run;

data likecap(keep= isin aisin argm /*capDiff*/ );
set likecap;
array C{10} isin_:;array d{10} argm_:/*capDiff*/; array f{10} ipodate_:; 
do order = 1 to 10;
 isin = C{order};
argm /*capDiff*/= d{order};
ipodate=f{order};
output;
 end;
run;
data likea;
set likecap likebm;
run;
proc sql;
create table s as
 select *,count(*) as n
  from likea
   group by aisin
    order by  n,aisin;
quit;

data close ;
if _n_ eq 1 then do;
 if 0 then  set s; 
  declare hash ha();
   ha.definekey('isin');
   ha.definedone();
end;
 set s; 
 by n aisin ;
 retain found;
 if first.aisin then found=0;
 if not found and ha.check() ne 0 then do;found=1;ha.add();output;end;
run;
data /*my.*/matching( keep= isin tisin );
set close(rename=(isin=tisin));
rename aisin= isin;
run;
data /*my.*/matchingid;
set /*my.*/matching(keep=tisin);
run;
/*data mt.daily;
set mt.daily;
rename isin=tisin;
run;*/
%merge(data1=/*my.*/matchingid,data2=mt.daily,data3= /*my.*/targetprc,var1=tisin,if=if in1);
data  targetprc(keep=tisin tprc date);
set /*my.*/targetprc;
tprc=prccd/AJEXDI;
run;
%sort(data1=targetprc,var1=tisin,var2=date);
proc expand data=targetprc out=targetprc method=none;
by tisin ;
convert tprc=ltprc/transform=(lag 1);
run;
data targetprc;
set targetprc;
trt=(tprc-ltprc)/ltprc;
run;
%merge(data1=rt,data2=/*my.*/matching,data3=rt,var1=isin);
%merge(data1=rt,data2=targetprc,data3=rt,var1=tisin,var2=date,if=if in1 );

data rt;
set rt;
treturn=rt-trt;
run;

%macro longrun;
%sort(data1=rt,var1=isin,var2=date);
data control ;
set rt;
rename date=t1b;
count +1;
by isin;
if first.isin then count =1;
run;

data count;
set control(keep=isin t1b count);
run;

%merge(data1=my.event,data2=count,data3=div1,var1=isin,var2=t1b,if=if in1);

proc sql;
create table startend
as select
a.*, b.t1b as startdate ,b.count as startdays
from div1 as a left join control as b
on a.isin=b.isin and b.count=a.count+6;
quit;

proc sql;
create table startend1
as select
a.*, b.t1b as enddate ,b.count as enddays
from startend as a left join control as b
on a.isin=b.isin and b.count=a.count+765;
quit;

data startend1;
set startend1;
if enddate='' then enddate=mdy(12,31,2013);
run;

proc sql;
create table file
as select
a.t1b,a.enddate,a.startdate,a.isin, b.t1b as gdate, b.count as gdays ,b.treturn,b.trt,b.rt
from startend1 as a left join control as b
on a.isin=b.isin and b.t1b>=startdate and b.t1b<=enddate;
quit;

proc sql;
 create table crets as
 select isin,  (exp(sum(log(1+rt)))-1)- (exp(sum(log(1+trt)))-1) as bhar , sum(treturn) as car, count(isin) as count
 from file 
group by isin;
quit;
%mend;
%longrun;
%sort (data1=crets,var1=count);
/*data event;
set my.event(drop=cret vol);
run;
%merge(data1=crets,data2= my.event,data3= EVENT,var1=isin,if=if  in2);
data less(keep=isin);
set crets;
if count<760 then output;
run;*/
/*%merge(data1=less,data2=mt.matching,data3=less,var1=isin,if=if in1);
data target;
set mt.target;
rename isin=tisin;
run;
%merge(Data1=my.matchingid,data2= target,data3=target,var1=tisin,if=if not in1 and in2);
data target;
set target;
rename tisin= isin;
run;

data  firmbm(keep=isin t1b  year cap bm)  firmcap(keep=isin year t1b  cap bm);
set less;
cap=exp(off_cap);
year=year(t1b);
if bm='' then output  firmcap;
if bm ne '' then output  firmbm;
run;


/*****match firms*****/

/* Find the firms comparable to offldb */
/* proc sql;
create table likebm  as
select O.*,
 A.isin as Aisin, abs(O.cap-A.cap) as capDiff,abs(O.bm-A.bm) as bmDiff ,a.t1b as t1b
from  target as O   inner join firmbm as A  
  on  O.Year=A.Year and O.ipodate<A.t1b
order by Aisin; 

proc means data=likebm  noprint;
by  Aisin;
output out=likebm idgroup(min(capDiff bmDiff ) out[10] (capDiff bmDiff ipodate isin )=);
run;

data likebm(keep= isin aisin ipodate capDiff bmDiff);
set likebm;
array C{10} isin_:;array d{10} capDiff_:;array e{10} bmDiff_:;array f{10} ipodate_:; 
do order = 1 to 10;
 isin = C{order};
capDiff= d{order};
bmDiff=e{order};
ipodate=f{order};
output;
 end;
run;

 
/* Find the firms comparable to offldb */
 /*proc sql;
create table likecap  as
select O.*,
 A.isin as Aisin, abs(O.cap-A.cap) as capDiff, abs(O.bm-A.bm) as bmDiff,a.t1b as t1b
from  target as O   inner join firmcap as A  
  on  O.Year=A.Year and O.ipodate<A.t1b
order by Aisin; 

proc means data=likecap  noprint;
by  Aisin;
output out=likecap idgroup(min( capDiff  ) out[10] (capDiff ipodate isin)=);
run;

data likecap(keep= isin aisin capDiff );
set likecap;
array C{10} isin_:;array d{10} capDiff_:; array f{10} ipodate_:; 
do order = 1 to 10;
 isin = C{order};
capDiff= d{order};
ipodate=f{order};
output;
 end;
run;
data likea;
set likecap likebm;
run;
%sort(data1=likea,var1=bmdiff);
proc sql;
create table s as
 select *,count(*) as n
  from likea
   group by aisin
    order by  n,aisin;
quit;

data close ;
if _n_ eq 1 then do;
 if 0 then  set s; 
  declare hash ha();
   ha.definekey('isin');
   ha.definedone();
end;
 set s; 
 by n aisin ;
 retain found;
 if first.aisin then found=0;
 if not found and ha.check() ne 0 then do;found=1;ha.add();output;end;
run;
data  matching( keep= isin tisin );
set close(rename=(isin=tisin));
rename aisin= isin;
run;
data  matchingid;
set matching(keep=tisin);
run;
data mt.daily;
set mt.daily;
rename isin=tisin;
run;
%merge(data1= matchingid,data2=mt.daily,data3=  targetprc,var1=tisin,if=if in1);
data  targetprc(keep=tisin tprc date);
set  targetprc;
tprc=prccd/AJEXDI;
run;
%sort(data1=targetprc,var1=tisin,var2=date);
proc expand data=targetprc out=targetprc method=none;
by tisin ;
convert tprc=ltprc/transform=(lag 1);
run;
data targetprc;
set targetprc;
trt=(tprc-ltprc)/ltprc;
run;
proc sort data=lib.adjusted_price;
by isin date;
run;
 
proc expand data=lib.adjusted_price out=rt method=none;
by isin ;
convert adjprc=lprc/transform=(lag 1);
convert adjprc=l5prc/transform=(lag 5);
convert date=ldate/transform=(lead 1);
run;
data rt;
set rt;
rt=(adjprc-lprc)/lprc;
stm=(adjprc-l5prc)/l5prc;
year=year(date);
run;
%merge(data1=rt,data2=lib.ipoprice,data3=rt,var1=isin,var2=date);
%merge(data1=rt,data2= matching,data3=rt,var1=isin);
%merge(data1=rt,data2=targetprc,data3=rt,var1=tisin,var2=date,if=if in1 );

data rt;
set rt;
treturn=rt-trt;
run;

%macro longrun;
%sort(data1=rt,var1=isin,var2=date);
data control ;
set rt;
rename date=t1b;
count +1;
by isin;
if first.isin then count =1;
run;

data count;
set control(keep=isin t1b count);
run;

%merge(data1=my.event,data2=count,data3=div1,var1=isin,var2=t1b,if=if in1);

proc sql;
create table startend
as select
a.*, b.t1b as startdate ,b.count as startdays
from div1 as a left join control as b
on a.isin=b.isin and b.count=a.count+6;
quit;

proc sql;
create table startend1
as select
a.*, b.t1b as enddate ,b.count as enddays
from startend as a left join control as b
on a.isin=b.isin and b.count=a.count+765;
quit;

data startend1;
set startend1;
if enddate='' then enddate=mdy(12,31,2013);
run;

proc sql;
create table file
as select
a.t1b,a.enddate,a.startdate,a.isin, b.t1b as gdate, b.count as gdays ,b.treturn,b.trt,b.rt
from startend1 as a left join control as b
on a.isin=b.isin and b.t1b>=startdate and b.t1b<=enddate;
quit;

proc sql;
 create table cret as
 select isin,  (exp(sum(log(1+rt)))-1)- (exp(sum(log(1+trt)))-1) as bhar , sum(treturn) as car, count(isin) as count
 from file 
group by isin;
quit;
%mend;
%longrun;
%sort (data1=cret,var1=count);
data cret;
set cret;
if count=760 and bhar ne '' then output;
run;
%merge(data1=cret,data2=crets,data3=cre,var1=isin,if=if not in1 and in2);
data cre;
set cre cret;
run;*/
data crets;
set crets;
rename bhar=bharm;
rename car=carm;
run;
data my.Lr_with_off_m1m;
set my.Lr_with_off_m1m(drop=bharm carm);
run;
%merge(data1=my.Lr_with_off_m1m,data2=crets,data3=my.Lr_with_off_m1m,var1=isin);
/*
%merge(Data1=my.event,data2=crets,data3=event,var1=isin);
data event;
set event;
year=year(t1b);
run;
data bm;
set mt.matching(keep=isin  bm);
if bm<0 then bm='';
run;
%merge(data1=bm,data2=event,data3=event,var1=isin );*/

/*
data bm;
set mt.bm;
year=year(date);
run;
data eve;
set my.event;
year=year(t1b);
run;
%merge(data1=eve,data2=bm,data3=test,var1=isin,var2=year,if=if in1 and in2);
data test;
set test(keep=year isin bm);
rename bm=bmwrds;
run;
%merge(data1=test,data2=mt.matching,data3=test1,var1=isin,if=if in1 and in2);
data test1;
set test1(keep=isin bm bmwrds);
run;
