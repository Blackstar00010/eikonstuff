/* ======================================================================== */
/* ======================================================================== */
/* ======================================================================== */
/* Change .sql to .sas to upload or just create a file on online SAS Studio */
/* ======================================================================== */
/* ======================================================================== */
/* ======================================================================== */


/* ======================================================================== */
/*                          Downloading full files                          */
/* ======================================================================== */

/* -------------------------------- ibes-y -------------------------------- */
proc sql;
	create table ibes_ann_est_all as 
	select * from ibes.statsum_epsint
	where measure = "EPS" and fpi = '1';
quit;
proc export data = ibes_ann_est_all
    outfile = "~/data_all/ibes_ann_est_all.csv"
    dbms = csv
    replace;
run;

/* -------------------------------- ibes-q -------------------------------- */
proc sql;
	create table ibes_qtr_est_all as 
	select * from ibes.statsum_epsint
	where measure = "EPS" and fpi = '6';
quit;
proc export data = ibes_qtr_est_all
    outfile = "~/data_all/ibes_qtr_est_all.csv"
    dbms = csv
    replace;
run;

/* ---------------------------- comp-g_company ---------------------------- */
proc sql;
	create table comp_company_all as 
	select * from comp.g_company;
quit;
proc export data = comp_company_all
    outfile = "~/data_all/comp_company_all.csv"
    dbms = csv
    replace;
run;
/* ----------------------------- comp-g_funda ----------------------------- */
proc sql;
	create table comp_funda_all as 
	select * from comp.g_funda;
quit;
proc export data = comp_funda_all
    outfile = "~/data_all/comp_funda_all.csv"
    dbms = csv
    replace;
run;
/* ----------------------------- comp-g_fundq ----------------------------- */
proc sql;
	create table comp_fundq_all as 
	select * from comp.g_fundq;
quit;
proc export data = comp_fundq_all
    outfile = "~/data_all/comp_fundq_all.csv"
    dbms = csv
    replace;
run;
/* ---------------------------- comp-secd ---------------------------- */
proc sql;
	create table comp_secd_all as 
	select * from comp.g_secd
	where exchg in (194,282);
quit;
proc export data = comp_secd_all
    outfile = "~/data_all/comp_secd_all.csv"
    dbms = csv
    replace;
run;
/* ----------------------------- comp-g_names ----------------------------- */
proc sql;
	create table comp_names1_all as 
	select * from comp.g_names;
quit;
proc export data = comp_names1_all
    outfile = "~/data_all/comp_names1_all.csv"
    dbms = csv
    replace;
run;
/* ---------------------------- comp-g_namesq ---------------------------- */
proc sql;
	create table comp_names2_all as 
	select * from comp.g_namesq;
quit;
proc export data = comp_names2_all
    outfile = "~/data_all/comp_names2_all.csv"
    dbms = csv
    replace;
run;

/* --------------------------- comp-bank_funda --------------------------- */
proc sql;
	create table bank_funda_all as 
	select * from comp.bank_funda;
quit;
proc export data = bank_funda_all
    outfile = "~/data_all/bank_funda_all.csv"
    dbms = csv
    replace;
run;
/* --------------------------- comp-bank_fundq --------------------------- */
proc sql;
	create table bank_fundq_all as 
	select * from comp.bank_fundq;
quit;
proc export data = bank_fundq_all
    outfile = "~/data_all/bank_fundq_all.csv"
    dbms = csv
    replace;
run;
/* --------------------------- comp-bank_names --------------------------- */
proc sql;
	create table bank_names1_all as 
	select * from comp.bank_names;
quit;
proc export data = bank_names1_all
    outfile = "~/data_all/bank_names1_all.csv"
    dbms = csv
    replace;
run;
/* --------------------------- comp-bank_namesq --------------------------- */
proc sql;
	create table bank_names2_all as 
	select * from comp.bank_namesq;
quit;
proc export data = bank_names2_all
    outfile = "~/data_all/bank_names2_all.csv"
    dbms = csv
    replace;
run;
/* ---------------------------- comp-currency ---------------------------- */
proc sql;
	create table comp_exrt_all as 
	select * from comp.g_exrt_dly;
quit;
proc export data = comp_exrt_all
    outfile = "~/data_all/comp_exrt_all.csv"
    dbms = csv
    replace;
run;
/* --------------------------- comp-g_idx_daily --------------------------- */
proc sql;
	create table comp_idx_daily as 
	select * from comp.g_idx_daily;
quit;
proc export data = comp_idx_daily
    outfile = "~/data_all/comp_idx_daily_all.csv"
    dbms = csv
    replace;
run;
/* ------------------------------ comp-g_idx ------------------------------ */
proc sql;
	create table comp_idx as 
	select * from comp.g_idx_index;
quit;
proc export data = comp_idx
    outfile = "~/data_all/comp_idx_all.csv"
    dbms = csv
    replace;
run;
/* ------------------------------ comp-g_idx ------------------------------ */
proc sql;
	create table sedolgvkey as 
	select * from comp.g_sedolgvkey;
quit;
proc export data = sedolgvkey
    outfile = "~/data_all/sedolgvkey.csv"
    dbms = csv
    replace;
run;



/* ======================================================================== */
/*                       Downloading neccessary files                       */
/* ======================================================================== */

/* -------------------------------- ibes-y -------------------------------- */
proc sql;
	create table ibes_ann_est as 
	select cname,ticker,cusip,oftic,fpedats,statpers,ANNDATS_ACT,numest,ANNTIMS_ACT,medest,meanest,actual,stdev
	from ibes.statsum_epsint
	where fpi='1'  /*1 is for annual forecasts, 6 is for quarterly*/
	and statpers<ANNDATS_ACT /*only keep summarized forecasts prior to earnings annoucement*/
	and measure='EPS' 
	and not missing(medest) and not missing(fpedats)
	and (fpedats-statpers)>=0;
	quit; 
quit;
proc export data = ibes_ann_est
    outfile = "~/data/ibes_ann_est.csv"
    dbms = csv
    replace;
run;

/* -------------------------------- ibes-q -------------------------------- */
proc sql;
	create table ibes_qtr_est as 
	select cname,ticker,cusip,oftic,fpedats,statpers,ANNDATS_ACT,numest,ANNTIMS_ACT,medest,actual,stdev
	from ibes.statsum_epsint
	where fpi='6'  /*1 is for annual forecasts, 6 is for quarterly*/
	and statpers<ANNDATS_ACT /*only keep summarized forecasts prior to earnings annoucement*/
	and measure='EPS' 
	and not missing(medest) and not missing(fpedats)
	and (fpedats-statpers)>=0;
	quit; 
quit;
proc export data = ibes_qtr_est
    outfile = "~/data/ibes_qtr_est.csv"
    dbms = csv
    replace;
run;

/* ---------------------------- comp-g_company ---------------------------- */
proc sql;
	create table comp_company as 
	select gvkey, cik, substr(sic,1,2) as sic2, sic, naics, dldte, ipodate
	from comp.g_company;
quit;
proc export data = comp_company
    outfile = "~/data/comp_company.csv"
    dbms = csv
    replace;
run;
/* ----------------------------- comp-g_funda ----------------------------- */
proc sql;
	create table comp_funda as 
	select gvkey,conm,datadate,fyear,curcd,
		sale,revt,cogs,xsga,dp,xrd,/*xad,*/ib,ebitda,ebit,nopi,spi,pi,txp,/*ni,txfed,txfo,*/txt,xint,

						/*CF statement and others*/
		capx,oancf,dvt,/*ob,gdwlia,gdwlip,gwo,*/

						/*assets*/
		rect,act,che,ppegt,invt,at,aco,intan,ao,ppent,gdwl,fatb,fatl,

						/*liabilities*/
		lct,dlc,dltt,lt,/*dm,dcvt,cshrc,dcpstk,*/pstk,ap,lco,lo,/*drc,drlt,*/txdi,

						/*equity and other*/
		ceq,/*scstkc,*/emp/*,csho,*/

						/*market*/
		/*prcc_f,csho*calculated prcc_f as mve_f*/
	from comp.g_funda;
quit;
proc export data = comp_funda
    outfile = "~/data/comp_funda.csv"
    dbms = csv
    replace;
run;
/* ----------------------------- comp-g_fundq ----------------------------- */
proc sql;
	create table comp_fundq as 
	select gvkey,fyearq,fqtr,datadate,/*rdq,*/curcdq,
		/*income items*/
			ibq,saleq,txtq,revtq,cogsq,xsgaq,
		/*balance sheet items*/
			atq,actq,cheq,lctq,dlcq,ppentq, 
		/*other*/
	  	/*abs(prccq) as prccq,cshoq,abs(prccq)*cshoq as mveq,*/ceqq,

		seqq,/*pstkq,*/atq,ltq/*,pstkrq*/
	from comp.g_fundq;
quit;
proc export data = comp_fundq
    outfile = "~/data/comp_fundq.csv"
    dbms = csv
    replace;
run;

/* ---------------------------- comp-adsprate ---------------------------- */
proc sql;
	create table comp_adsprate as 
	select * from comp.adsprate;
quit;
proc export data = comp_adsprate
    outfile = "~/data/comp_adsprate.csv"
    dbms = csv
    replace;
run;

/* ---------------------------- comp-currency ---------------------------- */
/* useless as all tocurd are 'GBP'
proc sql;
	create table comp_exrt as 
	select * from comp.g_exrt_dly
	where tocurd = "GBP";
quit;
proc export data = comp_exrt
    outfile = "~/data/comp_exrt.csv"
    dbms = csv
    replace;
run;
*/

/* ---------------------------- comp-secd ---------------------------- */
proc sql;
	create table comp_secd as 
	select gvkey, datadate, conm, isin, exchg, monthend, curcdd,
	/* adjustment factor, shrout, volume, quote unit */
	ajexdi, cshoc, cshtrd, qunit,
	/* ohlc */ 
	prccd, prchd, prcld, prcod, 
	/* currency code, div, paydate */
	curcddv, divd, paydate, 
	/* split rate */
	split
	from comp.g_secd
	where exchg in (194,282);
quit;
proc export data = comp_secd
    outfile = "~/data/comp_secd.csv"
    dbms = csv
    replace;
run;





/*
proc sql;
	create table asdf_some as 
	select *
	from comp.funda
	where cusip in ("037833100", "459200101", "713448108")
	and fyear = 2022;
quit;
*/
