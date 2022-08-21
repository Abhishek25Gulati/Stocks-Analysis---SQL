Create database project_final;

use project_final;

select * from bhartiartl;
select * from maruti;
select * from ultracemco;

/* Adding new_date coloumn in bhatiartl, maruti and ultracemco table */
alter table bhartiartl
add column new_date date;

alter table maruti
add column new_date date;

alter table ultracemco
add column new_date date;

set sql_safe_updates =0;

/*updating new_date column*/

update bhartiartl 
set new_date = str_to_date(date,"%d-%m-%Y");

update maruti
set new_date = str_to_date(date,"%d-%m-%Y");


update ultracemco
set new_date = str_to_date(date,"%d-%m-%Y");


/*Q1) Find the volatility of all the companies and compare them*/

drop view A1;
create view A1 as ( select symbol, high, low  from bhartiartl
union
select symbol, high, low  from maruti
union
select symbol, high, low  from ultracemco );

select * from A1;

select symbol, round(avg(high -low),2) as avg_volatility, dense_rank() over(order by avg(high -low) asc ) 
as ranking from A1
group by symbol;

/*Output- Bharti Airtel is the Least Volatile and Maruti is the most Volatile/

/* Q2) which stock fell the least during the covid times, time period 20-feb 2020 -23-march-2020*/

/*Find the fall in stock price of maruti suzuki*/

set @pre_covid_price_maruti:= ( select close from maruti
where new_date="2020-02-20");

	set @post_covid_price_maruti := (select close from maruti
where new_date="2020-03-23");

select @pre_covid_price_maruti; /*6757.6*/
select @post_covid_price_maruti; /*4220.35*/

select round(((-@pre_covid_price_maruti+@post_covid_price_maruti)/@pre_covid_price_maruti),4)* 100
 as Maruti_suzuki_drawdown;

/*Maruti Suzuki stock price fell 37.55 percent during covid fall*/

/*Find the fall in stock price of bhatiairtel*/

set @pre_covid_price_bhartiartl:= ( select close from bhartiartl
where new_date="2020-02-20");

	set @post_covid_price_bhartiartl := (select close from bhartiartl
where new_date="2020-03-23");

select @pre_covid_price_bhartiartl; /*535.55*/
select @post_covid_price_bhartiartl; /*399.85*/

select round(((-@pre_covid_price_bhartiartl+@post_covid_price_bhartiartl)/@pre_covid_price_bhartiartl),4)* 100 as BhartiAirtel_Drawdown;

/*Bharti Airtel stock price fell 25.34 percent during covid fall*/

/*Find the fall in stock price of Ultratech Cement*/

set @pre_covid_price_ultracemco:= ( select close from ultracemco
where new_date="2020-02-20");

	set @post_covid_price_ultracemco := (select close from ultracemco
where new_date="2020-03-23");

select @pre_covid_price_ultracemco; /*4428.1*/
select @post_covid_price_ultracemco; /*3054.85*/

select round(((-@pre_covid_price_ultracemco+@post_covid_price_ultracemco)/@pre_covid_price_ultracemco),5)* 100 as UltraTech_Cement_Drawdown;

/*UltraTech Cement stock price fell 31.012 percent during covid fall*/

/*Final Outup
UltraTech Cement fell -31.012%
Bharti Airtel fell   -25.34%
Maruti Suzuki fell  -37.55%*/

create table covid_fall_percentage 
(symbol varchar(100), percentage_fall double);

insert into covid_fall_percentage
(symbol,percentage_fall)
values ("MARUTI",-37.55),("BHARTIART",-25.34),("ULTRACEMCO",-31.01);

select *, dense_rank() over(order by percentage_fall desc) as ranking from covid_fall_percentage  ;

/*Bharti Airtel Stock price fell the least and Maruti Suzuki stock price fell the most during covid */

/*Q3) Number of days stock price took to reach its pre covid levels*/

 /*Maruti Suzuki*/
  set  @date_close_more_than_precovid_maruti := 
    (select new_date from(
   select new_date, close, row_number() over(order by new_date asc) as rank_based_on_new_date from maruti
 where new_date between "2020-03-23" and "2021-04-30" and close>=@pre_covid_price_maruti
) as  A3
 where rank_based_on_new_date  =1 );
 
 select   @date_close_more_than_precovid_maruti ;
 
 select timestampdiff(day,"2020-03-23",@date_close_more_than_precovid_maruti ) as
 number_of_days_required_by_maruti_stock_price_to_close_above_its_pre_covid_level; 
 
 /*Ultratech Cement*/
 
   set  @date_close_more_than_precovid_ultracemco := 
    (select new_date from(
   select new_date, close, row_number() over(order by new_date asc) as rank_based_on_new_date from ultracemco
 where new_date between "2020-03-23" and "2021-04-30" and close>=@pre_covid_price_ultracemco
) as  A3_1
 where rank_based_on_new_date  =1 );
 
 select   @date_close_more_than_precovid_ultracemco ;
 
 select timestampdiff(day,"2020-03-23",@date_close_more_than_precovid_ultracemco ) as
 number_of_days_required_by_ultratech_stock_price_to_close_above_its_pre_covid_level; 
 
 /*Bhari Airtel*/
 
  set  @date_close_more_than_precovid_bhatiartl := 
    (select new_date from(
   select new_date, close, row_number() over(order by new_date asc) as rank_based_on_new_date from bhartiartl
 where new_date between "2020-03-23" and "2021-04-30" and close>=@pre_covid_price_bhartiartl 
) as  A3_2
 where rank_based_on_new_date  =1  );
 select @date_close_more_than_precovid_bhatiartl ;

select timestampdiff(day,"2020-03-23",@date_close_more_than_precovid_bhatiartl) as
 number_of_days_required_by_bhartiairtel_stock_price_to_close_above_its_pre_covid_level; 
 
 /*Days took by each stock price to reach its pre covid levels
 Bhari Airtel 44 Days
 UltraTech Cement 205 Days
 Maruti Suzuki  147 days
 */
 
 create table recovery_days
(symbol varchar(100), recovery_days_precovid_levels int);

insert into recovery_days
(symbol,recovery_days_precovid_levels)
values ("MARUTI",147),("BHARTIART",44),("ULTRACEMCO",205);
 
 
select *, dense_rank() over(order by recovery_days_precovid_levels  asc) as ranking from recovery_days  ;

/*Bharti Airtel had fastest recovery and ultratech had the slowest*/

/*Q4) Number of days stock price closed above its previous day closing price*/

create view A4 as ( select symbol,new_date,close from bhartiartl
union
select symbol,new_date,close  from maruti
union
select symbol,new_date,close  from ultracemco );


select symbol, sum(if((close>prev_day_cc),1,0)) as number_days_close_above_prev_close, dense_rank() over(order by
 sum(if((close>prev_day_cc),1,0)) desc ) as `rank` from
(select symbol, new_Date,close,lag(close) over(partition by symbol order by new_date asc) as prev_day_cc from A4 
) as xyz
group by symbol;

/*Bharti Airtel Closed 1380 days above its previous day closing price
Ultratech Cement Closed 1440 days above its previous day closing price
Maruti Closed 1396 days above its previous day closing price
 */

/*Q5) CAGR Calculation*/

/*maruti*/
set @begin_price_maruti := (select close from maruti where new_date = "2010-01-04" );
set @end_price_maruti := (select close from maruti where new_date = "2021-04-30"  );

select @begin_price_maruti ;  /*price as on 2010-01-04 is 1551.45*/
select @end_price_maruti;  /*price as on 2021-04-30 is 6455.65*/

set @number_of_years:= (select round(timestampdiff(day,"2010-01-04","2021-04-30")/365,3));

select round((power((@end_price_maruti/@begin_price_maruti),(1/@number_of_years) )-1)*100,4) as Maruti_CAGR ;

/*Ultratech Cement*/

set @begin_price_ultracemco := (select close from ultracemco where new_date = "2010-01-04" );
set @end_price_ultracemco := (select close from ultracemco  where new_date = "2021-04-30"  );

select @begin_price_ultracemco ;  /*price as on 2010-01-04 is 966.1*/
select @end_price_ultracemco;  /*price as on 2021-04-30 is 6278.95*/

set @number_of_years:= (select round(timestampdiff(day,"2010-01-04","2021-04-30")/365,3));

select round((power((@end_price_ultracemco/@begin_price_ultracemco),(1/@number_of_years) )-1)*100,4) as UltraTech_CAGR ;

/*Bharti Airtel */

set @begin_price_bhartiartl := (select close from bhartiartl where new_date = "2010-01-04" );
set @end_price_bhartiartl := (select close from bhartiartl  where new_date = "2021-04-30"  );

select @begin_price_bhartiartl ;  /*price as on 2010-01-04 is 319.20*/
select @end_price_bhartiartl;  /*price as on 2021-04-30 is 526.85*/

set @number_of_years:= (select round(timestampdiff(day,"2010-01-04","2021-04-30")/365,3));

select round((power((@end_price_bhartiartl/@begin_price_bhartiartl),(1/@number_of_years) )-1)*100,4) AS BhartiAirtel_CAGR ;

/*Output CAGR Return of all companies
Bharti Airtel 4.52%
UltraTech Cement 17.96 %
Maruti 13.41% */


 create table cagr
(symbol varchar(100), cagr_return_1 decimal(5,3));


insert into cagr
(symbol,cagr_return_1)
values ("MARUTI",13.41),("BHARTIART",4.52),("ULTRACEMCO",17.96);

select * from cagr;
 
select *, dense_rank() over(order by cagr_return_1  desc) as ranking from cagr  ;

/* Q6) Find month with highest volume*/

/*	Bharti Airtel*/

select symbol,year(new_date) as year,month(new_date) as month,max(volume) as Maximun_Volume from bhartiartl
group by year(new_date),month(new_date)
order by max(volume) desc 
limit 1;

/*Ultratech Cement*/
select symbol,year(new_date) as year,month(new_date) as month,max(volume) as  Maximun_Volume from ultracemco
group by year(new_date),month(new_date)
order by max(volume) desc 
limit 1;

/*Maruti Suzuki*/

select symbol,year(new_date) as year ,month(new_date) as month,max(volume) as  Maximun_Volume   from Maruti
group by year(new_date),month(new_date)
order by max(volume) desc 
limit 1;

create table max_volume
(Symbol varchar(100),max_volume_month varchar(100));

insert into max_volume
(Symbol, max_volume_month)
values
("BhariArtl","FEBRUARY 2021"),
("ULTRACEMCO","NOVEMBER 2016"),
("MARUTI","NOVEMBER 2015");

 select * from max_volume;
 
 
/*Max Volume
BhartAirtel - Feb 2021
Ultratech Cement - Nov-2016
Maruti Suzuki - Nov -2015

*/


/*Scoring based on the parameters we have discussed above
1) VOLATILITY
(Bharti Airtel is the Least Volatile and Maruti is the most Volatile)
ThereFore Score based on Volatality
 BhartiAitel -3
 Ultratech Cement -2
 Maruti -1

2) LOWER DRAWDOWN (Fall during covid period)
UltraTech Cement fell -31.012%
Bharti Airtel fell   - 25.34%
Maruti Suzuki fell  - 37.55%*

ThereFore Score based on LowerDrawDown-
 BhartiAitel -3
 Ultratech Cement -2
 Maruti -1

3) FASTER RECOVERY (number of days Take to reach pre covid price)
 Bhari Airtel 44 Days
 UltraTech Cement 205 Days
 Maruti Suzuki  147 days
 
ThereFore Score based on Faster Recovery-
 BhartiAitel -3
 Ultratech Cement -1
 Maruti -2


4) NUMBER OF DAYS STOCK PRICE CLOSED ABOVE ITS PREVIOUS CLOSED PRICE (Strength)

Bharti Airtel Closed 1380 days above its previous day closing price
Ultratech Cement Closed 1440 days above its previous day closing price
Maruti Closed 1396 days above its previous day closing price

ThereFore Score based on  Strength-
 BhartiAitel -1
 Ultratech Cement -3
 Maruti -2

5) CAGR RETURNS 

Bharti Airtel 4.52%
UltraTech Cement 17.96 %
Maruti 13.41%

ThereFore Score based on  CAGR Returns-
 BhartiAitel -1
 Ultratech Cement -3
 Maruti -2

*/

Create Table Score_Table (Symbol varchar(100), `description` varchar(100), Score int);

insert into score_table (symbol, `description`, Score)
values
("Bhartiart","volatility",3),("Ultracemco","volatility",2),("maruti","volatility",1),
("Bhartiart","Lowerdrawdown",3),("Ultracemco","Lowerdrawdown",2),("maruti","Lowerdrawdown",1),
("Bhartiart","FasterRecovery",3),("Ultracemco","FasterRecovery",1),("maruti","FasterRecovery",2),
("Bhartiart","Strength",1),("Ultracemco","Strength",3),("maruti","Strength",2),
("Bhartiart","CAGR",1),("Ultracemco","CAGR",3),("maruti","CAGR",2)
;

create table weightage_table( `description` varchar(100), weightage decimal(2,2));

insert into weightage_table (`description`, weightage)
values
("volatility",0.1),
("Lowerdrawdown",0.2),
("FasterRecovery",0.2),
("Strength",0.2),
("CAGR",0.2);


select * from score_table;
select * from weightage_table;

select symbol,st.description,score,weightage from score_table st inner join weightage_table wt on 
st.description=wt.description;

/*Calculating final score*/

select symbol,sum(round((score*weightage),10)) as final_score from score_table st inner join weightage_table wt on 
st.`description`=wt.`description`
group by symbol
order by final_score desc;





 
 

 
 
 
 









