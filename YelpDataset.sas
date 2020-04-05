/* 
Create a library called yelp to store the yelp datasets 
*/

LIBNAME yelp '/folders/myfolders/yelp';


/*
Import the business.csv file and save it to a dataset called business.
*/

FILENAME REFFILE '/folders/myfolders/yelp_business.csv';

PROC IMPORT DATAFILE=REFFILE
	DBMS=CSV
	OUT=YELP.business;
	GETNAMES=YES;
RUN;

PROC CONTENTS DATA=YELP.business; 
RUN;

/*
Inspect the results

Dataset size = 46 MB

Fields: 

Any fields that we don't intend to use? Address. Neighborhood
Drop the variable. 

Formats:
Talk about the formats of each variable
*/

DATA YELP.business; SET YELP.business;
	drop address neighborhood;
RUN;

PROC CONTENTS DATA=YELP.business; 
RUN;

/*
Inspect the results

Dataset size = 36 MB
*/


/* Check the frequency of businesses by state.

The 11 states with the most businesses account for 99% of the data. */

PROC FREQ DATA = YELP.business ORDER= freq;
	table state;
RUN;

/*
We also want to check the percentage of businesses classified as restaurants

Before deleting the states that have few business listed (<1000),
we want to check the frequency of restaturants by state.

The categories variable consists of multiple entries separated by semicolons.
A visual inspect suggests that most of the businesses have fewer than 5 categories. 
We decided to extract the first 5 categories for each business using the scan function.
*/

DATA YELP.business2one; SET YELP.business;
		categorycount = countw(categories, ";");
RUN;

PROC FREQ DATA = YELP.business2one;
 table categorycount;
RUN;

DATA YELP.business2two; SET YELP.business;
	category1 = scan(categories, 1, ';');
	category2 = scan(categories, 2, ';');
	category3 = scan(categories, 3, ';');
	category4 = scan(categories, 4, ';');
	category5 = scan(categories, 5, ';');
RUN;

/*
How many categories do businesses have? Correlation with other variables?
Create a separate table for upto 10 categories. Add a variable that counts the number of entries.
Or: https://communities.sas.com/t5/SAS-Procedures/Counting-similar-comma-separated-values-in-columns/td-p/570670
*/

PROC CONTENTS DATA=YELP.business2; 
RUN;

PROC FREQ data = YELP.business2 ORDER= freq;
	tables category1 category2 category3 category4 category5;
RUN;

/* 
Restaurants is the most popular category. No error in tag name. We will subset restaurants
and check distribution across states.
*/

/* 
Delete all non restaurants. Remaining = 54206 
*/

data YELP.business3; set YELP.business;
	if index(categories, 'Restaurants');
	categorycount = countw(categories, ";");
RUN;


/*
Identify largest states by business count
10 states, 98% of data.
*/

PROC FREQ DATA = YELP.business3 ORDER= freq;
	table state;
RUN;

PROC CONTENTS DATA=YELP.business3; 
RUN;

PROC FREQ DATA = YELP.business3;
	table categorycount;
RUN;


/* 
Selected States Only - why we're rejecting EU states
*/

DATA YELP.business4; set YELP.business3;
IF state IN ("ON", "AZ", "NV", "QC", "OH", "NC" ,"PA");
RUN;

/*
Inspect each state - cities, urban/rural places 
*/

/* */

PROC FREQ DATA = YELP.business4;
TABLES stars * is_open;
RUN;


/*
____USERS____
*/

/* Exploratory Analysis on Users */
/* User data set was split into 12 chunks of smaller size */
/* First Chunk - User_1 */

FILENAME REFFILE '/folders/myfolders/Yelp/yelp_user-1.csv';

PROC IMPORT DATAFILE=REFFILE
	DBMS=CSV
	OUT=YELP.USER_1;
	GETNAMES=YES;
RUN;

PROC CONTENTS DATA=YELP.USER_1; RUN;


/* Size of the user_1 file is too large around 721 MB , So we have to remove some unwanted columns and make changes to the others */

DATA Yelp.USER_1_M; SET YELP.USER_1;
	TotComp = compliment_hot + compliment_more + compliment_profile + compliment_cute + compliment_list + compliment_note + compliment_plain + compliment_funny + compliment_writer + compliment_photos + compliment_cool ;
	DROP compliment_hot compliment_more compliment_profile compliment_cute compliment_list compliment_note compliment_plain compliment_funny compliment_writer compliment_photos compliment_cool;

RUN;

DATA YELP.USER_1_M; SET WORK.USER_1_M;
	IF NOT MISSING(TotComp);
	friendcount = countw(friends, ",");
 	DROP name friends;
RUN;

PROC CONTENTS DATA=YELP.USER_1_M; RUN; /* After some cleaning the file size has reduced to 10MB */

/* Second Chunk - User_2 */

FILENAME REFFILE '/folders/myfolders/Yelp/yelp_user-2.csv';

PROC IMPORT DATAFILE=REFFILE
	DBMS=CSV
	OUT=YELP.USER_2;
	GETNAMES=YES;
RUN;

PROC CONTENTS DATA=YELP.USER_2; RUN;

/* Size of the user_2 file is too large around 1GB , So we have to remove some unwanted columns and make changes to the others just like what we did with Chunk 1*/

DATA Yelp.USER_2_M; SET YELP.USER_2;
	TotComp = compliment_hot + compliment_more + compliment_profile + compliment_cute + compliment_list + compliment_note + compliment_plain + compliment_funny + compliment_writer + compliment_photos + compliment_cool ;
	DROP compliment_hot compliment_more compliment_profile compliment_cute compliment_list compliment_note compliment_plain compliment_funny compliment_writer compliment_photos compliment_cool;

RUN;

DATA YELP.USER_2_M; SET Yelp.USER_2_M;
	IF NOT MISSING(TotComp);
	friendcount = countw(friends, ",");
 	DROP name friends;
RUN;

PROC CONTENTS DATA=YELP.USER_2_M; RUN; /* After some cleaning the file size has reduced to 13 MB */

/* We keep on doing data cleaning for the remaining 10 chunks and then we merge them */

data yelp.final_users ;
set user_1_m user_2_m user_3_m user_4_m user_5_m user_6_m user_7_m user_8_m user_9_m user_10_m user_11_m user_12_m;
run; 

/* Changing the character columns in User to Numeric for our analysis */

data yelp.users; set yelp.final_users;
	format using_since yymmdd10.;
	avg_stars = input(average_stars, 8.);
	useful_n = input(useful, 8.);
	cool_n = input(cool, 8.);
	funny_n = input(funny, 8.);
	review_n = input(review_count, 8.);
	fans_n = input(fans, 8.);
	funny_n = input(funny, 8.);
	if elite = "None" 
		then elite_ever = 0;
	else elite_ever = 1;
	using_since = input(yelping_since, YYMMDD10.);

	drop average_stars useful cool funny review_count fans funny elite yelping_since;
run;


PROC CONTENTS DATA=YELP.users; RUN;  /* The final USERS dataset which we use for our analysis is of 132 MB */

/* Reviews Data set was even larger than USERS, so we cleaned it in R */
/* The CSV file reduced to 536 MB from 3.53 GB */ 
FILENAME REFFILE '/folders/myfolders/Yelp/yelp_Reviews.csv';

PROC IMPORT DATAFILE=REFFILE
	DBMS=CSV
	OUT=YELP.REVIEWS;
	GETNAMES=YES;
RUN;

PROC CONTENTS DATA=YELP.REVIEWS; RUN;  /* The final Reviews file we will be using for our analysis is 604 MB */


/*Business Table: Hypothesis Analysis  */
 
 
/* Logit Model */

/* Query code generated for SAS Studio by Common Query Services */

/* Using the Business4 table we calculated Zipdensity table using the postal codes(It shows us the competetion of resstaurants in the same area) */ 

PROC SQL; 
CREATE TABLE YELP.ZIPDENSITY 
AS 
SELECT BUSINESS4.postal_code, COUNT(BUSINESS4.business_id)
AS bus_count 
FROM YELP.BUSINESS4 BUSINESS4 
GROUP BY BUSINESS4.postal_code 
ORDER BY bus_count DESC; 
QUIT;

/* We then Joined Zipdensity with Business4 using Proc SQL Left join */

PROC SQL; 
CREATE TABLE YELP.LOGIT 
AS 
SELECT BUSINESS4.business_id, BUSINESS4.name, BUSINESS4.city, BUSINESS4.state, BUSINESS4.postal_code, BUSINESS4.latitude, BUSINESS4.longitude, BUSINESS4.stars, BUSINESS4.review_count, BUSINESS4.is_open, BUSINESS4.categories, BUSINESS4.categorycount, ZIPDENSITY.bus_count 
FROM YELP.BUSINESS4 BUSINESS4 
LEFT JOIN YELP.ZIPDENSITY ZIPDENSITY 
ON 
   ( BUSINESS4.postal_code = ZIPDENSITY.postal_code ) ; 
QUIT;

/* Running a Logit Model to predict if a restaurant is open or closed based on data made available */

ods noproctitle;
ods graphics / imagemap=on;

proc logistic data=YELP.LOGIT;
	model is_open(event='1')=stars review_count bus_count / link=logit 
		technique=fisher;
run;

/* Joining Business4 and Reviews for further analysis */

PROC SQL; 
CREATE TABLE Yelp.bus_rev 
AS 
SELECT BUSINESS4.business_id, BUSINESS4.name, BUSINESS4.city, BUSINESS4.state, BUSINESS4.postal_code, BUSINESS4.latitude, BUSINESS4.longitude, BUSINESS4.stars, BUSINESS4.review_count, BUSINESS4.is_open, BUSINESS4.categories, BUSINESS4.categorycount, REVIEWS.user_id, REVIEWS.review_id 
FROM YELP.BUSINESS4 BUSINESS4 
INNER JOIN YELP.REVIEWS REVIEWS 
ON 
   ( BUSINESS4.business_id = REVIEWS.business_id ) ; 
QUIT;

/* Joining Bus_rev and Users for further analysis */

PROC SQL; 
CREATE TABLE Yelp.usermode 
AS 
SELECT USERS.user_id, USERS.TotComp, USERS.friendcount, USERS.using_since, USERS.average_stars, USERS.useful, USERS.cool, USERS.funny, USERS.review_count, USERS.fans, USERS.elite_ever, BUS_REV.city, BUS_REV.state 
FROM YELP.BUS_REV BUS_REV 
INNER JOIN YELP.USERS USERS 
ON 
   ( BUS_REV.user_id = USERS.user_id ) ; 
QUIT;

/* The following codes gives you the most commonly occurring city for each user */

data yelp.usermode;
set yelp.usersel;
order = _n_;
run;



proc sql;
create table userCity as
select user_id, city
from
    (select user_id, city, last
    from
        (select user_id, city, count(*) as n, max(order) as last
        from yelp.usermode
        group by user_id, city)
    group by user_id
    having n = max(n))
group by user_id
having last=max(last); 
quit;
/* */

PROC SQL; 
CREATE TABLE YELP.USERCITYST 
AS 
SELECT USERS.user_id, USERS.TotComp, USERS.friendcount, USERS.using_since, USERS.average_stars, USERS.useful, USERS.cool, USERS.funny, USERS.review_count, USERS.fans, USERS.elite_ever, USERWITHCITY.city 
FROM YELP.USERS USERS 
INNER JOIN YELP.USERWITHCITY USERWITHCITY 
ON 
   ( USERS.user_id = USERWITHCITY.user_id ) ; 
QUIT;

/* Creating USERWITHLOC , We add the city back in */
PROC SQL; 
CREATE TABLE WORK.USERWITHLOC 
AS 
SELECT USERWITHCITY.user_id, USERWITHCITY.city, USERS.user_id 
AS user_id2, USERS.TotComp, USERS.friendcount, USERS.using_since, USERS.average_stars, USERS.useful, USERS.cool, USERS.funny, USERS.review_count, USERS.fans, USERS.elite_ever 
FROM YELP.USERS USERS 
INNER JOIN YELP.USERWITHCITY USERWITHCITY 
ON 
   ( USERS.user_id = USERWITHCITY.user_id ) ; 
QUIT;


/* Exploratory Data Analysis on USERSWITHLOC table */

/* A] FRIENDS AND FANS 
		A.1] USERS*/

PROC SQL; 
CREATE TABLE WORK.QUERY1 
AS 
SELECT USERWITHLOC.state 
AS STATE, COUNT(USERWITHLOC.user_id) 
AS TOTAL_USERS, SUM(USERWITHLOC.elite_ever) 
AS TOTAL_ELITE 
FROM Yelp.USERWITHLOC USERWITHLOC 
GROUP BY USERWITHLOC.state 
ORDER BY TOTAL_USERS DESC; 
QUIT;
	
PROC EXPORT 
	DATA=WORK.QUERY1
	DBMS=xlsx
	outfile='/folders/myfolders/Yelp/FRNDFAN_USER.xlsx'
	replace;
run;	

/* A.2] ELITE */
		
PROC SQL;
CREATE TABLE WORK.query 
AS
SELECT 'STATE'n , TOTAL_ELITE , AVG_FRIEND , AVG_FANS FROM WORK.QUERY1;
RUN;
QUIT;

PROC DATASETS NOLIST NODETAILS;
CONTENTS DATA=WORK.query OUT=WORK.details;
RUN;

PROC PRINT DATA=WORK.details;
RUN;
		
PROC EXPORT 
	DATA=WORK.QUERY
	DBMS=xlsx
	outfile='/folders/myfolders/Yelp/FRNDFAN_ELITE.xlsx'
	replace;
run;


/* B] AVEGRAGE RATINGS AND RATINGS COUNT 
		B.1] USERS*/


PROC SQL; 
CREATE TABLE WORK.STAR_REV_USER 
AS 
SELECT USERWITHLOC.state 
AS STATE, COUNT(USERWITHLOC.user_id) 
AS TOTAL_USERS, AVG(USERWITHLOC.review_count) 
AS AVG_REVIEW, AVG(USERWITHLOC.average_stars) 
AS AVG_STARS 
FROM YELP.USERWITHLOC USERWITHLOC 
GROUP BY USERWITHLOC.state; 
QUIT;


PROC EXPORT 
	DATA=WORK.STAR_REV_USER
	DBMS=xlsx
	outfile='/folders/myfolders/Yelp/STAR_REV_USER.xlsx'
	replace;
run;


/* B.2] ELITE */

PROC SQL; 
CREATE TABLE WORK.QUERY2 
AS 
SELECT USERWITHLOC.state 
AS STATE, SUM(USERWITHLOC.elite_ever) 
AS TOTAL_ELITE, AVG(USERWITHLOC.review_count) 
AS AVG_REVIEW, AVG(USERWITHLOC.average_stars) 
AS AVG_STARS 
FROM YELP.USERWITHLOC USERWITHLOC 
WHERE 
   ( USERWITHLOC.elite_ever = 1 ) 
GROUP BY USERWITHLOC.state; 
QUIT;


PROC EXPORT
	DATA = WORK.QUERY2
	DBMS = xlsx
	outfile='/folders/myfolders/Yelp/final_elite_review.xlsx'
	replace;
run;

/* Distribution of Average stars given by Users */

ods graphics / reset width=6.4in height=4.8in imagemap;

proc sgplot data=YELP.USERWITHLOC;
	histogram average_stars / scale=count nbins=25;
	density average_stars;
	yaxis grid;
run;

ods graphics / reset;

/* Distribution of Stars which restaurants got */

ods graphics / reset width=6.4in height=4.8in imagemap;

proc sgplot data=YELP.BUSINESS4;
	histogram stars / scale=count nbins=10;
	density stars;
	yaxis grid;
run;

ods graphics / reset;
	