############           EXPLORATION DATA ANALYSIS USING SQL- COVID DATA downloaded from 'https://ourworldindata.org/coronavirus'       #########

####################### Check the missing values or null values across the columns of interest #################################################
Select count(*) 
from covid.coviddeaths
where continent='' or location='' or date='' or total_cases is null or total_deaths is null or population is null;

select 'continent' as col, sum(case when continent is null || continent=''  then 1 else 0 end) as nullcount from covid.coviddeaths
union
select 'location' as col, sum(case when location is null || location='' then 1 else 0 end) as nullcount from covid.coviddeaths
union
select 'date' as col, sum(case when date is null || date='' then 1 else 0 end) as nullcount from covid.coviddeaths
union
select 'total_cases' as col, sum(case when total_cases is null || total_cases=''  then 1 else 0 end) as nullcount from covid.coviddeaths
union
select 'total_deaths' as col, sum(case when total_deaths is null || total_deaths='' then 1 else 0 end) as nullcount from covid.coviddeaths
union
select 'population' as col, sum(case when population is null || population='' then 1 else 0 end) as nullcount from covid.coviddeaths;

############################ Create a new date column to have the datatype changed to DATE format ################################################
alter table covid.coviddeaths 
add column new_date DATE;
update covid.coviddeaths
set new_date=str_to_date(date, '%m/%d/%Y'); 

alter table covid.covidvaccination 
add column new_date DATE;
update covid.covidvaccination
set new_date=str_to_date(date, '%m/%d/%Y'); 


################################### Need to find the missing continents so that we can update them ###############################################

select distinct location from covid.coviddeaths where continent='';

# Need to drop all these locations as these are alreay accounted under continents. 
 
 delete from covid.coviddeaths where continent='';
 delete from covid.covidvaccination where continent='';
 

####################################Total number of covid cases of each country/Continent##########################################################

with location_totals as 
(select continent, location, max(total_cases) as country_totals from covid.coviddeaths
group by location order by continent
)
 select 
  if(grouping(continent), 'All Continent', continent) as Continent,
 if(grouping(location), 'All Countries', location) as Country, sum(country_totals) as covid_Totals
 from location_totals
 group by continent, location with rollup;
#################################### Covid Rate by Continent ########################################################################################
with location_total as 
    (select continent, location, max(total_cases) as loc_case 
     from covid.coviddeaths
     group by continent, location
     order by continent asc),      
     
     continental_population as
     (select continent, sum(distinct population) as cont_population
     from covid.coviddeaths
     group by continent)

select a.continent, b.cont_population, sum(a.loc_case),
       #sum(a.loc_case) as continental_total,
       round((sum(a.loc_case)/b.cont_population)*100,2) as continental_covidrate
from location_total a join continental_population b on a.continent=b.continent
group by a.continent;     

############################################## Mortality Rate by Country ###############################################################################
Select location, max(total_cases)/max(population) as covid_rate from covid.coviddeaths
group by location
order by covid_rate desc
limit 10;


############################################ Fatality Rate by country ###################################################################################

Select location, round((max(total_deaths)/max(total_cases))*100,2) as Fatality_rate from covid.coviddeaths
group by location
order by Fatality_rate desc
limit 10;


 ################################### Using windows function to calculate new case and new deaths per day #################################################
 
 select location, new_date,
		total_cases-lag(total_cases,1) over 
	    (partition by location order by new_date) as NewCaseperDay,
        total_deaths-lag(total_deaths,1) over 
        (partition by location order by new_date) as NewDeathsperDay
 from covid.coviddeaths;
 
 ############################################ 7 day Moving Average of New covid cases #####################################################################

select *, 
	avg(new_cases_per_million) over (partition by location
                         order by new_date
                         rows between 7 preceding and current row) as 7day_moving_average
from covid.coviddeaths
where location in ('United States', 'United Kingdom', 'China', 'India', 'Canada');



###########################################Joining Covid deaths and covid vaccination tables#################################################################
(select cd.continent, cd.location, cd.new_date, cd.total_cases, cv.total_tests
from coviddeaths cd
left join covidvaccination cv
on cd.location=cv.location and cd.new_date = cv.new_date
)
union
(select cd.continent, cd.location, cd.new_date, cd.total_cases, cv.total_tests
from coviddeaths cd
right join c
on cd.location=cv.location and cd.new_date = cv.new_date
);

############################################################# Temp Table- Global Covid data per day ############################################################
Drop table if exists Global_covidData;
create temporary table Global_covidData
(
Date datetime,
Global_TotalCasesperDay bigint,
Global_TotalTestperDay bigint,
Global_TotalVaccinationperDay bigint,
Global_Boosters bigint
);
insert into Global_covidData
select cd.new_date, 
	   sum(cd.total_cases)  as Global_TotalCasesperDay,
       sum(cv.total_tests) as Global_TotalTestperDay,
       sum(cv.total_vaccinations)  as Global_TotalVaccinationperDay,
       sum(cv.total_boosters) as Global_Boosters
from  coviddeaths cd
inner join covidvaccination  cv
on  cd.new_date = cv.new_date 
group by cd.new_date
order by cd.new_date asc

########################################################## END ####################################################################################################