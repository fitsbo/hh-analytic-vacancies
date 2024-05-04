drop view if exists v_vacancies;
DROP TABLE IF EXISTS hh_project.vacancies;
CREATE TABLE hh_project.vacancies 
(
    id SERIAL PRIMARY KEY,
    vacancy_id INTEGER,
    created_at TIMESTAMP WITH TIME ZONE,
    name VARCHAR NOT NULL,
    title_category VARCHAR NOT NULL,
    alternate_url VARCHAR,
    area_id INTEGER,
    area_name VARCHAR,
    type_name VARCHAR,
    experience VARCHAR,
    schedule VARCHAR,
    employment VARCHAR,
    accept_handicapped BOOLEAN,
    accept_temporary BOOLEAN,
    archived BOOLEAN,
    salary VARCHAR,
    employer_id INTEGER,
    employer_name VARCHAR,
    salary_gross BOOLEAN,
    salary_currency VARCHAR,
    salary_from FLOAT,
    salary_to FLOAT,
    salary_avg FLOAT,
    salary_from_currency FLOAT,
    salary_to_currency FLOAT,
    salary_avg_currency FLOAT,
    region_id INTEGER,
    region_name VARCHAR,
    valid_dttm DATE DEFAULT '5999-12-31',
    processed_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS hh_project.dic_region;
CREATE TABLE hh_project.dic_region 
(
  id SERIAL PRIMARY KEY,
  name VARCHAR NOT NULL,
  type VARCHAR NOT NULL,
  okrug VARCHAR NOT NULL,
  iso3166 VARCHAR,
  gost767_2 VARCHAR,
  gost767_3 VARCHAR,
  valid_dttm DATE DEFAULT '5999-12-31',
  processed_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS hh_project.dic_cities;
CREATE TABLE hh_project.dic_cities 
(
  id SERIAL PRIMARY KEY,
  city VARCHAR NOT NULL,
  type VARCHAR NOT NULL,
  valid_dttm DATE DEFAULT '5999-12-31',
  processed_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS hh_project.dic_region_salaries;
CREATE TABLE hh_project.dic_region_salaries 
(
  id SERIAL PRIMARY KEY,
  month DATE,
  salary FLOAT,
  region_name VARCHAR,
  valid_dttm DATE DEFAULT '5999-12-31',
  processed_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS hh_project.languages;
CREATE TABLE hh_project.languages 
(
  id SERIAL PRIMARY KEY,
  vacancy_id INTEGER,
  language VARCHAR NOT NULL,
  level VARCHAR NOT NULL,
  valid_dttm DATE DEFAULT '5999-12-31',
  processed_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS hh_project.professional_roles;
CREATE TABLE hh_project.professional_roles 
(
  id SERIAL PRIMARY KEY,
  vacancy_id INTEGER,
  role_id INTEGER,
  role_name VARCHAR NOT NULL,
  valid_dttm DATE DEFAULT '5999-12-31',
  processed_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS hh_project.industries;
CREATE TABLE hh_project.industries 
(
  id SERIAL PRIMARY KEY,
  employer_id INTEGER,
  industry_id INTEGER,
  industry_name VARCHAR NOT NULL,
  valid_dttm DATE DEFAULT '5999-12-31',
  processed_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS hh_project.key_skills;
CREATE TABLE hh_project.key_skills 
(
  id SERIAL PRIMARY KEY,
  vacancy_id INTEGER,
  skill VARCHAR NOT NULL,
  valid_dttm DATE DEFAULT '5999-12-31',
  processed_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DROP TABLE IF EXISTS hh_project.employers;
CREATE TABLE hh_project.employers 
(
    id SERIAL PRIMARY KEY,
    employer_id INTEGER,
    name VARCHAR NOT NULL,
    type VARCHAR,
    alternate_url VARCHAR,
    site_url VARCHAR,
    area_id INTEGER,
    area_name VARCHAR,
    open_vacancies INTEGER,
    valid_dttm DATE DEFAULT '5999-12-31',
    processed_dttm TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

drop view if exists v_vacancies;
create view v_vacancies as (
select
    v.id
    ,v.vacancy_id
    ,v.created_at
    ,v.name
    ,v.title_category
    ,pr.role_name
    ,v.alternate_url
    ,v.area_id
    ,v.area_name
    ,v.type_name
    ,v.experience
    ,v.schedule
    ,v.employment
    ,v.accept_handicapped
    ,v.accept_temporary
    ,v.archived
    ,v.salary
    ,v.employer_id
    ,v.employer_name
    ,v.salary_from
    ,v.salary_to
    ,v.salary_avg
    ,v.salary_currency
    ,v.salary_gross
    ,v.salary_to_currency
    ,v.salary_from_currency
    ,v.salary_avg_currency
    ,v.region_id
    ,v.region_name
    ,v.processed_dttm
	,dr.okrug 
	,dr.iso3166 
	,rs.salary as region_salary
	,ms.market_salary
	,case
		when dc."type" is null then 'Регион'
		else dc."type" 
	end as area_type
from hh_project.vacancies v 
	left join hh_project.dic_region dr on v.region_name = dr."name"
										and dr.valid_dttm = '5999-12-31'
	left join hh_project.dic_cities dc on v.area_name = dc.city  
										and dc.valid_dttm = '5999-12-31'
	left join (
		select
			pr.vacancy_id 
			,pr.role_name 
			,row_number() over (partition by pr.vacancy_id order by pr.id desc) as rn
		from hh_project.professional_roles pr
		where valid_dttm = '5999-12-31'
	) as pr on v.vacancy_id = pr.vacancy_id and pr.rn = 1
	left join hh_project.dic_region_salaries as rs on v.region_name = rs.region_name
												and date_trunc('month', v.created_at) = rs.month
												and rs.valid_dttm = '5999-12-31'
	left join
	(
		select
			date_trunc('month', v.created_at)::date as month
			,pr.role_name
			,v.experience
			,v.schedule
			,v.region_name
			,round(avg(v.salary_avg)::numeric, 2) as market_salary
		from hh_project.vacancies v
			left join (
				select
					pr.vacancy_id 
					,pr.role_name 
					,row_number() over (partition by pr.vacancy_id order by pr.id desc) as rn
				from hh_project.professional_roles pr
				where valid_dttm = '5999-12-31'
			) as pr on v.vacancy_id = pr.vacancy_id and pr.rn = 1
		where v.valid_dttm = '5999-12-31'
		group by date_trunc('month', v.created_at)
			,pr.role_name
			,v.experience
			,v.schedule
			,v.region_name
	) as ms on date_trunc('month', v.created_at) = ms.month
		 and pr.role_name = ms.role_name
		 and v.experience = ms.experience
		 and v.schedule = ms.schedule
		 and v.region_name = ms.region_name
		 and v.salary_avg is not null
where v.valid_dttm = '5999-12-31'
);