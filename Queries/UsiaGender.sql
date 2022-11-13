select extract(year from age(to_date(birthdate_customer,'yyyy'))) as usia , gender_customer from bigdata_customer 
