-- This SQL Function creates a datamart from the historical data table for the
-- crypto-currency that is passed as a parameter to the script
-- example call : - select create_coin_datamart('BTC/USD')

CREATE OR REPLACE FUNCTION public.create_coin_datamart(coin text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
declare
table_name varchar;
table_name_new varchar;
test varchar;
count_var int;
	begin
		table_name := concat(coin, '_datamart_example') ;
		table_name_new := concat(left(coin,3), '_datamart_example') ;

		raise notice 'coin Value: %', coin;
	    raise notice 'table_name Value: %', table_name_new;


	   select  into count_var count(*)
	   FROM pg_catalog.pg_tables
	   WHERE  tablename  = lower(table_name_new);



	   if count_var >=1 then

	   raise notice 'Data Mart Already Exists!';

--	   	            execute format('INSERT INTO %s
--	                select * from hourly_historical_coin_data
--	                where symbol = $1
--					ON CONFLICT ("unix")
--					DO
--  					UPDATE SET
--  				    "date" = hourly_historical_coin_data."date" ,
--  				    "symbol" = hourly_historical_coin_data."symbol" ,
--  				    "open" = hourly_historical_coin_data."open" ,
--  				    "high" = hourly_historical_coin_data."high" ,
--  				    "low" = hourly_historical_coin_data."low" ,
--  				    "close" = hourly_historical_coin_data."close" ,
--  				    "volume-coin" = hourly_historical_coin_data."volume-coin" ,
--  				    "volume-usd" = hourly_historical_coin_data."volume-usd" ,
--  				    "just_date" = hourly_historical_coin_data."just_date" ,
--  				    "just_time" = hourly_historical_coin_data."just_time"
--  				    ;', table_name_new)
--  				    using coin;

--	   IF EXISTS (SELECT FROM pg_catalog.pg_tables
--	              WHERE  schemaname = 'public'
--	              AND    tablename  = lower(table_name_new))
--	              then
--
--	              execute format('INSERT INTO %s
--	                select * from hourly_historical_coin_data
--	                where symbol = $1
--					ON CONFLICT ("unix")
--					DO
--  					UPDATE SET
--  				    "date" = hourly_historical_coin_data."date" ,
--  				    "symbol" = hourly_historical_coin_data."symbol" ,
--  				    "open" = hourly_historical_coin_data."open" ,
--  				    "high" = hourly_historical_coin_data."high" ,
--  				    "low" = hourly_historical_coin_data."low" ,
--  				    "close" = hourly_historical_coin_data."close" ,
--  				    "volume-coin" = hourly_historical_coin_data."volume-coin" ,
--  				    "volume-usd" = hourly_historical_coin_data."volume-usd" ,
--  				    "just_date" = hourly_historical_coin_data."just_date" ,
--  				    "just_time" = hourly_historical_coin_data."just_time"
--  				    ;', table_name_new)
--  				    using coin;

	      RAISE NOTICE 'Table myschema.mytable already exists.';
	   else
	   		EXECUTE format('CREATE TABLE %s	("unix" numeric, "date" varchar(50), "symbol" varchar,
				 "open" float8, "high" float8, "low" float8, "close" float8,
	     "volume-coin" float8, "volume-usd" float8,
	     "just_date" date, "just_time" varchar)', table_name_new);

  			    --USING table_name;

  			 /*  CREATE TABLE public.table_name
	     (
	      "unix" numeric, "date" varchar(50), "symbol" varchar,
	     "open" float8, "high" float8, "low" float8, "close" float8,
	     "volume-coin" float8, "volume-usd" float8,
	     "just_date" date, "just_time" varchar ); */



 EXECUTE format('INSERT INTO %s
					select * from hourly_historical_coin_data where symbol = $1',table_name_new)
					using coin;

	raise  notice 'here3';
  			    --USING table_name, coin;
       /* INSERT INTO table_name
        select * from hourly_historical_coin_data
        where symbol = coin;*/
	   END IF;

	END;
$function$
;
