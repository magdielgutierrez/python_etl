
DROP TABLE IF EXISTS  sales_car.brnz_car_prices;
CREATE TABLE IF NOT EXISTS brnz_car_prices AS 
SELECT year				, make		, model	
		, transmission	, state		, odometer
		, color			, interior	, seller
		, sellingprice	, saledate	, trim
		, body			, mmr 
FROM raw_car_prices 
WHERE p_year=strftime('%Y', 'now') 
AND p_month=strftime('%m', 'now') 
AND p_day=strftime('%d', 'now') 
AND  LENGTH (state)=2;


DROP TABLE IF EXISTS  slver_car_prices;
CREATE TABLE slver_car_prices AS
SELECT year, make, model, transmission, color,  seller, body,   
	 odometer,
	 sellingprice,
	 CASE WHEN sellingprice < 10000 THEN ' Menos de 10K' 
		 WHEN sellingprice > 9999 AND sellingprice < 20000 THEN 'Entre 10K-20K'
		 WHEN sellingprice > 19999 AND sellingprice < 30000 THEN 'Entre 20K-30K'
		 WHEN sellingprice > 29999 AND sellingprice < 40000 THEN 'Entre 30K-40K'
		 WHEN sellingprice > 39999 AND sellingprice < 50000 THEN 'Entre 40K-50K'
		 WHEN sellingprice > 49999 AND sellingprice < 60000 THEN 'Entre 50K-60K'
		 WHEN sellingprice > 59999 AND sellingprice < 70000 THEN 'Entre 60K-70K'
		 WHEN sellingprice > 69999 AND sellingprice < 80000 THEN 'Entre 70K-80K'
		 WHEN sellingprice > 79999 AND sellingprice < 90000 THEN 'Entre 80K-90K'
		 WHEN sellingprice > 89999 AND sellingprice < 100000 THEN 'Entre 90K-100K'
		 WHEN sellingprice > 99999 THEN 'Mas de 100K'
	END wk_range_price,
	saledate,
	CASE substr(saledate, 1, 3)
        WHEN 'Mon' THEN 'Lunes'
        WHEN 'Tue' THEN 'Martes'
        WHEN 'Wed' THEN 'Miércoles'
        WHEN 'Thu' THEN 'Jueves'
        WHEN 'Fri' THEN 'Viernes'
        WHEN 'Sat' THEN 'Sábado'
        WHEN 'Sun' THEN 'Domingo'
    END AS dia,
    -- Extraer la fecha en formato YYYY-MM-DD
    substr(saledate, 9, 2) || '-' || 
    CASE substr(saledate, 5, 3)
        WHEN 'Jan' THEN '01'
        WHEN 'Feb' THEN '02'
        WHEN 'Mar' THEN '03'
        WHEN 'Apr' THEN '04'
        WHEN 'May' THEN '05'
        WHEN 'Jun' THEN '06'
        WHEN 'Jul' THEN '07'
        WHEN 'Aug' THEN '08'
        WHEN 'Sep' THEN '09'
        WHEN 'Oct' THEN '10'
        WHEN 'Nov' THEN '11'
        WHEN 'Dec' THEN '12'
    END || '-' ||
    substr(saledate, 12, 4) AS fecha,
    -- Extraer y convertir la hora al formato de 12 horas
         CASE 
        WHEN CAST(substr(saledate, 17, 2) AS INTEGER) > 12 THEN CAST(substr(saledate, 17, 2) AS INTEGER) - 12
        WHEN CAST(substr(saledate, 17, 2) AS INTEGER) = 0  THEN 12
        ELSE CAST(substr(saledate, 17, 2) AS INTEGER)
    END || ':' ||
    substr(saledate, 20, 2) || ' ' ||
 	CASE WHEN CAST(substr(saledate, 12, 2) AS INTEGER) >= 12 THEN 'PM' ELSE 'AM' END AS hora
FROM brnz_car_prices WHERE sellingprice IS NOT NULL AND year>2001;
