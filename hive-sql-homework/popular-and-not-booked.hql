SELECT not_booked.hotel_continent,
       not_booked.hotel_country,
       not_booked.hotel_market,
       COUNT(*) AS number_of_rows
FROM
  (SELECT *
   FROM train_csv_only_table
   WHERE is_booking=0) AS not_booked
GROUP BY not_booked.hotel_continent,
         not_booked.hotel_country,
         not_booked.hotel_market
ORDER BY number_of_rows DESC
LIMIT 3;