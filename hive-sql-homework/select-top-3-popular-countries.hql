SELECT hotel_country,COUNT(hotel_country) AS booking_count 
FROM (SELECT hotel_country,is_booking FROM train_csv_only_table WHERE is_booking = 1) AS booked_only 
GROUP BY hotel_country 
ORDER BY booking_count DESC 
LIMIT 3;