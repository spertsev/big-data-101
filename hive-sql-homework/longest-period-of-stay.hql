SELECT d.days_of_stay,
       d.date_time,
       d.orig_destination_distance,
       d.user_id,
       d.srch_ci,
       d.srch_co
FROM
  (SELECT datediff(to_date(srch_co), to_date(srch_ci)) AS days_of_stay,
          date_time,
          orig_destination_distance,
          user_id,
          srch_ci,
          srch_co
   FROM train_csv_only_table
   WHERE srch_adults_cnt=2
     AND srch_children_cnt>0) AS d
ORDER BY d.days_of_stay DESC
LIMIT 10;