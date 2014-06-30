postcodes = LOAD '/user/cscarion/postcodes' USING PigStorage(',') AS(postcode,x,y);
customers = LOAD '/user/cscarion/aggregated_customers_text' using PigStorage('|') AS(id, vertical, trade, turnover, claims,rfq_id, postcode);

with_coordinates = JOIN customers BY postcode, postcodes BY postcode;

final_data = FOREACH with_coordinates GENERATE customers::id, customers::vertical, customers::trade, customers::turnover, customers::claims, customers::rfq_id, customers::postcode, postcodes::x,postcodes::y;
store final_data into 'aggregated_customers_with_coordinates' using PigStorage('|');
