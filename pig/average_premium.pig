premiums = LOAD '/user/cscarion/imported_quotes' USING PigStorage('|') AS(rfq_id, premium);
cluster = LOAD '/user/cscarion/individual-clusters/part-r-00000' AS(clusterId, customerId);
customers = LOAD '/user/cscarion/aggregated_customers_text' using PigStorage('|') AS(id, vertical, trade, turnover, claims,rfq_id);

withPremiums = JOIN premiums BY rfq_id, customers BY rfq_id;

rmf '/user/cscarion/withPremiums';

store withPremiums into 'withPremiums' using PigStorage('|');

groupCluster2 = JOIN withPremiums BY customers::id, cluster BY customerId;

grouped2 = GROUP groupCluster2 BY cluster::clusterId;

premiumsAverage = FOREACH grouped2 GENERATE group,    AVG(groupCluster2.withPremiums::premiums::premium);

rmf '/user/cscarion/premiumsAverage';

STORE premiumsAverage into 'premiumsAverage' using PigStorage('|');
