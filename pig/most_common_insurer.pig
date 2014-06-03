quotes = LOAD '/user/cscarion/imported_quotes' USING PigStorage('|') AS(rfq_id, premium, insurer);
cluster = LOAD '/user/cscarion/individual-clusters/part-r-00000' AS(clusterId, customerId);
customers = LOAD '/user/cscarion/aggregated_customers_text' using PigStorage('|') AS(id, vertical, trade, turnover, claims,rfq_id);

withInsurers = JOIN quotes BY rfq_id, customers BY rfq_id;

groupCluster2 = JOIN withInsurers BY customers::id, cluster BY customerId;

grouped2 = GROUP groupCluster2 BY (cluster::clusterId, withInsurers::quotes::insurer);

countOfInsurer = FOREACH grouped2 GENERATE group, COUNT(groupCluster2.withInsurers::quotes::insurer) AS counted;

flattenedCount = FOREACH countOfInsurer GENERATE FLATTEN(group), counted;

rmf /user/cscarion/flattenedCount

STORE flattenedCount INTO 'flattenedCount' USING PigStorage('|');

flattenedCount = LOAD 'flattenedCount' USING PigStorage('|') AS (cluster, insurer, count:int);

commonInsurer = GROUP flattenedCount BY(cluster);

commonInsurerAggregate = FOREACH commonInsurer {
   elems = ORDER flattenedCount BY count DESC;
   one = LIMIT elems 1;
   GENERATE group, FLATTEN(one.insurer), FLATTEN(one.count);
 };

rmf /user/cscarion/commonInsurerAggregate

STORE commonInsurerAggregate into 'commonInsurerAggregate' using PigStorage('|');




