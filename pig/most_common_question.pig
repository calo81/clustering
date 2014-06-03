cluster = LOAD '/user/cscarion/individual-clusters/part-r-00000' AS(clusterId, customerId);
customers = LOAD '/user/cscarion/aggregated_customers_text_questions' using PigStorage('|') AS(id, vertical, trade, turnover, claims,rfq_id, question);

groupCluster2 = JOIN customers BY id, cluster BY customerId;

grouped2 = GROUP groupCluster2 BY (cluster::clusterId, customers::question);

countOfQuestions = FOREACH grouped2 GENERATE group, COUNT(groupCluster2.customers::question) AS counted;

flattenedCount = FOREACH countOfQuestions GENERATE FLATTEN(group), counted;

rmf /user/cscarion/flattenedCountOfQuestions

STORE flattenedCount INTO 'flattenedCountOfQuestions' USING PigStorage('|');

flattenedCount = LOAD 'flattenedCountOfQuestions' USING PigStorage('|') AS (cluster, question, count:int);

commonQuestion = GROUP flattenedCount BY(cluster);

commonQuestionAggregate = FOREACH commonQuestion {
   elems = ORDER flattenedCount BY count DESC;
   one = LIMIT elems 1;
   GENERATE group, FLATTEN(one.question), FLATTEN(one.count);
 };

rmf /user/cscarion/commonQuestionAggregate

STORE commonQuestionAggregate into 'commonQuestionAggregate' using PigStorage('|');
