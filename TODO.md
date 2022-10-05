# TODO

This document contains a list of bulker problems. We are aware of this problems, and they need to be fixed before final release. Those
problems are split by different destinations

## Regular Redshift

 * Add load test for Redshift in streaming (aka autocommit) mode and make sure it could handle a decent load. If it couldn't handle a decent load, streaming 
mode for Redshift should be disabled of discouraged
 * Redshift supports [SORTKEY](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-sort-key.html). We need to introduce an option to defined 
sort key columnin the destination table. If the option is present, we should make sure that the either already created, or create it. Once we know sort key 
we should take advantage of it while doing deduplication by defining deduplication window which should be also a config param
 * 

## Serverless Redshift

We should test how bulker works with Serverless Redshift and make sure it won't cost a lot. As a result we should either discourage using Serverlress Redshift,
or suggest the cost optimization strategy. Bulker as is could cost a lot.

