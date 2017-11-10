 create table if not exists phone_stream(
     reporttime string,
     msisdn string,
     apmac string,
     acmac string,
     host string,
     sitetype string,
     uppacknum bigint,
     downpacknum bigint,
     uppayload bigint,
     downpayload bigint,
     httpstatus string)
     row format delimited fields terminated by '\t';

