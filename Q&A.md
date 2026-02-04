1. **We run scrapes continuously, both on the same websites as data changes over time and on new websites that we find interesting. How would you monitor the activity of the scrapers to make sure they were functioning and functioning correctly?**
    - *Alberto mentioned that you are using Prefect as the orchestration tool. I could setup Prefect to various metrics such as retried, failed and completed tasks.*
    - *I could also setup Prefect to send alerts when certain metrics exceed certain thresholds.*
    - *Setup logging to keep an eye on HTTP status codes and response times.*
    - *Monitor resource utilization and performance metrics to ensure the underlying infrastructure is healthy.*
    - *Logs and traces could be redirected to a centralized observability platform.*
    - *Implement data quality checks such as records scraped vs expected, missing data, and data consistency.*


2. **We join data from lots of sources and this can lead to sparsity in the data, often it’s a case of identifying when we are missing data and differentiating that from when data simply isn’t available. How could you determine missing data in a scalable way?**
    - *Formulate the definition of missing data.*
    - *Categorize missing values as truly missing, expected to be missing, implicitly missing.*
    - *Utilize a data quality framework such as Great Expectations.*
    - *Implement conditional validation to take action based on the type of missing data.*
    - *Generating completeness reports per data source by processing the entire dataset after the ETL pipeline completes using a low cost solution by sacrificing speed for cost.*


3. **We release data on a weekly cadence, as time goes on we query more data and it can take longer to scrape and process the data we need. How would you scale the system to do more work within a shorter period of time?**
    - *Parallelize the scraping and processing tasks using multiple threads or processes.*
    - *Distribute the workload across multiple nodes when using Spark.*
    - *Utilize connection pooling for database connections, use async inserts for ClickHouse.*
    - *If API responses are larger than memory of available on the worker nodes in Prefect, stream the data to Object Storage, writing in micro-batches.*


4. **A recent change to the codebase has caused a feature to begin failing, the failure has made it’s way to production and needs to be resolved. What would you do to get the system back on track and reduce these sorts of incidents happening in future?**
    - *First step would be to alert the stakeholders and rollback to an earlier version.*
    - *Engage in an on-call to implement a hotfix after a peer-review if rollback is not feasible.*
    - *Conduct a RCA using logs, trace and metrics.*
    - *Implement a monitoring system to detect and alert on potential issues before they reach production.*
    - *Implement canary deployments to test new features.*
    - *Add feature flags and*
    - *Improve integration and end-to-end testing.*
