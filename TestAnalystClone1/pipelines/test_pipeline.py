Schedule = Schedule(cron = "* 0 2 * * * *", timezone = "GMT", emails = ["email@gmail.com"], enabled = False)

with DAG(Schedule = Schedule):
    S3Source_0 = SourceTask(
        task_id = "S3Source_0", 
        component = "OrchestrationSource", 
        kind = "S3Source", 
        connector = Connection(kind = "s3", id = "s3"), 
        format = CSVFormat(header = True, separator = ","), 
        filePath = "/annual-enterprise-survey-2020-financial-year-provisional-csv.csv"
    )
    OrchestrationSource_1 = SourceTask(
        task_id = "OrchestrationSource_1", 
        component = "OrchestrationSource", 
        kind = "SharepointSource", 
        connector = Connection(kind = "sharepoint"), 
        format = CSVFormat(header = True, separator = ",")
    )
