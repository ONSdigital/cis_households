from cishouseholds.filter import filter_all_not_null

# test
columns = "sample string, date_tested string, cq_value double"

data = [('ONS10948406', '2021-08-13 07:09:41', 12.782375), # Example 1:
        ('ONS10948406', '2021-08-13 07:09:41', 12.782370), # smaller than 10^-5 - DELETE
        ('ONS10948406', '2021-08-13 07:09:41', 12.783275), # larger than 10^-5 - KEEP
        ('ONS10948406', '2021-08-13 07:09:41', 12.983275), # larger than 10^-5 - KEEP
        
        ('ONS74697669', '2021-08-17 07:09:41', 200.782375), # Example 2:
        ('ONS74697669', '2021-08-17 07:09:41', 200.782370), # smaller than 10^-5 - DELETE
        ('ONS74697669', '2021-08-17 07:09:41', 200.783275), # larger than 10^-5 - KEEP
]

df = spark.createDataFrame(data = data, schema = columns)

df = filter_all_not_null(df, 'cq_value', 'date_tested')