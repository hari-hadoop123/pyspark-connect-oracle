db_reader=spark.read.format('jdbc').
option('driver','oracle.jdbc.OracleDriver').
option('url','jdbc:oracle:thin:@test-environ.com:8889/customer').
option('user','mytestuser').
option('password','mytestpwd')


temp_df=db_reader.option('mytable','select * from test.testtable limit 1000').load()

temp_df.createOrReplaceTempView('TempTable')

spark.sql('select * from TempTable').show()