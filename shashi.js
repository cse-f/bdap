employee_rdd = df.rdd

filtered_rdd = employee_rdd.filter(lambda row: row['SALARY'] > 10000)

dept_salary_rdd = filtered_rdd.map(lambda row: (row['DEPARTMENT_ID'], row['SALARY']))

sum_count_rdd = dept_salary_rdd.aggregateByKey(
    (0, 0),
    lambda acc, salary: (acc[0] + salary, acc[1] + 1),
    lambda acc1, acc2: (acc1[0] + acc2[0], acc1[1] + acc2[1])
)

avg_salary_rdd = sum_count_rdd.mapValues(lambda sum_count: sum_count[0] / sum_count[1])



///////////


agent1.sources = src1
agent1.sinks = sink1
agent1.channels = ch1

agent1.sources.src1.type = netcat
agent1.sources.src1.bind = localhost
agent1.sources.src1.port = 44444

agent1.sinks.sink1.type = logger

agent1.channels.ch1.type = memory
agent1.channels.ch1.capacity = 1000
agent1.channels.ch1.transactionCapacity = 100

agent1.sources.src1.channels = ch1
agent1.sinks.sink1.channel = ch1

/////////


from pyspark.sql.functions import avg

avg_salary_by_dept = df.groupBy('DEPARTMENT_ID').agg(avg('SALARY').alias('Average_Salary'))
avg_salary_by_dept.show()
