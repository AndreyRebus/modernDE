from dagster import job, op

@op
def hello():
    return "Привет, Dagster!"

@job
def hello_job():
    hello()
