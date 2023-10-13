def get_connectionString():
    user = '' 
    password = ''
    host = '' 
    port = ''
    database = '' 
    return "postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database)
    