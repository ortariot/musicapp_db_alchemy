import sqlalchemy

db = 'postgresql://postgres:passs@localhost:5432/postgres'
engine = sqlalchemy.create_engine(db)
connection = engine.connect()
