from sys import path
from sqlalchemy import create_engine, MetaData, Table, Integer, String, \
    Column, ForeignKey, CheckConstraint, PrimaryKeyConstraint
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os


su_name = 'postgres'
su_pass = ''
db_name = 'musicapp'
u_name = 'test'
u_pass = 'testpassword'


def db_create(su_name, su_pass, db_name, u_name, u_pass):
    connection = psycopg2.connect(user=su_name, password=su_pass)
    connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = connection.cursor()
    cursor.execute(f'CREATE DATABASE {db_name}')
    cursor.execute(f"CREATE USER {u_name} WITH PASSWORD '{u_pass}'")
    cursor.execute(f"ALTER DATABASE {db_name} OWNER TO {u_name}")
    cursor.close()
    connection.close()


def struct_create():
    db = f"postgresql://{u_name}:{u_pass}@localhost:5432/{db_name}"
    engine = create_engine(db)
    metadata = MetaData()

    Table("Genre", metadata,
          Column('id', Integer(), primary_key=True),
          Column('name', String(255), nullable=False)
          )

    Table("Artists", metadata,
          Column('id', Integer(), primary_key=True),
          Column('name', String(255), nullable=False),
          Column('country', String(255))
          )

    Table("Albums", metadata,
          Column('id', Integer(), primary_key=True),
          Column('name', String(255), nullable=False),
          Column('year', Integer()),
          CheckConstraint('year > 1887', name="year_check")
          )

    Table("Songs", metadata,
          Column('id', Integer(), primary_key=True),
          Column('name', String(255), nullable=False),
          Column('playing_time', Integer(), nullable=False),
          Column('album_id', ForeignKey('Albums.id'), nullable=False)
          )

    Table("Vol", metadata,
          Column('id', Integer(), primary_key=True),
          Column('name', String(255), nullable=False),
          Column('year', Integer()),
          CheckConstraint('year > 1887', name="year_check")
          )

    Table("GenreArtists", metadata,
          Column('artist_id', ForeignKey('Artists.id')),
          Column('genre_id', ForeignKey('Genre.id')),
          PrimaryKeyConstraint('artist_id', 'genre_id', name='pk_genreartists')
          )

    Table("ArtistAlbum", metadata,
          Column('artist_id', ForeignKey('Artists.id')),
          Column('album_id', ForeignKey('Genre.id')),
          PrimaryKeyConstraint('artist_id', 'album_id', name='pk_artistsalbum')
          )

    Table("SongsVol", metadata,
          Column('vol_id', ForeignKey('Artists.id')),
          Column('song_id', ForeignKey('Genre.id')),
          PrimaryKeyConstraint('vol_id', 'song_id', name='pk_songsvol')
          )
    # connection = engine.connect()
    metadata.create_all(engine)


def load_data():
    file_list = os.listdir(path='data/')
    txt_file_list = [file for file in
                     file_list if file[len(file) - 4:] == '.txt']
    tmp = []
    for file in txt_file_list:
        with open('data/' + file, encoding='utf-8') as f:
            tmp.append(f.readlines())
    print(tmp)








if __name__ == '__main__':
    # db_create(su_name, su_pass, db_name, u_name, u_pass)
    # struct_create()
    load_data()

