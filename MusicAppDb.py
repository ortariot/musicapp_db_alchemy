from sqlalchemy import create_engine, MetaData, Table, Integer, String, \
    Column, ForeignKey, CheckConstraint, PrimaryKeyConstraint, select, func
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from random import randint

from sqlalchemy.sql.expression import insert


class MuscAppDb():
    def __init__(self, su_name, su_pass, usr_name, usr_pass,
                 db_allready_exist=True):
        if db_allready_exist is False:
            self._db_create(su_name, su_pass, 'muscapp',
                            usr_name, usr_pass)

        db = f"postgresql://{usr_name}:{usr_pass}@localhost:5432/{'muscapp'}"
        self.engine = create_engine(db)
        self.metadata = MetaData()
        self._table_create()

        if db_allready_exist is False:
            self.metadata.create_all(self.engine)

    def _db_create(self, su_name, su_pass, db_name, u_name, u_pass):
        connection = psycopg2.connect(user=su_name, password=su_pass)
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()
        cursor.execute(f'CREATE DATABASE {db_name}')
        cursor.execute(f"CREATE USER {u_name} WITH PASSWORD '{u_pass}'")
        cursor.execute(f"ALTER DATABASE {db_name} OWNER TO {u_name}")
        cursor.close()
        connection.close()

    def _table_create(self):
        self.genre = Table("Genre", self.metadata,
                           Column('id', Integer(), primary_key=True),
                           Column('name', String(255), nullable=False)
                           )

        self.artists = Table("Artists", self.metadata,
                             Column('id', Integer(), primary_key=True),
                             Column('name', String(255), nullable=False),
                             Column('country', String(255))
                             )

        self.albums = Table("Albums", self.metadata,
                            Column('id', Integer(), primary_key=True),
                            Column('name', String(255), nullable=False),
                            Column('year', Integer()),
                            CheckConstraint('year > 1887', name="year_check")
                            )

        self.songs = Table("Songs", self.metadata,
                           Column('id', Integer(), primary_key=True),
                           Column('name', String(255), nullable=False),
                           Column('playing_time', Integer(), nullable=False),
                           Column('album_id', ForeignKey('Albums.id'),
                                  nullable=False)
                           )

        self.vols = Table("Vols", self.metadata,
                          Column('id', Integer(), primary_key=True),
                          Column('name', String(255), nullable=False),
                          Column('year', Integer()),
                          CheckConstraint('year > 1887', name="year_check")
                          )

        self.genre_artists = Table("GenreArtists", self.metadata,
                                   Column('artist_id',
                                          ForeignKey('Artists.id')
                                          ),
                                   Column('genre_id', ForeignKey('Genre.id')),
                                   PrimaryKeyConstraint('artist_id',
                                                        'genre_id',
                                                        name='pk_genreartists'
                                                        )
                                   )

        self.artists_albums = Table("ArtistAlbum", self.metadata,
                                    Column('artist_id',
                                           ForeignKey('Artists.id')
                                           ),
                                    Column('album_id',
                                           ForeignKey('Albums.id')),
                                    PrimaryKeyConstraint('artist_id',
                                                         'album_id',
                                                         name='pk_artistsalbum'
                                                         )
                                    )

        self.songs_vol = Table("SongsVol", self.metadata,
                               Column('vol_id', ForeignKey('Vols.id')),
                               Column('song_id', ForeignKey('Songs.id')),
                               PrimaryKeyConstraint('vol_id', 'song_id',
                                                    name='pk_songsvol'
                                                    )
                               )

    def ins_to_table(self, table, data):
        connection = self.engine.connect()
        connection.execute(insert(table), data)
        filter = func.max(table.c.id)
        sel_last_id = select(filter)
        last_id = connection.execute(sel_last_id).fetchone()[0]
        connection.close()
        return last_id

    def load_genre(self, genre_name):
        connection = self.engine.connect()
        sel_name = select(self.genre).where(self.genre.c.name == genre_name)
        out = connection.execute(sel_name).fetchone()
        connection.close()
        if out:
            last_id = out.id
        else:
            genre_data = {'name': genre_name}
            last_id = self.ins_to_table(self.genre, genre_data)
        return last_id

    def load_artist(self, artist_name, country):
        artist_data = {'name': artist_name,
                       'country': country
                       }
        last_id = self.ins_to_table(self.artists, artist_data)
        return last_id

    def load_album(self, album_name, album_year):
        album_data = {'name': album_name,
                      'year': album_year
                      }
        last_id = self.ins_to_table(self.albums, album_data)
        return last_id

    def load_vol(self, vol_name, vol_year):
        vol_data = {'name': vol_name,
                    'year': vol_year
                    }
        last_id = self.ins_to_table(self.vols, vol_data)
        return last_id

    def load_song(self, song_name, song_time, album_id):
        song_data = {'name': song_name,
                     'playing_time': song_time,
                     'album_id': album_id
                     }
        last_id = self.ins_to_table(self.songs, song_data)
        return last_id

    def load_genre_artist(self, artist_id, genre_id):
        sel_artist = select(self.genre_artists
                            ).where(self.genre_artists.c.artist_id
                                    == artist_id)
        connection = self.engine.connect()
        out = connection.execute(sel_artist).fetchone()
        if not out or (out and out.genre_id != genre_id):
            genre_artist_data = {'artist_id': artist_id,
                                 'genre_id': genre_id
                                 }
            connection.execute(insert(self.genre_artists), genre_artist_data)

    def load_artist_album(self, artist_id, album_id):
        artist_album_data = {'artist_id': artist_id,
                             'album_id': album_id
                             }
        connection = self.engine.connect()
        connection.execute(insert(self.artists_albums), artist_album_data)

    def load_song_vol(self, vol_id, song_id):
        song_vol_data = {'vol_id': vol_id,
                         'song_id': song_id
                         }
        connection = self.engine.connect()
        connection.execute(insert(self.songs_vol), song_vol_data)

    def load_albums_from_path(self, data_path='data/'):
        file_list = os.listdir(path=data_path)
        txt_file_list = [file for file in
                         file_list if file[len(file) - 4:] == '.txt']

        for file in txt_file_list:
            with open(data_path + file, encoding='utf-8') as f:
                album_id = 0
                genre_id = 0
                artist_id = 0
                artist_name = f.readline().strip()
                country = f.readline().strip()
                artist_id = self.load_artist(artist_name, country)
                for line in f:
                    if line != '\n':
                        if '«' in line:
                            song_str = line.strip()
                            llim = song_str.index('«') + 1
                            rlim = song_str.index('»')
                            song_name = song_str[llim:rlim]
                            song_time = song_str[rlim+1:]
                            song_min = int(song_time[:song_time.index(':')])*60
                            song_sec = int(song_time[song_time.index(':')
                                           + 1:])
                            song_time_dig = song_min + song_sec
                            self.load_song(song_name, song_time_dig, album_id)
                        elif line.strip()[:4].isdigit():
                            album_id = self.load_album(line.strip()[6:],
                                                       line.strip()[:4])
                            self.load_artist_album(artist_id, album_id)
                        elif '«' not in line:
                            genre_id = self.load_genre(line.strip())
                            self.load_genre_artist(artist_id, genre_id)

    def generate_vol(self, name, country, year):
        vol_id = self.load_vol(name, year)
        sel = select([self.songs.c.id, self.songs.c.name]
                     ).select_from(self.songs.join(self.albums).
                                   join(self.artists_albums).join(self.artists)
                                   ).where(self.albums.c.year < year,
                                           self.artists.c.country == country
                                           )
        connection = self.engine.connect()
        out = connection.execute(sel)
        connection.close
        all_track = out.fetchall()
        track_list = []
        i = 0
        while(len(track_list) != 10):
            i += 1
            if i > 100:
                break
            track = all_track[randint(1, len(all_track)-1)]
            if track not in track_list:
                track_list.append(track)

        for track in track_list:
            self.load_song_vol(vol_id, track.id)

    def get_genre(self):
        sel = select([self.genre.c.name])
        connection = self.engine.connect()
        out = connection.execute(sel)
        connection.close()
        genre_list = out.fetchall()
        return genre_list

    def get_country(self):
        sel = select([self.artists.c.country])
        connection = self.engine.connect()
        out = connection.execute(sel)
        connection.close
        country_list = set(out.fetchall())
        return list(country_list)

    def get_album_by_year(self, year):
        sel = select([self.albums.c.name, self.albums.c.year]
                     ).where(self.albums.c.year == year)
        connection = self.engine.connect()
        out = connection.execute(sel)
        connection.close
        return out.fetchall()

    def get_long_playng_track(self):
        sel = select(self.songs.c.name, self.songs.c.playing_time
                     ).where(self.songs.c.playing_time.in_
                             (select(func.max(self.songs.c.playing_time))))
        connection = self.engine.connect()
        out = connection.execute(sel)
        connection.close
        return out.fetchone()

    def get_track_by_playng_time(self, p_time):
        minutes = int(p_time[:p_time.index(":")])*60
        seconds = int(p_time[p_time.index(":") + 1:])
        p_time_i = minutes + seconds
        sel = select(self.songs.c.name
                     ).where(self.songs.c.playing_time > p_time_i)
        connection = self.engine.connect()
        out = connection.execute(sel)
        connection.close()
        return out.fetchall()

    def get_vols_by_years(self, start_year, finish_year):
        sel = select([self.vols.c.name]
                     ).where(self.vols.c.year.between(start_year, finish_year)
                     )
        connection = self.engine.connect()
        out = connection.execute(sel)
        connection.close
        return out.fetchall()

    def get_one_word_name_aerists(self):
        sel = select([self.artists.c.name]
                     ).where(self.artists.c.name.notlike('% %'))
        connection = self.engine.connect()
        out = connection.execute(sel)
        connection.close
        return out.fetchall()

    def get_trek_by_word(self, word):
        sel = select([self.songs.c.name]
                     ).where(self.songs.c.name.like(f'%{word}%'))
        connection = self.engine.connect()
        out = connection.execute(sel)
        connection.close()
        return out.fetchall()
