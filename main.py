from MusicAppDb import MuscAppDb
from random import randint
import sqlalchemy


def select_request_by_hw5(u_name, u_pass):
    db = f"postgresql://{u_name}:{u_pass}@localhost:5432/{'muscapp'}"
    engine = sqlalchemy.create_engine(db)
    connection = engine.connect()

    # 1 количество исполнителей в каждом жанре:
    sel1 = connection.execute("""
    SELECT genre_id, COUNT(artist_id) FROM public."GenreArtists"
    GROUP BY genre_id;"""
                              )
    print(sel1.fetchall())

    # 2 количество треков, вошедших в альбомы 2019-2020 годов:
    sel2 = connection.execute("""
    SELECT COUNT(id) FROM public."Songs"
    WHERE album_id IN (SELECT id FROM public."Albums"
    WHERE year BETWEEN 2019 AND 2020
    );
    """
                              )
    print(sel2.fetchall())

    # 3 средняя продолжительность треков по каждому альбому:
    sel3 = connection.execute("""
    SELECT album_id, AVG(playing_time) FROM public."Songs"
    GROUP BY album_id
    ORDER BY album_id;
    """
                              )
    print(sel3.fetchall())

    # 4 все исполнители, которые не выпустили альбомы в 2020 году:
    sel4 = connection.execute("""
    SELECT name FROM public."Artists"
    WHERE id NOT IN (SELECT artist_id FROM public."ArtistAlbum"
    WHERE album_id IN (SELECT id FROM public."Albums"
    WHERE year = 2020));
    """
                              )
    print(sel4.fetchall())

    # 5 названия сборников, в которых присутствует
    # конкретный исполнитель (выберите сами):
    sel5 = connection.execute("""
    SELECT "Vols".name FROM public."Vols"
    JOIN public."SongsVol" ON public."Vols".id = public."SongsVol".vol_id
    JOIN public."Songs" ON public."SongsVol".song_id = public."Songs".id
    JOIN public."Albums" ON public."Songs".album_id = public."Albums".id
    JOIN public."ArtistAlbum" ON public."Albums".id =
    public."ArtistAlbum".album_id
    JOIN public."Artists" ON public."ArtistAlbum".artist_id =
    public."Artists".id
    WHERE  public."Artists".name = 'Moonspell'
    GROUP BY "Vols".name;
    """
                              )
    print(sel5.fetchall())

    # 6 название альбомов, в которых присутствуют исполнители более 1 жанра;
    sel6 = connection.execute("""
    SELECT "Albums".name FROM public."Albums"
    JOIN public."ArtistAlbum" ON public."Albums".id =
     public."ArtistAlbum".album_id
    JOIN public."Artists" ON public."ArtistAlbum".artist_id =
     public."Artists".id
    JOIN public."GenreArtists" ON public."Artists".id =
     public."GenreArtists".artist_id
    GROUP BY "Albums".name
    HAVING COUNT(genre_id) > 1;
    """
                              )
    print(sel6.fetchall())

    # 7 наименование треков, которые не входят в сборники;
    sel7 = connection.execute("""
    SELECT name FROM public."Songs"
    WHERE id NOT IN (SELECT song_id FROM public."SongsVol");
    """
                              )
    print(sel7.fetchall())

    # 8 исполнителя(-ей), написавшего самый короткий по продолжительности трек
    # (теоретически таких треков может быть несколько)
    sel8 = connection.execute("""
    SELECT "Artists".name FROM public."Artists"
    WHERE "Artists".id IN
    (SELECT public."ArtistAlbum".artist_id FROM public."ArtistAlbum"
    JOIN public."Albums" ON public."ArtistAlbum".album_id = public."Albums".id
    JOIN public."Songs" ON public."Albums".id = public."Songs".album_id
    WHERE playing_time = ( SELECT MIN(playing_time) FROM public."Songs")
    );
    """
                              )
    print(sel8.fetchall())

    # 9 название альбомов, содержащих наименьшее количество треков
    sel9 = connection.execute("""
    SELECT public."Albums".name FROM public."Albums"
    WHERE "Albums".id IN (SELECT album_id FROM public."Songs"
    GROUP BY album_id
    HAVING COUNT("Songs".id) =
    (SELECT COUNT("Songs".id) FROM public."Songs"
    GROUP BY album_id
    ORDER BY COUNT("Songs".id)
    LIMIT 1)
    );
    """
                              )
    print(sel9.fetchall())


if __name__ == '__main__':
    su_name = 'postgres'
    su_pass = ''
    u_name = 'test'
    u_pass = 'testpassword'
    db = MuscAppDb(su_name, su_pass, u_name, u_pass, db_allready_exist=False)
    db.load_albums_from_path()

    country_list = db.get_country()
    pretentious = ['Th best', "The gold", "The Monsters", "The legend"]

    for i in range(12):
        year = 2010 + i
        country = country_list[randint(0, len(country_list) - 1)][0]
        vol_name = pretentious[randint(0, len(pretentious)-1)
                               ] + ' traks of ' + country + " " + str(year)
        db.generate_vol(vol_name, country, year)

    print("название и год выхода альбомов, вышедших в 2018 году:")
    print(db.get_album_by_year(2018))

    print("название и продолжительность самого длительного трека:")
    long_playng_track = db.get_long_playng_track()
    long_playng_track_name = long_playng_track[0]
    long_playng_track_p_time = str(long_playng_track[1] // 60
                                   ) + ':' + str(long_playng_track[1] % 60)
    print(long_playng_track_name, long_playng_track_p_time)

    print("название треков, продолжительность которых не менее 3,5 минуты:")
    print(db.get_track_by_playng_time('3:30'))
    print("названия сборников, вышедших в"
          "период с 2018 по 2020 год включительно:")
    print(db.get_vols_by_years(2018, 2020))
    print("исполнители, чье имя состоит из 1 слова:")
    print(db.get_one_word_name_aerists())
    print('название треков, которые содержат слово "Время"/"Love"')
    print(db.get_trek_by_word("Love"))
    print(db.get_trek_by_word("Время"))

    # Домашнее задание 5:
    select_request_by_hw5(u_name, u_pass)
