from MusicAppDb import MuscAppDb
from random import randint


if __name__ == '__main__':
    su_name = 'postgres'
    su_pass = '1una}{od'
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





