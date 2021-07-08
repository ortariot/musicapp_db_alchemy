# Домашнее задание к лекции «Select-запросы, выборки из одной таблицы»

Задание выполнено на Python с использованием `sqlalchemy` без прямого использования SQL.

## Задание 1

### Тебуется выполнить несколько запросов для заполнения таблиц

Разработан класс для работы с базой данных musicapp podtgreesql, в котором реализован метод `load_albums_from_path()` позволяющая считать текстовые файлы с дискографией исполнителей и автоматически заполнить таблицы.

### пример заполнения таблицы

    vol_id = 1
    song_id = 1
    def load_song_vol(self, vol_id, song_id):
        song_vol_data = {'vol_id': vol_id,
                         'song_id': song_id
                         }
        connection = self.engine.connect()
        connection.execute(insert(self.songs_vol), song_vol_data)

эквивалентно:

    INSERT INTO songs_vol(vol_id, song_id) VALUES(NOW(), 1, 1);

## Задание 2

### Требуется выполнить запросы для получения данных из базы данных

1 название и год выхода альбомов, вышедших в 2018 году:

Реализован методом `get_album_by_year()` класса `MuscAppDb`

SQL:

    SELECT name, year FROM public."Albums"
        WHERE year = 2018

2 название и продолжительность самого длительного трека:

Реализован методом `get_long_playng_track()` класса `MuscAppDb`

SQL:

    SELECT name FROM public."Songs"
       ORDER BY playing_time DESC
    LIMIT 1;

3 название треков, продолжительность которых не менее 3,5 минуты:

Реализован методом `get_track_by_playng_time()` класса `MuscAppDb`

SQL:

    SELECT name FROM public."Songs"
       WHERE playing_time > 210


4 названия сборников, вышедших в период с 2018 по 2020 год включительно

Реализован методом `get_vols_by_years()` класса `MuscAppDb`

SQL:

    SELECT name FROM public."Vols"
        WHERE year BETWEEN 2018 AND 2020;

5 исполнители, чье имя состоит из 1 слова:

Реализован методом `get_one_word_name_aerists()` класса `MuscAppDb`

SQL:

    SELECT name FROM public."Artists"
        WHERE name NOT LIKE '% %';

6 название треков, которые содержат слово "мой"/"my"

Реализован методом `get_trek_by_word()` класса `MuscAppDb` , однако в загруженнйо бибилиотеке подходящих треков не нашлось, поэтому в примере испытан на других запросах.

SQL:

    SELECT name FROM public."Songs"
        WHERE name LIKE '%мой%' OR name LIKE '%my%';
