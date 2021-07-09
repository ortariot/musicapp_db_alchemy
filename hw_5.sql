--1 количество исполнителей в каждом жанре:
SELECT genre_id, COUNT(artist_id) FROM public."GenreArtists"
GROUP BY genre_id;

--2 количество треков, вошедших в альбомы 2019-2020 годов:
SELECT COUNT(id) FROM public."Songs"
WHERE album_id IN (SELECT id FROM public."Albums" 
				   WHERE year BETWEEN 2019 AND 2020
				   );


--3 средняя продолжительность треков по каждому альбому:
SELECT album_id, AVG(playing_time) FROM public."Songs"
GROUP BY album_id
ORDER BY album_id;

--4 все исполнители, которые не выпустили альбомы в 2020 году:
SELECT name FROM public."Artists"
WHERE id NOT IN (SELECT artist_id FROM public."ArtistAlbum"
				 WHERE album_id IN (SELECT id FROM public."Albums"
								     WHERE year = 2020
									)
				 );

--5 названия сборников, в которых присутствует конкретный исполнитель (выберите сами):
SELECT "Vols".name FROM public."Vols"
JOIN public."SongsVol" ON public."Vols".id = public."SongsVol".vol_id
JOIN public."Songs" ON public."SongsVol".song_id = public."Songs".id
JOIN public."Albums" ON public."Songs".album_id = public."Albums".id
JOIN public."ArtistAlbum" ON public."Albums".id = public."ArtistAlbum".album_id
JOIN public."Artists" ON public."ArtistAlbum".artist_id = public."Artists".id
WHERE  public."Artists".name = 'Moonspell'
GROUP BY "Vols".name;

--6 название альбомов, в которых присутствуют исполнители более 1 жанра;
SELECT "Albums".name FROM public."Albums"
JOIN public."ArtistAlbum" ON public."Albums".id = public."ArtistAlbum".album_id
JOIN public."Artists" ON public."ArtistAlbum".artist_id = public."Artists".id
JOIN public."GenreArtists" ON public."Artists".id = public."GenreArtists".artist_id
GROUP BY "Albums".name 
HAVING COUNT(genre_id) > 1;

-- 7 наименование треков, которые не входят в сборники;
SELECT name FROM public."Songs"
WHERE id NOT IN (SELECT song_id FROM public."SongsVol");

--8 исполнителя(-ей), написавшего самый короткий по продолжительности трек 
--(теоретически таких треков может быть несколько)
SELECT "Artists".name FROM public."Artists"
	WHERE "Artists".id IN(SELECT public."ArtistAlbum".artist_id FROM public."ArtistAlbum"
						  JOIN public."Albums" ON public."ArtistAlbum".album_id = public."Albums".id
						  JOIN public."Songs" ON public."Albums".id = public."Songs".album_id
						  WHERE playing_time = ( SELECT MIN(playing_time) FROM public."Songs")
						  );
-- 9 название альбомов, содержащих наименьшее количество треков.						  
SELECT public."Albums".name FROM public."Albums"
WHERE "Albums".id IN (SELECT album_id FROM public."Songs"
					  GROUP BY album_id
					  HAVING COUNT("Songs".id) = 
					       (SELECT COUNT("Songs".id) FROM public."Songs"
							GROUP BY album_id
							ORDER BY COUNT("Songs".id)
							LIMIT 1
							)
					 );
		