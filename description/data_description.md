# Zestaw 13 – football-matches

Dane sztuczne wygenerowane z użyciem biblioteki DataFaker (https://www.datafaker.net/)  
Uwaga! Dane pobieramy z miejsca wskazanego w ramach Twojego kursu

## Dwa zbiory danych

### `datasource1` – informacje o meczach piłkarskich (matches)

Dane mają format CSV, pliki posiadają nagłówek.  

Pola w pliku:

0. `match_id` – unikalny identyfikator meczu (UUID)  
1. `home_team_id` – identyfikator drużyny gospodarzy (`team_id` z pliku drużyn)  
2. `away_team_id` – identyfikator drużyny gości (`team_id` z pliku drużyn)  
3. `home_score` – liczba bramek strzelonych przez gospodarzy  
4. `away_score` – liczba bramek strzelonych przez gości  
5. `date` – data i godzina meczu (`yyyy-MM-dd'T'HH:mm`)  
6. `attendance` – liczba widzów na meczu  

### `datasource4` – informacje o drużynach piłkarskich (teams)

Dane mają format CSV, pliki posiadają nagłówek.  

Pola w pliku:

0. `team_id` – unikalny identyfikator drużyny (UUID)  
1. `name` – nazwa drużyny  
2. `city` – miasto drużyny  
3. `league` – liga, w której drużyna występuje (`Premier League`, `La Liga`, `Bundesliga`, `Serie A`, `Ligue 1`)  
4. `coach` – imię i nazwisko trenera drużyny  
