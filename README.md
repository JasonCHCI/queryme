# Query Me
This project is a sql query program through command line

### Prerequisites
Make sure you already install python version 2.7 and install pip

### Installing
run commands below on terminal
```
sudo pip install sqlparse
sudo pip install pandas
```
### Running the project

To run script:
```
$ python2 mysql.py
```

When you see "Input a query or input exit:", please input a query statement or type 'exit' to exit: <br />
```
$ SELECT * FROM movies.csv WHERE year=2015
$ exit<br />
```
Example: <br />
```
$ SELECT movie_title, title_year, imdb_score FROM movies.csv WHERE movie_title LIKE '%Kevin%' AND imdb_score > 7
```
```
$ SELECT A1.Year, A1.Film, A1.Award, A1.Name, A2.Award, A2.Name FROM oscars.csv A1, oscars.csv A2 WHERE A1.Film = A2.Film AND A1.Film <> '' AND A1.Winner = 1 AND A2.Winner=1 AND A1.Award > A2.Award AND A1.Year > '2010'
```
```
$ SELECT title_year, movie_title, Award, imdb_score, movie_facebook_likes FROM movies.csv M, oscars.csv A WHERE M.movie_title = A.Film AND A.Winner = 1 AND (M.imdb_score < 6 OR M.movie_facebook_likes < 10000)
```
```
$ SELECT M.movie_title, M.title_year, M.imdb_score, A1.Name, A1.Award, A2.Name, A2.Award FROM movies.csv M, oscars.csv A1, oscars.csv A2 WHERE M.movie_title = A1.Film AND M.movie_title = A2.Film AND A1.Award = 'Actor' AND A2.Award = 'Actress'
```

