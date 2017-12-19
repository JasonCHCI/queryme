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

Run preprocessing first: 
```
python2 storeAsFeather.py
```
Then the main program:
```
$ python2 mysql.py
```

When you see "Input a query or input exit:", please input a query statement or type 'exit' to exit: <br />
```
$ SELECT * FROM movies.csv WHERE year=2015
$ exit
```
Examples: <br />
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

Queries from demo:
```
SELECT R.review_id, R.funny, R.useful FROM review-1m.csv R WHERE R.funny >= 20 AND R.useful > 30
```
```
SELECT B.name, B.city, B.state FROM business.csv B WHERE B.city = 'Champaign' AND B.state = 'IL'
```
```
SELECT R1.user_id, R2.user_id, R1.stars, R2.stars FROM review-1m.csv R1, review-1m.csv R2 WHERE R1.stars = 5 AND R2.stars = 1 AND R1.useful > 50 AND R2.useful > 50 AND R1.business_id = R2.business_id
```
```
SELECT B.name, R1.user_id, R2.user_id FROM business.csv B, review-1m.csv R1, review-1m.csv R2 WHERE B.business_id = R1.business_id AND R1.business_id = R2.business_id AND R1.stars = 5 AND R2.stars = 1 AND R1.useful > 50 AND R2.useful > 50
```

