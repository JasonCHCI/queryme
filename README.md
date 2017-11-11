# Query Me
This project is a sql query program through command line

## Get started
### Prerequisites
Make sure you already install python version 2.7 and install pip
### Installing
run commands below on terminal
```
sudo pip install sqlparse
sudo pip install pandas
```
### Clone
go the directory you prefer and clone this project, similar to track1
```
$ git clone https://github.com/JasonCHCI/queryme.git
```
To update:
```
$ git checkout master
$ git pull
```
To contribute:
`follow track1`

To run script: <br />
```
$ main.py -i <\inputfile1> [-i <\inputfile2>] [-i <\inputfile3>]
```
<br />
When you see "Input a query or input exit:", please input a query statement or type 'exit' to exit:
<br />
```
$ SELECT * FROM students [WHERE id=1]
$ exit
```
<br />
Example: 
```
$ python main.py -i students.csv "SELECT * FROM students"
```
