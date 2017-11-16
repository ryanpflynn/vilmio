from config.py import *
import json
from datetime import date
from datetime import datetime
import pymysql
import pymysql.cursors
from flask import Flask, flash, redirect, render_template, request, session, abort

theatres = {'rialto':'3076','barnstormer':'7629','old-mill-playhouse':'10656'}

app = Flask(__name__)

def getTheatreDetails(theatre_name):
    c=5

def getMovieDetails(movie_id):
    c=5

def getAllShowingsForMovie(movie_id):
    c=5

def getAllDates():
    connection = pymysql.connect(host=dbhost,
                                user=dbuser,
                                password=dbpw,
                                db='movies',
                                cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "SELECT DISTINCT DATE(showings.date_time) FROM `showings`ORDER BY DATE(showings.date_time) ASC"
            cursor.execute(sql)
            dbResult = cursor.fetchall()

    finally:
        connection.close()

    dates_with_data = {}

    for availDates in dbResult:
        dates_with_data[availDates['DATE(showings.date_time)'].strftime("%A %B %d")]=availDates['DATE(showings.date_time)'].strftime("%Y-%m-%d")
        #print(str(dates_with_data))
    return(dates_with_data)

def getAllMoviesWithShowings():
    connection = pymysql.connect(host=dbhost,
                                user=dbuser,
                                password=dbpw,
                                db='movies',
                                cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "SELECT DISTINCT movies.name, movies.id FROM `showings` LEFT JOIN `movies` ON movies.id = showings.movie_id WHERE showings.date_time BETWEEN '2017-01-21 00:00:00' AND '2117-09-21 23:59:59' ORDER BY movies.name ASC"
            cursor.execute(sql)
            dbResult = cursor.fetchall()

    finally:
        connection.close()

    movie_titles = {}
    for movies in dbResult:
        if movies['name'] not in movies:
            movie_titles[movies['name']] = movies['id']
    #print(type(movie_titles))
    return(movie_titles)

def getTheaterShowingsForDate(theatre_name, date):
    if theatres.has_key(theatre_name.lower()):
        connection = pymysql.connect(host=dbhost,
                                    user=dbuser,
                                    password=dbpw,
                                    db='movies',
                                    cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "SELECT movies.name, movies.id, movies.genre, movies.rating, movies.running_time, movies.poster_url, movies.synopsis, showings.date_time FROM `showings` LEFT JOIN `movies` ON movies.id = showings.movie_id WHERE showings.theatre_id = \'"+theatres[theatre_name]+"\' AND showings.date_time BETWEEN \'"+date+" 00:00:00\' AND \'"+date+" 23:59:59\' ORDER BY movies.name, showings.date_time"
                cursor.execute(sql)
                dbResult = cursor.fetchall()

        finally:
            connection.close()
        #build new dictionary for results
        movie_titles = {}
        for movies in dbResult:
            if movies['name'] not in movies:
                movie_titles[movies['name']] = {}
                movie_titles[movies['name']]['showtimes'] = []
                movie_titles[movies['name']]['id'] = movies['id']
                movie_titles[movies['name']]['rating'] = movies['rating']
                movie_titles[movies['name']]['running_time'] = movies['running_time']
                movie_titles[movies['name']]['genre'] = movies['genre']
                movie_titles[movies['name']]['synopsis'] = movies['synopsis']
        for movies in dbResult:
            movie_titles[movies['name']]['showtimes'].append(movies['date_time'].strftime("%-I:%M %p"))
        return(json.dumps(movie_titles))
        #return str(dbResult)
    else:
        return json.dumps("Theatre Not Found")

@app.route("/")
def home():
    movielist2=getAllMoviesWithShowings()
    #print(type(movielist2))
    return render_template('index.html', dates=getAllDates(), movielist=movielist2)

@app.route("/dates/")
def dates():
    y=1

@app.route("/movies/")
def movies():
    return (getAllMoviesWithShowings())

@app.route("/movie/<string:movie_id>/")
def movieDetail(movie_id):
    return (getMovieDetails(movie_id))

@app.route("/theatres")
def theatre_root():
    return redirect('/', code=307)

@app.route("/theatres/<string:theatre_name>/")
def theatre(theatre_name):
    today = str(date.today())
    return (getTheaterShowingsForDate(theatre_name, today))

@app.route("/theatres/<string:theatre_name>/<string:desired_date>/")
def theatre_on_date(theatre_name,desired_date):
    #Check desired_date for the correct format YYYY-MM-DD
    try:
        datetime.strptime(desired_date, '%Y-%m-%d')
    except ValueError:
        raise ValueError("Incorrect data format, date should be YYYY-MM-DD")
    #Do we have showings for desired date?
    return (getTheaterShowingsForDate(theatre_name, desired_date))

if __name__ == "__main__":
    app.run()
