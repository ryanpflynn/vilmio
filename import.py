#!/usr/bin/python3

from config import *
import requests
import json
import datetime
from datetime import date
from datetime import datetime
import pymysql
import pymysql.cursors

zipcode = '32162'
radius = '10'
numDays = '7'
today = date.today();
startDate = date.today();

def getMovieShowings(startDate, zipcode, radius, api_key):
    request_url = "http://data.tmsapi.com/v1.1/movies/showings"
    query_string = {'numDays':numDays,'startDate':startDate,'zip':'32162','radius':'10','units':'mi','imageSize':'Sm','imageText':'true','api_key':'mkxfn7du4vtvtck43v45vmfe'}
    r = requests.get(request_url, params=query_string)
    r.raise_for_status()
    if r.text == "":
        return None
    else:
        return r.json()

def addNewMovies(results):
    #Check to see if movie is in the database
    for index, item in enumerate(results):
        rootId = results[index]['rootId']
        movieName = results[index]['title'];
        if 'releaseDate' in results[index]:
            releaseDate = results[index]['releaseDate'];
            if len(releaseDate) == 4:
                releaseDate = releaseDate + "-01-01"
        else:
            releaseDate = '0000-01-01'
        if 'ratings' in results[index]:
            rating = results[index]['ratings'][0]['code'];
        else:
            rating = 'NR'
        runTime = str(results[index]['runTime'])
        runTime = runTime[3:100]
        runTime = runTime.lower()
        if 'releaseDate' in results[index]:
            genre = results[index]['genres'][0];
        else:
            genre = 'Not Available'
        description = results[index]['shortDescription'];
        imgUrl = 'http://tmsimg.com/'+results[index]['preferredImage']['uri'];

        if doesMovieNameExist(movieName) == 0 and doesMovieIdExist(rootId) == 0:
            addNewMovie(rootId, movieName, releaseDate, rating, runTime, genre, description, imgUrl)
        elif doesMovieNameExist(movieName) == 0 and doesMovieIdExist(rootId) == 1:
            print("A movie with the ID \'" + rootId+"\' is already in the database.")
            newId = rootId + "3"
            print("Modifying the ID from " +rootId+ " to " +newId)
            addNewMovie(newId, movieName, releaseDate, rating, runTime, genre, description, imgUrl)


def doesMovieIdExist(movieId):
    movieExists = 0
    connection = pymysql.connect(host=dbhost,
                                user=dbuser,
                                password=dbpw,
                                db='movies',
                                cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "SELECT * FROM `movies` WHERE `id` = \"" + movieId +"\""
            cursor.execute(sql)
            dbResult = cursor.fetchall()
            number_of_rows=dbResult[0]
            if len(number_of_rows) > 0:
                movieExists = 1

    finally:
        connection.close()
        return(movieExists)

def doesMovieNameExist(movieName):
    movieExists = 0
    connection = pymysql.connect(host=dbhost,
                                user=dbuser,
                                password=dbpw,
                                db='movies',
                                cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "SELECT * FROM `movies` WHERE `name` = \"" + movieName +"\""
            cursor.execute(sql)
            dbResult = cursor.fetchall()
            number_of_rows=dbResult[0]
            if len(number_of_rows) > 0:
                movieExists = 1

    finally:
        connection.close()
        return(movieExists)

def addNewMovie(rootId, movieName, releaseDate, rating, runTime, genre, description, imgUrl):
    # Connect to the database
    connection = pymysql.connect(host=dbhost,
                                user=dbuser,
                                password=dbpw,
                                db='movies',
                                cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO `movies`(`id`,`name`, `release_date`, `genre`, `rating`, `running_time`, `poster_url`, `synopsis`) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, (rootId, movieName, releaseDate, genre, rating, runTime, imgUrl, description))

            connection.commit()
    finally:
        print(movieName+" added.\n")
        connection.close()

def getMovieIDFromName(movieName):
        connection = pymysql.connect(host=dbhost,
                                    user=dbuser,
                                    password=dbpw,
                                    db='movies',
                                    cursorclass=pymysql.cursors.DictCursor)
        try:
            with connection.cursor() as cursor:
                # Create a new record
                sql = "SELECT `id` FROM `movies` WHERE `name` = \"" + movieName +"\""
                cursor.execute(sql)
                dbResult = cursor.fetchall()

        finally:
            connection.close()
            return(dbResult[0]['id'])

def addShowings(results):
    print("Add showings...")
    #delete all existing showings for this date.
    #deleteAllShowingsForDate(startDate)
    deleteAllShowings()
    #This allows us to blanket or force update showings that may have changed
    for index, item in enumerate(results):
        movieId = getMovieIDFromName(results[index]['title'])
        showtimes = results[index]['showtimes'];
        for showtimeIndex, showtime in enumerate(showtimes):
            date_time = showtimes[showtimeIndex]['dateTime']
            theatreId = showtimes[showtimeIndex]['theatre']['id']
            addShowing(theatreId, movieId,date_time)

def addShowing(theatreId, movieId, date_time):
    # Connect to the database
    connection = pymysql.connect(host=dbhost,
                                user=dbuser,
                                password=dbpw,
                                db='movies',
                                cursorclass=pymysql.cursors.DictCursor)

    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO `showings`(`movie_id`, `theatre_id`, `date_time`) VALUES ( %s, %s, %s)"
            cursor.execute(sql, (movieId,theatreId,date_time))

            connection.commit()
    finally:
        print("Added movie "+ str(movieId) + " showing for theatre "+ str(theatreId) + " at "+str(date_time))
        connection.close()

def deleteAllShowings():
    connection = pymysql.connect(host=dbhost,
                                user=dbuser,
                                password=dbpw,
                                db='movies',
                                cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "DELETE FROM `showings`"
            cursor.execute(sql)
            connection.commit()
    finally:
        connection.close()
        print("All showings have been purged from database")

def deleteAllMovies():
    connection = pymysql.connect(host=dbhost,
                                user=dbuser,
                                password=dbpw,
                                db='movies',
                                cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "DELETE FROM `movies`"
            cursor.execute(sql)
            connection.commit()
    finally:
        connection.close()
        print("All movies have been purged from database")

def deleteAllShowingsForDate(startDate):
    a = datetime.strptime(startDate, "%Y-%m-%d").date()
    startDateTime = datetime.combine(a, datetime.min.time())
    print(str(startDateTime))
    endDateTime = datetime.combine(a, datetime.max.time())
    print(str(endDateTime))
        # Connect to the database
    connection = pymysql.connect(host=dbhost,
                                user=dbuser,
                                password=dbpw,
                                db='movies',
                                cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "DELETE FROM `showings` WHERE `date_time` BETWEEN  \"" + str(startDate) + "\" AND \"" + str(endDateTime) + "\""
            cursor.execute(sql)
            connection.commit()
    finally:
        connection.close()


def removeOldMovies(today):
    oneYearAgo = str(today - datetime.timedelta(365))
        # Connect to the database
    connection = pymysql.connect(host=dbhost,
                                user=dbuser,
                                password=dbpw,
                                db='movies',
                                cursorclass=pymysql.cursors.DictCursor)
    try:
        with connection.cursor() as cursor:
            # Create a new record
            sql = "DELETE FROM `movies` WHERE `release_date` <  \"" + oneYearAgo +"\""
            cursor.execute(sql)
            connection.commit()
    finally:
        connection.close()

def main():
    results = getMovieShowings(startDate,zipcode,radius,api_key)
    if results == None:
        print("no data for "+startDate)
    else:
        print(results)
        addNewMovies(results)
        addShowings(results) #This must be called after addNewMovies()


main()
