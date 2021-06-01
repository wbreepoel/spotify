import os
from cs50 import SQL
from flask import Flask, flash, redirect, render_template, request, session
from flask_session import Session
from tempfile import mkdtemp
from werkzeug.exceptions import default_exceptions, HTTPException, InternalServerError
from werkzeug.security import check_password_hash, generate_password_hash
from helper import apology, login_required
import sqlalchemy
import pandas as pd
from sqlalchemy.orm import sessionmaker
import requests
import json
from datetime import datetime
import datetime
import sqlite3


# Configure application
app = Flask(__name__)

# Ensure templates are auto-reloaded
app.config["TEMPLATES_AUTO_RELOAD"] = True


# Ensure responses aren't cached
@app.after_request
def after_request(response):
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Expires"] = 0
    response.headers["Pragma"] = "no-cache"
    return response


# Configure session to use filesystem (instead of signed cookies)
app.config["SESSION_FILE_DIR"] = mkdtemp()
app.config["SESSION_PERMANENT"] = False
app.config["SESSION_TYPE"] = "filesystem"
Session(app)

# Configure CS50 Library to use SQLite database
db = SQL("sqlite:///my_played_tracks.db")


@app.route("/", methods=["GET", "POST"])
@login_required
def index():
    if request.method == 'POST':

        gen_tok = request.form.get("token")
        if gen_tok == "":
            return redirect('/')
        if update_database(gen_tok) == False:
            flash("Tables not updated, wrong token..")
            return redirect('/')

        update_database(gen_tok)
        flash("Tables updated")

    return render_template("index.html")


@app.route("/logout")
def logout():
    """Log user out"""

    # Forget any user_id
    session.clear()

    # Redirect user to login form
    return redirect("/")



@app.route("/songs")
@login_required
def songs():

    songs = db.execute("""SELECT song_name as song, artist_name as artist, count(*) as frequency FROM my_played_tracks
                          WHERE spotify_id = ?
                          GROUP BY song_name
                          ORDER BY count(*) DESC
                          LIMIT 5 """, session["user_id"])

    for song in songs:
        song["artist"] = song["artist"]
        song["song"] = song["song"]
        song["frequency"] = song["frequency"]

    return render_template("songs.html", songs = songs)

@app.route("/artists")
@login_required
def artists():

    artists = db.execute("""SELECT artist_name as artists, count(*) as frequency FROM my_played_tracks
                            WHERE spotify_id = ?
                            GROUP BY artists
                            ORDER BY frequency desc
                            LIMIT 5; """, session["user_id"])

    for artist in artists:
        artist["artists"] = artist["artists"]
        artist["frequency"] = artist["frequency"]

    return render_template("artists.html", artists = artists)

@app.route("/search")
@login_required
def search():

    songs = db.execute("""SELECT song_name as Song, artist_name as Artist, timestamp as Date, substr(played_at,12,8) as Time
                          FROM my_played_tracks
                          ORDER BY Date DESC, Time DESC""")

    for song in songs:
        song["Song"] = song["Song"]
        song["Artist"] = song["Artist"]
        song["Date"] = song["Date"]
        song["Time"] = song["Time"]

    return render_template("search.html", songs = songs)


@app.route("/login", methods=["GET", "POST"])
def login():
    """Log user in"""

    # Forget any user_id
    session.clear()

    # User reached route via POST (as by submitting a form via POST)
    if request.method == "POST":

        # Ensure username was submitted
        if not request.form.get("username"):
            return apology("must provide username", 403)

        # Ensure password was submitted
        elif not request.form.get("password"):
            return apology("must provide password", 403)

        # Query database for username
        rows = db.execute("SELECT * FROM users WHERE username = ?", request.form.get("username"))

        # Ensure username exists and password is correct
        if len(rows) != 1 or not check_password_hash(rows[0]["hash"], request.form.get("password")):
            return apology("invalid username and/or password", 403)

        # Remember which user has logged in
        session["user_id"] = rows[0]["spotify_id"]

        # Redirect user to home page
        return redirect("/")

    # User reached route via GET (as by clicking a link or via redirect)
    else:
        return render_template("login.html")


@app.route("/register", methods=["GET", "POST"])
def register():
    if request.method == "POST":
        if not request.form.get("username"):
            return apology("Please provide username", 400)

        if not request.form.get("spotify_id"):
            return apology("Please provide Spotify ID", 400)

        # Ensure password was submitted
        elif not request.form.get("password"):
            return apology("Please provide password", 400)
        elif not request.form.get("confirmation"):
            return apology("Please type password again", 400)
        if request.form.get("password") != request.form.get("confirmation"):
            return apology("Please enter the same password")

        try:
            row = db.execute("""INSERT INTO users (username, hash, spotify_id) VALUES(:username, :hash, :spotify_id)""",
                            username = request.form.get("username"),
                            hash = generate_password_hash(request.form.get("password")),
                            spotify_id = request.form.get("spotify_id"))
        except:
            return apology("Spotify ID already used", 400)

        session["user_id"] = row


        return redirect("/")
    else:
        return render_template("register.html")



@app.route("/change_password", methods=["GET", "POST"])
def change_password():
    if request.method == "POST":
        if not request.form.get("oldpass"):
            return apology("must provide old password", 403)

        elif not request.form.get("newpass"):
            return apology("must provide a new password", 403)
        elif not request.form.get("confirmation"):
            return apology("Please insert new password again", 403)
        if request.form.get("newpass") != request.form.get("confirmation"):
            return apology("Please enter the same new password")

        rows = db.execute("SELECT * FROM users WHERE spotify_id = :spotify_id", spotify_id = session["user_id"])
        if not check_password_hash(rows[0]["hash"], request.form.get("oldpass")):
            return apology("Invalid current password", 403)

        db.execute("UPDATE users SET hash = :hash WHERE spotify_id = :spotify_id ", hash = generate_password_hash(request.form.get("newpass")), spotify_id = session["user_id"] )

        flash("Password changed!")
        #remember which user has logged in
        session["user_id"] = rows[0]["spotify_id"]

        return redirect("/")
    else:
        return render_template("change_password.html")


#Update the database function via the token and a kind of ETL process:

def update_database(gen_tok):


    DATABASE_LOCATION = "sqlite:///my_played_tracks.db"
    USER_ID = "wbreepoel"
    TOKEN = gen_tok

    def check_if_valid(df: pd.DataFrame) -> bool:
        if df.empty:
            print("No songs downloaded. Finishing execution")
            return false

        #check primary key for duplicates
        if pd.Series(df["played_at"]).is_unique:
            pass
        else:
            raise Exception("Primary key is duplicated")

        # check for nulls

        if df.isnull().values.any():
            raise Exception("Null values found")



        #extract part of ETL process


    headers = {
        "Accept" : "aplication/json",
        "Content-Type" : "application.json",
        "Authorization" : "Bearer {token}".format(token = TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days = 10)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?limit=50&after={time}".format(time=yesterday_unix_timestamp),headers = headers)
    r2 = requests.get("https://api.spotify.com/v1/me", headers = headers)

    data = r.json()
    data2 = r2.json()

    if data['error'] or data2['error']:
        return False

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []
    spotify_id = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])
        spotify_id.append(data2["id"])


    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps,
        "spotify_id" : spotify_id
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "artist_name", "played_at", "timestamp", "spotify_id"])

    if check_if_valid(song_df):
        print('The data is valid, proceed to loading stage')


    for i in range(song_df.shape[0]):
        try:
            db.execute("""INSERT INTO my_played_tracks (song_name, artist_name, played_at, timestamp, spotify_id) VALUES (:song_name, :artist_name, :played_at, :timestamp, :spotify_id)""",
                              song_name = song_df.loc[i][0],
                              artist_name = song_df.loc[i][1],
                              played_at = song_df.loc[i][2],
                              timestamp = song_df.loc[i][3],
                              spotify_id = song_df.loc[i][4])

        except:
            print("Data already exists in database")



    print("Entered data succesfully")

