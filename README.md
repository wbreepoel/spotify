# Spotify
Application where people can see their top 5 listened songs and artists on Spotify. They also can see their history of songs played. An user has to follow the link to the Spotify
website, copy the token and paste it in the field. When clicking the submit button, the tables will update and data from 10 days back is displayed. If data already exists in the
table it will not be duplicated. Data will be added to the existing table, so over time an user has a table with all the songs he or she listened to on Spotify.  

This application is written in Flask with a SQLite database. The Spotify API is used to retrieve the data and display it in the application. This is touching the field of data engineering. An improvement could be to automate the token generation process so that the tables are updated automatically, either via Airflow or the Spotify API documentation. 
