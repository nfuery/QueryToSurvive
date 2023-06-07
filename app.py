from helper import helper
from datetime import datetime
import mysql.connector
import pandas
import streamlit as st
from PIL import Image
import time
import streamlit.components.v1 as components
from datetime import datetime
from datetime import date
import base64

constructors = helper.data_cleaner("Datasets/constructors.csv")
races = helper.data_cleaner("Datasets/races.csv")
drivers = helper.data_cleaner("Datasets/drivers.csv")
cars = helper.data_cleaner("Datasets/cars.csv")
upcomingRaces = helper.data_cleaner("Datasets/upRaces.csv")

st.title('QTS - F1 Betting Assistance')

# establish the connection 
connection = mysql.connector.connect(host="localhost",
user="root",
password="cpsc408!",
auth_plugin='mysql_native_password',
database = 'Formula1')

# create a cursor object using the connection 
cursor = connection.cursor()

def create_tables(): # creates all the tables

        teams = '''
            CREATE TABLE teams(
                teamID INTEGER NOT NULL PRIMARY KEY,
                points INTEGER NOT NULL,
                name VARCHAR(50),
                nationality VARCHAR(50),
                driver1ID INTEGER,
                driver2ID INTEGER
            );
        '''
        drivers = '''
            CREATE TABLE drivers(
                driverID INTEGER NOT NULL PRIMARY KEY,
                points INTEGER NOT NULL,
                driverRef VARCHAR(50),
                carNumber INTEGER,
                code VARCHAR(3),
                name VARCHAR(50),
                team VARCHAR(50),
                dob VARCHAR(50),
                nationality VARCHAR(50),
                url VARCHAR(50)
            );
        '''

        races = '''
            CREATE TABLE races(
                raceID INTEGER NOT NULL PRIMARY KEY,
                year INTEGER,
                round INTEGER,
                trackID INTEGER NOT NULL,
                name VARCHAR(50),
                date VARCHAR(50),
                time VARCHAR(50),
                url VARCHAR(100)
            );
        '''

        cars = '''
            CREATE TABLE cars(
                carID INTEGER NOT NULL PRIMARY KEY,
                name VARCHAR(50),
                year INTEGER,
                cylinders INTEGER,
                valves INTEGER,
                oil VARCHAR(50),
                capacity VARCHAR(50),
                veeAngle INTEGER,
                weight VARCHAR(50),
                maxrpm VARCHAR(50),
                powerInput VARCHAR(50)
            );
        '''

        bets = '''
            CREATE TABLE bets(
                betID INTEGER NOT NULL PRIMARY KEY,
                amount INTEGER,
                driverName VARCHAR(50),
                teamName VARCHAR(50),
                status VARCHAR(50)
            );
        '''

        upcomingRaces = '''
            CREATE TABLE upcomingRaces(
                raceNumber INTEGER NOT NULL PRIMARY KEY,
                year INTEGER,
                name VARCHAR(50),
                date VARCHAR(50),
                time VARCHAR(50)
            )
        '''
        queries = [teams, drivers, races, cars, bets, upcomingRaces]
        for query in queries:
            run(query)

# run query 
def run(query):
    cursor.reset()
    cursor.execute(query)
    connection.commit()

#return single attribute from a table 
def single_record(query):
    cursor.reset()
    cursor.execute(query)
    record = cursor.fetchone()
    return record

# execute many records at once
def bulk_insert(query, records):
    cursor.executemany(query, records)
    connection.commit()

# returns many attributes from a table
def multi_attribute(query):
    cursor.reset()
    cursor.execute(query)
    results = cursor.fetchall()
    if len(results) == 0:
        cleaned_results = None
    else:
        cleaned_results = []
        for row in results:
            cleaned_row = []
            for i in row:
                if i is not None:
                    cleaned_row.append(i)
            cleaned_results.append(cleaned_row)
    return cleaned_results

# Inserts values from csv into previously created table
def drivers_preprocess():
    attribute_count = len(drivers[0])
    placeholders = ",".join(["%s"]*attribute_count)
    query = "INSERT INTO drivers VALUES(" + placeholders + ")"
    cursor.executemany(query, drivers)
    connection.commit()

# Inserts values from csv into previously created table
def races_preprocess():
    attribute_count = len(races[0])
    placeholders = ",".join(["%s"]*attribute_count)
    query = "INSERT INTO races VALUES(" + placeholders + ")"
    cursor.executemany(query, races)
    connection.commit()

# Inserts values from csv into previously created table
def constructors_preprocess():
    attribute_count = len(constructors[0])
    placeholders = ",".join(["%s"]*attribute_count)
    query = "INSERT INTO teams VALUES(" + placeholders + ")"
    cursor.executemany(query, constructors)
    connection.commit()

# Inserts values from csv into previously created table
def cars_preprocess():
    attribute_count = len(cars[0])
    placeholders = ",".join(["%s"]*attribute_count)
    query = "INSERT INTO cars VALUES(" + placeholders + ")"
    cursor.executemany(query, cars)
    connection.commit()

# Inserts values from csv into previously created table
def upcomingRaces_preprocess():
    attribute_count = len(upcomingRaces[0])
    placeholders = ",".join(["%s"]*attribute_count)
    query = "INSERT INTO upcomingRaces VALUES(" + placeholders + ")"
    cursor.executemany(query, upcomingRaces)
    connection.commit()

# ---------------------------- IMPORTANT ------------------------------
# CALL THE FOLLOWING FUNCTIONS ONCE THEN RECOMMENT THEM
# create_tables()
# drivers_preprocess()
# races_preprocess()
# constructors_preprocess()
# cars_preprocess()
# upcomingRaces_preprocess()

# Gets all names from drivers
def getDrivers():
    query = '''
        SELECT name
        FROM drivers;
    '''
    return multi_attribute(query)

# Find specific driver based on their name
def findDriver(dName):
    query = '''
        SELECT name,team
        FROM drivers
        WHERE name = '%s';
    ''' % dName
    result = multi_attribute(query)
    if(result):
        return result[0]
    else:
        return None
    
# Displays all bets for the user to see, only allows specific attributes from the bets to be displayed. Utilizes a view.
def MyBets():
    st.header("Current Bets")
    drop_view_query = '''
        DROP VIEW IF EXISTS vBets;
    '''
    run(drop_view_query)

    createViewQuery = '''
            CREATE VIEW vBets AS
            SELECT betID, amount, driverName, teamName
            FROM bets;
            '''
    multi_attribute(createViewQuery)

    getViewInfo = '''
            SELECT *
            FROM vBets
            ORDER BY betID ASC;
            '''
    results = multi_attribute(getViewInfo)
    button = st.button('Clear bets')
    query = '''
        TRUNCATE TABLE bets;
    '''
    if button:
        run(query)
    elif results:
        df = pandas.DataFrame(
            results,
            columns=('Bet Number', 'Amount Bet', 'Driver For Pole', 'Team')
        )
        st.table(df)
    if results is None:
        st.write("No bets made")

# Allows the user to enter amount and driver name to create a bet. This bet is added to the bets table.
def makeBet():
    amount = st.text_input('Enter amount to bet')
    if(amount):
        driver = st.text_input('Enter driver name')
        if(driver):
            dAttributes = findDriver(driver)
            if(dAttributes):
                betQuery1 = '''
                SELECT COUNT(*)
                FROM bets;
                ''' 
                betCount = single_record(betQuery1)[0]
                betID = betCount + 1
                betQuery2 = f'''
                INSERT INTO bets
                VALUES ({betID}, {int(amount)}, '{dAttributes[0]}', '{dAttributes[1]}','Ongoing');
                '''
                button = st.button('Make bet')
                if(button):
                    run(betQuery2)
                    progress_text = "Bet in progress. Please wait."
                    my_bar = st.progress(0, text=progress_text)

                    for percent_complete in range(100):
                        time.sleep(0.025)
                        my_bar.progress(percent_complete + 1, text=progress_text)
                    st.write("Bet Registered!")
                    st.balloons()
            else:
                st.write("Enter valid driver name")

# Displays the upcoming race and the race after that
def UpcomingRace():
    i = 0
    nextButton = st.button('Subsequent Race')
    previousButton = st.button('Upcoming Race')
    query = '''
            SELECT sq.raceNumber, sq.year, sq.name, sq.date, sq.time
            FROM (
                SELECT raceNumber, year, name, date, time
                FROM upcomingRaces
                ORDER BY raceNumber ASC
            ) AS sq;
            '''
    results = multi_attribute(query)
    if(nextButton and i < len(results)):
        i += 1
    if(previousButton and i > 0):
        i -= 1
    st.subheader("The " + results[i][2] + " Grand Prix")
    st.write(str(results[i][3]) + ", " + str(results[i][1]))
    st.write("Race Time: " + str(results[i][4]))
    
# Displays the drivers which are ordered by their points
def DriverStandings():
    query = '''SELECT points,code,name,team
               FROM drivers
               ORDER BY points DESC;
            '''
    df = pandas.DataFrame(
        multi_attribute(query),
        index=('Pos %d' % i for i in range(1, 21)),
        columns=('Points','Code','Name','Team'))
    
    st.table(df)
    
# Displays the teams which are ordered by their points
def TeamStandings():
    query = '''SELECT points,name,nationality
               FROM teams
               ORDER BY points DESC;
            '''
    df = pandas.DataFrame(
        multi_attribute(query),
        index=('Pos %d' % i for i in range(1, 11)),
        columns=('Points','Name','Nationality'))
    
    st.table(df)

# Searches for a race based on the race name and specific years
def SearchForRace():
    raceName = st.text_input('Race Name')
    years = st.multiselect(
        'Select specific year(s)',
        ['2018', '2017', '2016', '2015', '2014', '2013', '2012', '2011', '2010'])
    
    year_str = ', '.join(years)
    if (year_str and raceName): # If the year and race have specific selections
        query = '''
        SELECT name, date, round, time
        FROM races
        WHERE name = '%s'
        AND year IN (%s)
        ORDER BY date DESC;
        ''' % (raceName, year_str)
        results = multi_attribute(query)
        if(results):
            df = pandas.DataFrame(
                results,
                columns=('Name','Date','Round','Time'))
            st.dataframe(df, width=2500)
    elif(year_str == '' and raceName):
        # handle case where year_str is empty
        query = '''
        SELECT name, date, round, time
        FROM races
        WHERE name = '%s'
        GROUP BY name, date, round, time
        ORDER BY date DESC;
        ''' % raceName
        results = multi_attribute(query)
        if(results):
            df = pandas.DataFrame(
                results,
                columns=('Name','Date','Round','Time'))
            st.dataframe(df, width=2500)
    elif(year_str and raceName == ''): # If the race name is empty
        query = '''
        SELECT name, date, round, time
        FROM races
        WHERE year IN (%s)
        GROUP BY name, date, round, time
        ORDER BY date DESC;
        ''' % year_str
        results = multi_attribute(query)
        if(results):
            df = pandas.DataFrame(
                results,
                columns=('Name','Date','Round','Time'))
            st.dataframe(df, width=2500)
    else: # If both year and race name are empty
        query = '''
        SELECT name, date, round, time
        FROM races
        ORDER BY date DESC;
        '''
        df = pandas.DataFrame(
            multi_attribute(query),
            columns=('Name','Date','Round','Time'))
        st.dataframe(df, width=2500)

def SearchForTeam(): # Searches for team based on specific name
    teamName = st.text_input('Team Name:')
    query = '''
    SELECT name,points,nationality,driver1ID,driver2ID
    FROM teams
    WHERE name = '%s';
    ''' % teamName
    results = multi_attribute(query)
    if(results): 
        header = results[0][0] + ' - ' + str(results[0][1]) + ' points'
        st.header(header)
        st.image('F1Images/'+results[0][0] + '1.png')
        st.header("The Car:")
        st.image('F1Images/'+results[0][0] + '2.png')
        st.header("The Drivers")
        query = '''
        SELECT name,code
        FROM drivers
        WHERE driverID = '%s';
        ''' % results[0][3]
        driver1 = multi_attribute(query)[0]
        driver1String = driver1[0] + ' - ' + driver1[1]
        st.subheader(driver1String)
        query = '''
        SELECT name,code
        FROM drivers
        WHERE driverID = '%s';
        ''' % results[0][4]
        driver2 = multi_attribute(query)[0]
        driver2String = driver2[0] + ' - ' + driver2[1]
        st.subheader(driver2String)
    elif(results == None and teamName != ''):
        st.subheader('Invalid team name')

def download_csv(df, file_name): # Downloads csv to computer
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()  # Convert DataFrame to CSV and encode as base64
    href = f'<a href="data:file/csv;base64,{b64}" download="{file_name}">Download CSV file</a>'
    return href

def createCSV(): # Creates a csv of teams, bets, and drivers utilizing joins and UNION
    button = st.button('Get CSV')
    query = '''
    SELECT * 
    FROM teams
    LEFT JOIN bets ON teams.name = bets.teamName
    LEFT JOIN drivers ON teams.name = drivers.team
    UNION
    SELECT *
    FROM teams
    RIGHT JOIN bets ON teams.name = bets.teamName
    RIGHT JOIN drivers ON teams.name = drivers.team;
    '''
    if(button):
        table = multi_attribute(query)
        if(table):
            df = pandas.DataFrame(table)
            # Create the download link
            download_link = download_csv(df, 'data.csv')

            # Display the download button
            st.markdown(download_link, unsafe_allow_html=True)

option = st.selectbox(
    '',
    ('Choose One','Make a Bet', 'My Bets', 'Upcoming Race', 'Driver Standings', 'Team Standings', 'Search For Race', 'Search For Team','Generate CSV'))

if(option == 'Make a Bet'):
    makeBet()

if(option == 'My Bets'):
    MyBets()

if(option == 'Upcoming Race'):
    UpcomingRace()

if(option == 'Driver Standings'):
    DriverStandings()

if(option == 'Team Standings'):
    TeamStandings()

if(option == 'Search For Race'):
    SearchForRace()

if(option == 'Search For Team'):
    SearchForTeam()

if(option == 'Generate CSV'):
    createCSV()

connection.close()