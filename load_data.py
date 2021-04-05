import mysql.connector as mysql
import glob2
from datetime import datetime, timedelta

f = open('/Users/leighroy/Dropbox/PyCharmProjects/Load_Racing_2020/db_users/account.txt', 'r')
lines = f.readlines()
username = lines[0]
password = lines[1]

db = mysql.connect(host='localhost', database='racing_data', user=username, password=password,
                   allow_local_infile=True)

# creating an instance of 'cursor' class which is used to execute the 'SQL' statements in 'Python'
cursor = db.cursor(buffered=True)

########################################################################################################################

# load new data
print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Loading new data..")

for filename in glob2.iglob(
        '/Users/leighroy/Dropbox/PyCharmProjects/Get_Racing_2020/project/meetings_processed_csv/*.csv',
        with_matches=True):

    data_already_loaded = "No"

    # check to see if the file has already been loaded
    SQL_statement = "SELECT * FROM racing_data.f_race_meets WHERE SourceFileName ='" + str(filename[1][0]) + ".csv'"
    cursor.execute(SQL_statement)
    SQL_result = cursor.fetchall()
    for row in SQL_result:
        if row[0] == str(filename[1][0]) + ".csv":
            #print(row[0] + " already loaded..")
            data_already_loaded = "Yes"

    # if file doesn't exist insert it into racing_data.f_race_meets and then load the data, if it does don't do anything
    if data_already_loaded == "No":
        # insert file name into file name lookup table
        SQL_statement = "INSERT INTO racing_data.f_race_meets (SourceFileName) VALUES ('" + str(
            filename[1][0]) + ".csv')"
        cursor.execute(SQL_statement)
        db.commit()

        # load data into temp table
        print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Loading: " + str(filename[1][0]) + ".csv")

        SQL_statement = "LOAD DATA LOCAL INFILE \'/Users/leighroy/Dropbox/PyCharmProjects/Get_Racing_2020/project/meetings_processed_csv/" + str(
            filename[1][
                0]) + ".csv\' " + "INTO TABLE racing_data.r_race_meets FIELDS TERMINATED BY ',' ENCLOSED BY \'\"\' LINES TERMINATED BY \'\\n\' IGNORE 1 ROWS;"
        # print("LOAD DATA LOCAL INFILE \'/Users/leighroy/Dropbox/PyCharmProjects/Get_Racing_2020/project/meetings_processed_csv/" + str(filename[1][0]) + ".csv\' " + "INTO TABLE racing_data.r_race_meets FIELDS TERMINATED BY ',' ENCLOSED BY \'\"\' LINES TERMINATED BY \'\\n\' IGNORE 1 ROWS;")
        cursor.execute(SQL_statement)
        db.commit()

        # insert data into persistent table with file name
        SQL_statement = """INSERT INTO racing_data.p_race_meets (MeetingDate,MeetingId,NumberOfRaces,Penetrometer,RailPosition,Track,TrackId,Weather,SourceFileName)
                        SELECT MeetingDate,MeetingId,NumberOfRaces,Penetrometer,RailPosition,Track,TrackId,Weather
                        , '""" + str(filename[1][0]) + ".csv" + """' FROM racing_data.r_race_meets"""
        cursor.execute(SQL_statement)
        db.commit()
        SQL_statement = "DELETE FROM racing_data.r_race_meets"
        cursor.execute(SQL_statement)
        db.commit()

########################################################################################################################

for filename in glob2.iglob(
        '/Users/leighroy/Dropbox/PyCharmProjects/Get_Racing_2020/project/races_meta_processed_csv/*.csv',
        with_matches=True):

    data_already_loaded = "No"

    # check to see if the file has already been loaded
    SQL_statement = "SELECT * FROM racing_data.f_races_meta WHERE SourceFileName ='" + str(filename[1][0]) + ".csv'"
    cursor.execute(SQL_statement)
    SQL_result = cursor.fetchall()
    for row in SQL_result:
        if row[0] == str(filename[1][0]) + ".csv":
            #print(row[0] + " already loaded..")
            data_already_loaded = "Yes"

    # if file doesn't exist insert it into racing_data.f_races_meta and then load the data, if it does don't do anything
    if data_already_loaded == "No":
        # insert file name into file name lookup table
        SQL_statement = "INSERT INTO racing_data.f_races_meta (SourceFileName) VALUES ('" + str(
            filename[1][0]) + ".csv')"
        cursor.execute(SQL_statement)
        db.commit()

        # load data into temp table
        print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Loading: " + str(filename[1][0]) + ".csv")

        SQL_statement = "LOAD DATA LOCAL INFILE \'/Users/leighroy/Dropbox/PyCharmProjects/Get_Racing_2020/project/races_meta_processed_csv/" + str(
            filename[1][
                0]) + ".csv\' " + "INTO TABLE racing_data.r_races_meta FIELDS TERMINATED BY ',' ENCLOSED BY \'\"\' LINES TERMINATED BY \'\\n\' IGNORE 1 ROWS;"
        # print("LOAD DATA LOCAL INFILE \'/Users/leighroy/Dropbox/PyCharmProjects/Get_Racing_2020/project/races_meta_processed_csv/" + str(filename[1][0]) + ".csv\' " + "INTO TABLE racing_data.r_races_meta FIELDS TERMINATED BY ',' ENCLOSED BY \'\"\' LINES TERMINATED BY \'\\n\' IGNORE 1 ROWS;")
        cursor.execute(SQL_statement)
        db.commit()

        # insert data into persistent table with file name
        SQL_statement = """INSERT INTO racing_data.p_races_meta (AgeRestrictions, Class, Distance, Group_, IsJumps, RaceId, RaceNumber, RaceTime, RaceTimeString, SectionalDistance, SectionalTime, SectionalTimeString, SexRestrictions, TrackCondition, TrackConditionNumber, TrackType, WeightRestrictions, WindDirection, WindSpeed, SourceFileName)
                        SELECT AgeRestrictions, Class, Distance, Group_, IsJumps, RaceId, RaceNumber, RaceTime, RaceTimeString, SectionalDistance, SectionalTime, SectionalTimeString, SexRestrictions, TrackCondition, TrackConditionNumber, TrackType, WeightRestrictions, WindDirection, WindSpeed
                        , '""" + str(filename[1][0]) + ".csv" + """' FROM racing_data.r_races_meta"""
        cursor.execute(SQL_statement)
        db.commit()
        SQL_statement = "DELETE FROM racing_data.r_races_meta"
        cursor.execute(SQL_statement)
        db.commit()

########################################################################################################################

for filename in glob2.iglob(
        '/Users/leighroy/Dropbox/PyCharmProjects/Get_Racing_2020/project/race_runners_processed_csv/*.csv',
        with_matches=True):

    data_already_loaded = "No"

    # check to see if the file has already been loaded
    SQL_statement = "SELECT * FROM racing_data.f_race_runners WHERE SourceFileName ='" + str(filename[1][0]) + ".csv'"
    cursor.execute(SQL_statement)
    SQL_result = cursor.fetchall()
    for row in SQL_result:
        if row[0] == str(filename[1][0]) + ".csv":
            #print(row[0] + " already loaded..")
            data_already_loaded = "Yes"

    # if file doesn't exist insert it into racing_data.f_race_runners and then load the data, if it does don't do anything
    if data_already_loaded == "No":
        # insert file name into file name lookup table
        SQL_statement = "INSERT INTO racing_data.f_race_runners (SourceFileName) VALUES ('" + str(
            filename[1][0]) + ".csv')"
        cursor.execute(SQL_statement)
        db.commit()

        # load data into temp table
        print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Loading: " + str(filename[1][0]) + ".csv")

        SQL_statement = "LOAD DATA LOCAL INFILE \'/Users/leighroy/Dropbox/PyCharmProjects/Get_Racing_2020/project/race_runners_processed_csv/" + str(
            filename[1][
                0]) + ".csv\' " + "INTO TABLE racing_data.r_race_runners FIELDS TERMINATED BY ',' ENCLOSED BY \'\"\' LINES TERMINATED BY \'\\n\' IGNORE 1 ROWS;"
        # print("LOAD DATA LOCAL INFILE \'/Users/leighroy/Dropbox/PyCharmProjects/Get_Racing_2020/project/race_runners_processed_csv/" + str(filename[1][0]) + ".csv\' " + "INTO TABLE racing_data.r_race_runners FIELDS TERMINATED BY ',' ENCLOSED BY \'\"\' LINES TERMINATED BY \'\\n\' IGNORE 1 ROWS;")
        cursor.execute(SQL_statement)
        db.commit()

        # insert data into persistent table with file name
        SQL_statement = """INSERT INTO racing_data.p_race_runners (RaceId,Barrier,Flucs,FormId,Gears,HorseId,InRun,Jockey,JockeyId,Margin,Name,Number,Position,Price_Betfair,Price_NSWTab,Price_SP,Price_VICTab,RunNumber,Stewards,Trainer,TrainerId,Weight,SourceFileName)
                        SELECT RaceId,Barrier,Flucs,FormId,Gears,HorseId,InRun,Jockey,JockeyId,Margin,Name,Number,Position,Price_Betfair,Price_NSWTab,Price_SP,Price_VICTab,RunNumber,Stewards,Trainer,TrainerId,Weight
                        , '""" + str(filename[1][0]) + ".csv" + """' FROM racing_data.r_race_runners"""
        cursor.execute(SQL_statement)
        db.commit()
        SQL_statement = "DELETE FROM racing_data.r_race_runners"
        cursor.execute(SQL_statement)
        db.commit()

########################################################################################################################

for filename in glob2.iglob(
        '/Users/leighroy/Dropbox/PyCharmProjects/Get_Racing_2020/project/future_data/meetings_future/*.csv',
        with_matches=True):

    data_already_loaded = "No"

    # check to see if the file has already been loaded
    SQL_statement = "SELECT * FROM racing_data.f_meetings_future WHERE SourceFileName ='" + str(
        filename[1][0]) + ".csv'"
    cursor.execute(SQL_statement)
    SQL_result = cursor.fetchall()
    for row in SQL_result:
        if row[0] == str(filename[1][0]) + ".csv":
            #print(row[0] + " already loaded..")
            data_already_loaded = "Yes"

    # if file doesn't exist insert it into racing_data.f_meetings_future and then load the data, if it does don't do anything
    if data_already_loaded == "No":
        # insert file name into file name lookup table
        SQL_statement = "INSERT INTO racing_data.f_meetings_future (SourceFileName) VALUES ('" + str(
            filename[1][0]) + ".csv')"
        cursor.execute(SQL_statement)
        db.commit()

        # load data into temp table
        print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Loading: " + str(filename[1][0]) + ".csv")

        SQL_statement = "LOAD DATA LOCAL INFILE \'/Users/leighroy/Dropbox/PyCharmProjects/Get_Racing_2020/project/future_data/meetings_future/" + str(
            filename[1][
                0]) + ".csv\' " + "INTO TABLE racing_data.r_meetings_future FIELDS TERMINATED BY ',' ENCLOSED BY \'\"\' LINES TERMINATED BY \'\\n\' IGNORE 1 ROWS;"
        # print("LOAD DATA LOCAL INFILE \'/Users/leighroy/Dropbox/PyCharmProjects/Get_Racing_2020/project/future_data/meetings_future/" + str(filename[1][0]) + ".csv\' " + "INTO TABLE racing_data.r_meetings_future FIELDS TERMINATED BY ',' ENCLOSED BY \'\"\' LINES TERMINATED BY \'\\n\' IGNORE 1 ROWS;")
        cursor.execute(SQL_statement)
        db.commit()

        # insert data into persistent table with file name
        SQL_statement = """INSERT INTO racing_data.p_meetings_future (RaceNumber,TabNumber,Horse,Weight,Claim,Barrier,Jockey,Trainer,RaceId,HorseId,JockeyId,TrainerId,FormId,MeetingDate,LoadDate,SourceFileName)
                        SELECT RaceNumber,TabNumber,Horse,Weight,Claim,Barrier,Jockey,Trainer,RaceId,HorseId,JockeyId,TrainerId,FormId
                        , '""" + str(filename[1][0])[:10] + """','""" + datetime.strftime(datetime.now(),
                                                                                          '%Y-%m-%d') + """','""" + str(
            filename[1][0]) + ".csv" + """' FROM racing_data.r_meetings_future"""
        cursor.execute(SQL_statement)
        db.commit()
        SQL_statement = "DELETE FROM racing_data.r_meetings_future"
        cursor.execute(SQL_statement)
        db.commit()

########################################################################################################################

for filename in glob2.iglob(
        '/Users/leighroy/Dropbox/PyCharmProjects/Get_Racing_2020/project/future_data/forms_future/*.csv',
        with_matches=True):

    data_already_loaded = "No"

    # check to see if the file has already been loaded
    SQL_statement = "SELECT * FROM racing_data.f_forms_future WHERE SourceFileName ='" + str(filename[1][0]) + ".csv'"
    cursor.execute(SQL_statement)
    SQL_result = cursor.fetchall()
    for row in SQL_result:
        if row[0] == str(filename[1][0]) + ".csv":
            #print(row[0] + " already loaded..")
            data_already_loaded = "Yes"

    # if file doesn't exist insert it into racing_data.f_forms_future and then load the data, if it does don't do anything
    if data_already_loaded == "No":
        # insert file name into file name lookup table
        SQL_statement = "INSERT INTO racing_data.f_forms_future (SourceFileName) VALUES ('" + str(
            filename[1][0]) + ".csv')"
        cursor.execute(SQL_statement)
        db.commit()

        # load data into temp table
        print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Loading: " + str(filename[1][0]) + ".csv")

        SQL_statement = "LOAD DATA LOCAL INFILE \'/Users/leighroy/Dropbox/PyCharmProjects/Get_Racing_2020/project/future_data/forms_future/" + str(
            filename[1][
                0]) + ".csv\' " + "INTO TABLE racing_data.r_forms_future FIELDS TERMINATED BY ',' ENCLOSED BY \'\"\' LINES TERMINATED BY \'\\n\' IGNORE 1 ROWS;"
        # print("LOAD DATA LOCAL INFILE \'/Users/leighroy/Dropbox/PyCharmProjects/Get_Racing_2020/project/future_data/forms_future/" + str(filename[1][0]) + ".csv\' " + "INTO TABLE racing_data.r_forms_future FIELDS TERMINATED BY ',' ENCLOSED BY \'\"\' LINES TERMINATED BY \'\\n\' IGNORE 1 ROWS;")
        cursor.execute(SQL_statement)
        db.commit()

        # insert data into persistent table with file name
        SQL_statement = """INSERT INTO racing_data.p_forms_future (MeetingDate,Track,RaceNumber,StartTime,Distance,AgeRestrictions,ClassRestrictions,WeightRestrictions,RacePrizeMoney,SexRestrictions,WeightType,RaceName,JockeysCanClaim,HorseName,HorseAge,HorseSex,HorseSire,HorseDam,HorseNumber,HorseJockey,HorseBarrier,HorseTrainer,HorseWeight,HorseClaim,HorseLast10,HorseRecord,HorseRecordDistance,HorseRecordTrack,HorseRecordTrackDistance,HorseRecordFirm,HorseRecordGood,HorseRecordSoft,HorseRecordHeavy,HorseRecordJumps,HorseRecordFirstUp,HorseRecordSecondUp,HorsePrizeMoney,FormBarrier,FormClass,FormDistance,FormJockey,FormMargin,FormMeetingDate,FormName,FormOtherRunners,FormPosition,FormPrice,FormTime,FormTrack,FormTrackCondition,FormWeight,HorseRecordSynthetic,MeetingId,RaceId,HorseId,HorseTrainerId,HorseJockeyId,FormTrainerId,FormJockeyId,PrizeMoney,LoadDate,SourceFileName)
                        SELECT MeetingDate,Track,RaceNumber,StartTime,Distance,AgeRestrictions,ClassRestrictions,WeightRestrictions,RacePrizeMoney,SexRestrictions,WeightType,RaceName,JockeysCanClaim,HorseName,HorseAge,HorseSex,HorseSire,HorseDam,HorseNumber,HorseJockey,HorseBarrier,HorseTrainer,HorseWeight,HorseClaim,HorseLast10,HorseRecord,HorseRecordDistance,HorseRecordTrack,HorseRecordTrackDistance,HorseRecordFirm,HorseRecordGood,HorseRecordSoft,HorseRecordHeavy,HorseRecordJumps,HorseRecordFirstUp,HorseRecordSecondUp,HorsePrizeMoney,FormBarrier,FormClass,FormDistance,FormJockey,FormMargin,FormMeetingDate,FormName,FormOtherRunners,FormPosition,FormPrice,FormTime,FormTrack,FormTrackCondition,FormWeight,HorseRecordSynthetic,MeetingId,RaceId,HorseId,HorseTrainerId,HorseJockeyId,FormTrainerId,FormJockeyId,PrizeMoney
                        ,'""" + datetime.strftime(datetime.now(), '%Y-%m-%d') + """','""" + str(
            filename[1][0]) + ".csv" + """' FROM racing_data.r_forms_future"""
        cursor.execute(SQL_statement)
        db.commit()
        SQL_statement = "DELETE FROM racing_data.r_forms_future"
        cursor.execute(SQL_statement)
        db.commit()

########################################################################################################################

# rebuild indices
print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Rebuilding indices..")

SQL_statement = """OPTIMIZE TABLE f_race_runners"""
cursor.execute(SQL_statement)
db.commit()

SQL_statement = """OPTIMIZE TABLE t_race_results"""
cursor.execute(SQL_statement)
db.commit()

SQL_statement = """OPTIMIZE TABLE p_races_meta"""
cursor.execute(SQL_statement)
db.commit()

SQL_statement = """OPTIMIZE TABLE p_race_runners"""
cursor.execute(SQL_statement)
db.commit()

SQL_statement = """OPTIMIZE TABLE p_race_meets"""
cursor.execute(SQL_statement)
db.commit()

SQL_statement = """DROP TABLE IF EXISTS racing_data.t_race_results_delta"""
cursor.execute(SQL_statement)
db.commit()

########################################################################################################################

# create delta of new data

# *** NOTE! - for this to work properly the base set must have already been run
print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Creating results delta..")

SQL_statement = """CREATE TABLE racing_data.t_race_results_delta AS
SELECT DISTINCT
MeetingDate,
MeetingId,
NumberOfRaces,
Penetrometer,
RailPosition,
Track,
TrackId,
Weather,
AgeRestrictions,
Class,
Distance,
Group_,
IsJumps,
A.RaceId,
RaceNumber,
RaceTime,
RaceTimeString,
SectionalDistance,
SectionalTime,
SectionalTimeString,
SexRestrictions,
TrackCondition,
TrackConditionNumber,
TrackType,
WeightRestrictions,
WindDirection,
WindSpeed,
#RaceId,
Barrier,
Flucs,
FormId,
Gears,
HorseId,
InRun,
Jockey,
JockeyId,
Margin,
Name,
Number,
Position,
Price_Betfair,
Price_NSWTab,
Price_SP,
Price_VICTab,
RunNumber,
Stewards,
Trainer,
TrainerId,
Weight,
B.SourceFileName
FROM p_races_meta A LEFT JOIN p_race_runners B
ON A.RaceId = B.RaceId
LEFT JOIN (SELECT *, CONCAT(MeetingDate, '_races_meta_', SUBSTRING(SourceFileName,22,LENGTH(SourceFileName)-21)) metaCSVname FROM p_race_meets) C
ON A.SourceFileName = C.metaCSVname
INNER JOIN
     (
         SELECT DISTINCT A.SourceFileName
         FROM f_race_runners A
                  LEFT JOIN t_race_results B
                            ON A.SourceFileName = B.SourceFileName
         WHERE B.SourceFileName IS NULL
     ) D
ON B.SourceFileName = D.SourceFileName"""
cursor.execute(SQL_statement)
db.commit()

# insert delta into pre processed table
SQL_statement = """INSERT INTO racing_data.t_race_results  (SELECT * FROM racing_data.t_race_results_delta)"""
cursor.execute(SQL_statement)
db.commit()

########################################################################################################################

# drop old (step 1) processed table
SQL_statement = """DROP TABLE IF EXISTS racing_data.p_race_results"""
cursor.execute(SQL_statement)
db.commit()

# re-create new processed table - step 1
print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Creating new processed table - step 1..")

SQL_statement = """CREATE TABLE racing_data.p_race_results AS
SELECT MeetingDate,
MeetingId,
NumberOfRaces,
Penetrometer,
RailPosition,
Track,
TrackId,
Weather,
AgeRestrictions,
Class,
Distance,
Group_,
IsJumps,
RaceId,
RaceNumber,
RaceTime,
RaceTimeString,
SectionalDistance,
SectionalTime,
SectionalTimeString,
SexRestrictions,
TrackCondition,
TrackConditionNumber,
TrackType,
WeightRestrictions,
WindDirection,
WindSpeed,
Barrier,
Flucs,
FormId,
Gears,
HorseId,
InRun,
Jockey,
JockeyId,
Margin,
Name,
Number,
Position,
Price_Betfair,
Price_NSWTab,
Price_SP,
Price_VICTab,
RunNumber,
Stewards,
Trainer,
TrainerId,
Weight,
SourceFileName,
MilliSecondsAfterFirst,
MilliSecRaceFinishTS,
RaceFinishTS_1,
RaceFinishTS_2,
       CASE WHEN Distance > 0 AND (((MilliSecRaceFinishTS / 1000) / 60) / 60) > 0
            THEN (Distance / 1000) / (((MilliSecRaceFinishTS / 1000) / 60) / 60)
            ELSE 0 END AS AvgKMH FROM (
                  SELECT *
                       , X.RaceMilliSeconds + X.RaceMinutesTS + X.RaceSecondsTS +
                         X.MilliSecondsAfterFirst                                                       AS MilliSecRaceFinishTS
                       , FROM_UNIXTIME(
                              (X.RaceMilliSeconds + X.RaceMinutesTS + X.RaceSecondsTS + X.MilliSecondsAfterFirst) *
                              0.001)                                                                    AS RaceFinishTS_1
                       , SUBSTRING(FROM_UNIXTIME((X.RaceMilliSeconds + X.RaceMinutesTS + X.RaceSecondsTS +
                                                  X.MilliSecondsAfterFirst) * 0.001, '%i:%s:%f'), 1,
                                  9)                                                                    AS RaceFinishTS_2
                  FROM (
                           SELECT *
                                , CASE
                                      WHEN Position = 1 THEN 0
                                      ELSE
                                          0.16 * Margin END                   AS SecondsAfterFirst
                                , SUBSTRING(RaceTimeString, 4, 2)             AS RaceMinutes
                                , SUBSTRING(RaceTimeString, 7, 2)             AS RaceSeconds
                                , SUBSTRING(RaceTimeString, 10, 3)            AS RaceMilliSeconds
                                , SUBSTRING(RaceTimeString, 4, 2) * 60 * 1000 AS RaceMinutesTS
                                , SUBSTRING(RaceTimeString, 7, 2) * 1000      AS RaceSecondsTS
                                , ROUND(CASE
                                            WHEN Position = 1 THEN 0
                                            ELSE
                                                0.16 * Margin END * 1000)     AS MilliSecondsAfterFirst
                           FROM racing_data.t_race_results
                       ) X
              ) Y"""
cursor.execute(SQL_statement)
db.commit()

SQL_statement = """OPTIMIZE TABLE racing_data.p_race_results"""
cursor.execute(SQL_statement)
db.commit()

########################################################################################################################

# re-create new processed table - step 2
print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Creating new processed table - step 2..")

# drop old future forms table
SQL_statement = """DROP TABLE IF EXISTS racing_data.p_forms_future_1"""
cursor.execute(SQL_statement)
db.commit()

# re-create future forms table with the extra info we need
SQL_statement = """
CREATE TABLE racing_data.p_forms_future_1 AS
SELECT DISTINCT
  RaceName
, RacePrizeMoney
, STR_TO_DATE(SUBSTRING(MeetingDate, 1, 10), '%d/%c/%Y') AS MeetingDate_DT
, Track
, RaceNumber
, StartTime
FROM p_forms_future
"""
cursor.execute(SQL_statement)
db.commit()

# drop old (step 2) processed table
SQL_statement = """DROP TABLE IF EXISTS racing_data.p_race_results_1"""
cursor.execute(SQL_statement)
db.commit()

# re-create new processed table - step 2
SQL_statement = """CREATE TABLE racing_data.p_race_results_1 AS
SELECT
  RaceName
, RacePrizeMoney
, A.Track
, TrackId
, A.MeetingDate
, A.MeetingId
, A.RaceNumber
, StartTime
, RailPosition
, Weather
, A.AgeRestrictions
, A.WeightRestrictions
, Class
, A.Distance
, Group_
, TrackType
, TrackConditionNumber
, TrackCondition
, IsJumps
, A.RaceId
, Barrier
, Jockey
, JockeyId
, FormId
, Gears
, A.HorseId
, Trainer
, TrainerId
, Weight
, InRun
, Margin
, Name
, Number
, CAST(Position AS UNSIGNED) AS Position
, RaceFinishTS_2
, ROUND(AvgKMH,2) AS AvgKMH
, MilliSecondsAfterFirst
, ROUND(Price_Betfair,2) AS Price_Betfair
, Price_NSWTab
, Price_SP
, Price_VICTab
, Flucs
, RunNumber
, Stewards
FROM racing_data.p_race_results A INNER JOIN p_forms_future_1 B
ON A.MeetingDate = B.MeetingDate_DT
AND A.RaceNumber = B.RaceNumber
AND A.Track = B.Track"""
cursor.execute(SQL_statement)
db.commit()

SQL_statement = """OPTIMIZE TABLE racing_data.p_race_results_1"""
cursor.execute(SQL_statement)
db.commit()

########################################################################################################################

# create jockey stats
print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Creating jockey stats..")

# drop old jockey stats table
SQL_statement = """DROP TABLE IF EXISTS racing_data.p_jockey_stats"""
cursor.execute(SQL_statement)
db.commit()

SQL_statement = """CREATE TABLE racing_data.p_jockey_stats AS
SELECT A.*, B.WinCountL10, B.WinPercentL10, B.NumberOfRidesL10
FROM (
# win percentage all available races races
         SELECT *
         FROM (
                  SELECT JockeyId
                       , Jockey
                       , SUM(CASE WHEN MinPositionForMeet = 1 THEN 1 ELSE 0 END) AS WinCountCareer
                       , CASE
                             WHEN SUM(CASE WHEN MinPositionForMeet = 1 THEN 1 ELSE 0 END) = 0 THEN 0
                             ELSE (SUM(CASE WHEN MinPositionForMeet = 1 THEN 1 ELSE 0 END) / SUM(1)) * 100
                      END                                                        AS WinPercentCareer
                       , SUM(1)                                                  AS NumberOfRidesCareer
                  FROM (
                           SELECT *
                           FROM (
                                    SELECT *
                                         , RANK() OVER (PARTITION BY
                                        JockeyId
                                        ORDER BY
                                            CAST(MeetingDate AS DATE) DESC
                                        ) RaceOrder
                                    FROM (
                                             SELECT JockeyId
                                                  , Jockey
                                                  , MeetingDate
                                                  , MIN(Position) MinPositionForMeet
                                             FROM racing_data.p_race_results_1
                                             WHERE JockeyId > 0
                                             GROUP BY JockeyId
                                                    , Jockey
                                                    , MeetingDate
                                         ) X
                                ) Y
                       ) Z
                  GROUP BY JockeyId
                         , Jockey
              ) AA
         ORDER BY WinPercentCareer DESC
     ) A
         LEFT JOIN (
# win percentage in last 10 races
    SELECT *
    FROM (
             SELECT JockeyId
                  , Jockey
                  , SUM(CASE WHEN MinPositionForMeet = 1 THEN 1 ELSE 0 END) AS WinCountL10
                  , CASE
                        WHEN SUM(CASE WHEN MinPositionForMeet = 1 THEN 1 ELSE 0 END) = 0 THEN 0
                        ELSE (SUM(CASE WHEN MinPositionForMeet = 1 THEN 1 ELSE 0 END) / SUM(1)) * 100
                 END                                                        AS WinPercentL10
                  , SUM(1)                                                  AS NumberOfRidesL10
             FROM (
                      SELECT *
                      FROM (
                               SELECT *
                                    , RANK() OVER (PARTITION BY
                                   JockeyId
                                   ORDER BY
                                       CAST(MeetingDate AS DATE) DESC
                                   ) RaceOrder
                               FROM (
                                        SELECT JockeyId
                                             , Jockey
                                             , MeetingDate
                                             , MIN(Position) MinPositionForMeet
                                        FROM racing_data.p_race_results_1
                                        WHERE JockeyId > 0
                                        GROUP BY JockeyId
                                               , Jockey
                                               , MeetingDate
                                    ) X
                           ) Y
                      WHERE RaceOrder <= 10
                  ) Z
             GROUP BY JockeyId
                    , Jockey
         ) AA
    ORDER BY WinPercentL10 DESC
) B ON A.JockeyId = B.JockeyId"""
cursor.execute(SQL_statement)
db.commit()

SQL_statement = """OPTIMIZE TABLE racing_data.p_jockey_stats"""
cursor.execute(SQL_statement)
db.commit()

########################################################################################################################

# create trainer stats
print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Creating trainer stats..")

# drop old trainer stats table
SQL_statement = """DROP TABLE IF EXISTS racing_data.p_trainer_stats"""
cursor.execute(SQL_statement)
db.commit()

SQL_statement = """CREATE TABLE racing_data.p_trainer_stats AS
SELECT A.*, B.WinCountL10, B.WinPercentL10, B.NumberOfRidesL10
FROM (
         SELECT *
         FROM (
                  SELECT TrainerId
                       , Trainer
                       , SUM(CASE WHEN MinPositionForMeet = 1 THEN 1 ELSE 0 END) AS WinCountCareer
                       , CASE
                             WHEN SUM(CASE WHEN MinPositionForMeet = 1 THEN 1 ELSE 0 END) = 0
                                 THEN 0
                             ELSE (SUM(CASE WHEN MinPositionForMeet = 1 THEN 1 ELSE 0 END) / SUM(1)) * 100
                      END                                                        AS WinPercentCareer
                       , SUM(1)                                                  AS NumberOfRidesCareer
                  FROM (
                           SELECT *
                           FROM (
                                    SELECT *
                                         , RANK()
                                            OVER (PARTITION BY
                                                TrainerId
                                                ORDER BY
                                                    CAST(MeetingDate AS DATE) DESC
                                                ) RaceOrder
                                    FROM (
                                             SELECT TrainerId
                                                  , Trainer
                                                  , MeetingDate
                                                  , Track
                                                  , MIN(Position) MinPositionForMeet
                                             FROM racing_data.p_race_results_1
                                             GROUP BY TrainerId
                                                    , Trainer
                                                    , MeetingDate
                                                    , Track
                                             ORDER BY TrainerId ASC, MeetingDate DESC, Track
                                         ) X
                                ) Y
                       ) Z
                  GROUP BY TrainerId
                         , Trainer
              ) AA
         ORDER BY WinPercentCareer DESC
     ) A
         LEFT JOIN (
    SELECT *
    FROM (
             SELECT TrainerId
                  , Trainer
                  , SUM(CASE WHEN MinPositionForMeet = 1 THEN 1 ELSE 0 END) AS WinCountL10
                  , CASE
                        WHEN SUM(CASE WHEN MinPositionForMeet = 1 THEN 1 ELSE 0 END) = 0 THEN 0
                        ELSE (SUM(CASE WHEN MinPositionForMeet = 1 THEN 1 ELSE 0 END) / SUM(1)) * 100
                 END                                                        AS WinPercentL10
                  , SUM(1)                                                  AS NumberOfRidesL10
             FROM (
                      SELECT *
                      FROM (
                               SELECT *
                                    , RANK() OVER (PARTITION BY
                                   TrainerId
                                   ORDER BY
                                       CAST(MeetingDate AS DATE) DESC
                                   ) RaceOrder
                               FROM (
                                        SELECT TrainerId
                                             , Trainer
                                             , MeetingDate
                                             , Track
                                             , MIN(Position) MinPositionForMeet
                                        FROM racing_data.p_race_results_1
                                        GROUP BY TrainerId
                                               , Trainer
                                               , MeetingDate
                                               , Track
                                        ORDER BY TrainerId ASC, MeetingDate DESC, Track
                                    ) X
                           ) Y
                      WHERE RaceOrder <= 10
                  ) Z
             GROUP BY TrainerId
                    , Trainer
         ) AA
    ORDER BY WinPercentL10 DESC
) B ON A.TrainerId = B.TrainerId"""
cursor.execute(SQL_statement)
db.commit()

SQL_statement = """OPTIMIZE TABLE racing_data.p_trainer_stats"""
cursor.execute(SQL_statement)
db.commit()

########################################################################################################################

# create par speeds table
print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Creating par speeds..")

# drop old track par speeds table
SQL_statement = """DROP TABLE IF EXISTS racing_data.p_track_par_speeds"""
cursor.execute(SQL_statement)
db.commit()

# re-create track par speeds table
SQL_statement = """
CREATE TABLE racing_data.p_track_par_speeds AS
SELECT * FROM (
    SELECT Track, Distance, Class, ROUND(AVG(AvgKMH),3) AS ParSpeed, ROUND(MAX(AvgKMH),3) AS FastestSpeed, ROUND(MAX(AvgKMH) - AVG(AvgKMH),3) AS FastestDistanceFromAvg FROM racing_data.p_race_results_1
    WHERE AvgKMH <> 0
    GROUP BY Track, Distance, Class
    ) A
WHERE ParSpeed < 100
ORDER BY Track, Distance, Class
"""
cursor.execute(SQL_statement)
db.commit()

########################################################################################################################

# create race results with par speeds
print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Creating race results with par speeds..")

# drop old track par speeds table
SQL_statement = """DROP TABLE IF EXISTS racing_data.p_race_results_2"""
cursor.execute(SQL_statement)
db.commit()

# re-create race results 2 table
SQL_statement = """
CREATE TABLE racing_data.p_race_results_2 AS
SELECT A.*, B.ParSpeed, B.FastestDistanceFromAvg, ROUND((AvgKMH - ParSpeed),3) AS DistanceFromParSpKMH, ROUND((AvgKMH - FastestSpeed),3) AS DistanceFromFastestSpKMH FROM racing_data.p_race_results_1 A LEFT JOIN racing_data.p_track_par_speeds B
ON A.Track = B.Track AND A.Distance = B.Distance AND A.Class = B.Class
"""
cursor.execute(SQL_statement)
db.commit()

########################################################################################################################

# create list of horses and their current par speeds
print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Creating horses and their current par speeds..")

# drop old horse par speeds table
SQL_statement = """DROP TABLE IF EXISTS racing_data.p_horse_speed_rating"""
cursor.execute(SQL_statement)
db.commit()

# re-create horse par speeds table
SQL_statement = """
CREATE TABLE racing_data.p_horse_speed_rating AS
SELECT HorseId, ROUND(AVG(DistanceFromParSpKMH),3) AS HorseAvgSpRating, ROUND(ROUND(AVG(DistanceFromParSpKMH),3) * ((SUM(1) * 0.2)) ,3) AS HorseAvgSpRating_1, SUM(1) AS NumOfRaces FROM (
    SELECT *, RANK() OVER (PARTITION BY HorseId ORDER BY MeetingDate DESC, CAST(RaceNumber AS UNSIGNED ) DESC) AS RaceOrderSequence FROM racing_data.p_race_results_2
    WHERE DistanceFromParSpKMH <= 3.5 AND DistanceFromParSpKMH >= -3.5
) A
WHERE RaceOrderSequence <= 5
GROUP BY HorseId
ORDER BY HorseAvgSpRating_1 DESC
"""
cursor.execute(SQL_statement)
db.commit()

########################################################################################################################

# create current next day model data
print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Creating current 'next day' model data..")

# drop old next day model data
SQL_statement = """DROP TABLE IF EXISTS racing_data.p_model_data_temp"""
cursor.execute(SQL_statement)
db.commit()

# re-create next day model data table
SQL_statement = """
CREATE TABLE racing_data.p_model_data_temp AS
SELECT DISTINCT
#MeetingDate,
Track,
RaceNumber,
StartTime,
Distance,
AgeRestrictions,
ClassRestrictions,
WeightRestrictions,
#RacePrizeMoney,
SUBSTRING_INDEX(SUBSTRING_INDEX(RacePrizeMoney, ' ', 2), ' ', -1) AS RacePrizeMoneyTotal,
SexRestrictions,
WeightType,
RaceName,
JockeysCanClaim,
A.HorseId,
HorseName,
HorseAge,
HorseSex,
#HorseSire,
#HorseDam,
HorseNumber,
HorseJockey,
HorseJockeyId,
HorseBarrier,
HorseTrainer,
HorseTrainerId,
HorseWeight,
(HorseWeight - HorseClaim) AS HorseWeightAfterClaim,
HorseLast10,
LPAD(REPLACE(HorseLast10, 'x', '') ,10,0) AS HorseLast10_1,
LENGTH(REPLACE(REPLACE(HorseLast10, 'x', ''),'0','')) AS HorseNumberOfRaces,
CHAR_LENGTH(HorseLast10) - CHAR_LENGTH( REPLACE ( HorseLast10, '1', '') ) AS HorseNumberOfWinsL10,
CASE WHEN (LENGTH(REPLACE(REPLACE(HorseLast10, 'x', ''),'0',''))) > 0 THEN
     CASE WHEN (((CHAR_LENGTH(HorseLast10) - CHAR_LENGTH( REPLACE ( HorseLast10, '1', '') )) / LENGTH(REPLACE(REPLACE(HorseLast10, 'x', ''),'0',''))) * 100) IS NULL THEN 0.0000
     ELSE (((CHAR_LENGTH(HorseLast10) - CHAR_LENGTH( REPLACE ( HorseLast10, '1', '') )) / LENGTH(REPLACE(REPLACE(HorseLast10, 'x', ''),'0',''))) * 100) END ELSE 0.0000
END AS HorseNumberOfWinsPercentL10,
CASE WHEN B.WinPercentL10 IS NULL THEN 0.0000 ELSE B.WinPercentL10 END AS JockeyWinPercentL10,
CASE WHEN B.NumberOfRidesL10 IS NULL THEN 0 ELSE B.NumberOfRidesL10 END AS JockeyNumberOfRidesL10,
CASE WHEN B.WinPercentCareer IS NULL THEN 0.0000 ELSE B.WinPercentCareer END AS JockeyWinPercentCareer,
CASE WHEN B.NumberOfRidesCareer IS NULL THEN 0 ELSE B.NumberOfRidesCareer END AS JockeyNumberOfRidesCareer,
CASE WHEN C.WinPercentL10 IS NULL THEN 0.0000 ELSE C.WinPercentL10 END AS TrainerWinPercentL10,
CASE WHEN C.NumberOfRidesL10 IS NULL THEN 0 ELSE C.NumberOfRidesL10 END AS TrainerNumberOfRidesL10,
CASE WHEN C.WinPercentCareer IS NULL THEN 0.0000 ELSE C.WinPercentCareer END AS TrainerWinPercentCareer,
CASE WHEN C.NumberOfRidesCareer IS NULL THEN 0 ELSE C.NumberOfRidesCareer END AS TrainerNumberOfRidesCareer,
CASE WHEN E.HorseAvgSpRating_1 IS NULL THEN 0 ELSE E.HorseAvgSpRating_1 END AS HorseAvgSpRating_1,
CASE WHEN DATEDIFF(STR_TO_DATE(LEFT(StartTime,11),'%d-%b-%Y'), CAST(LastRaceDate AS DATE)) IS NULL THEN -1 ELSE DATEDIFF(STR_TO_DATE(LEFT(StartTime,11),'%d-%b-%Y'), CAST(LastRaceDate AS DATE)) END AS DaysSinceLastRace,
LoadDate
FROM racing_data.p_forms_future A
    LEFT JOIN racing_data.p_jockey_stats B
ON A.HorseJockeyId = B.JockeyId
    LEFT JOIN racing_data.p_trainer_stats C
ON A.HorseTrainerId = C.TrainerId
    LEFT JOIN
    (
        SELECT HorseId, MAX(MeetingDate) AS LastRaceDate FROM racing_data.p_race_results_1
        GROUP BY HorseId
    ) D
ON A.HorseId = D.HorseId
    LEFT JOIN racing_data.p_horse_speed_rating E
ON A.HorseId = E.HorseId
WHERE A.HorseId IS NOT NULL
AND LoadDate = CURDATE()
ORDER BY STR_TO_DATE(LEFT(StartTime,11),'%d-%b-%Y') DESC, Track ASC, RaceNumber ASC, CAST(HorseBarrier AS UNSIGNED) ASC
"""
cursor.execute(SQL_statement)
db.commit()

# insert into model history table
SQL_statement = """
INSERT INTO racing_data.p_model_data
SELECT * FROM racing_data.p_model_data_temp"""
cursor.execute(SQL_statement)
db.commit()

########################################################################################################################

# create next day odds meet list for scraping
print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Creating 'next day' odds meet list for scraping..")

# drop old next day odds meet list table
SQL_statement = """DROP TABLE IF EXISTS racing_data.p_next_day_odds_race_list"""
cursor.execute(SQL_statement)
db.commit()

# create next day odds meet list table
SQL_statement = """
CREATE TABLE racing_data.p_next_day_odds_race_list AS
SELECT * FROM (
                  SELECT DISTINCT STR_TO_DATE(LEFT(MeetingDate, INSTR(MeetingDate, ' ')), '%d/%c/%Y') AS MeetingDate,
                                  RaceNumber,
                                  REPLACE(Track,' ','-') AS Track
                  FROM racing_data.p_forms_future
              ) A
#WHERE  MeetingDate = CURDATE() + 1
WHERE MeetingDate = DATE_ADD(CURDATE(), INTERVAL 1 DAY)
ORDER BY MeetingDate DESC, Track, CAST(RaceNumber AS SIGNED) ASC"""
cursor.execute(SQL_statement)
db.commit()

########################################################################################################################

print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Processing complete")