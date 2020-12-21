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

print(datetime.now().strftime("%d.%b %Y %H:%M:%S") + " ** Processing complete")