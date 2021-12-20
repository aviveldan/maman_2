from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Match import Match
from Business.Player import Player
from Business.Stadium import Stadium
from psycopg2 import sql


def createTables():
    conn = None

    try:
        conn = Connector.DBConnector()
        conn.execute("CREATE TABLE Teams(id integer PRIMARY KEY, CHECK(id>0))")
        conn.execute(
            "CREATE TABLE Matches(id integer PRIMARY KEY NOT NULL CHECK(id>0), competition TEXT NOT NULL CHECK(competition = 'International' OR competition = 'Domestic' ), home_id integer NOT NULL CHECK(home_id>0), away_id integer NOT NULL CHECK(away_id>0 AND away_id != home_id), FOREIGN KEY (home_id) REFERENCES Teams(id) ON DELETE CASCADE, FOREIGN KEY (away_id) REFERENCES Teams(id) ON DELETE CASCADE)")
        conn.execute(
            "CREATE TABLE Players(id integer PRIMARY KEY NOT NULL CHECK(id>0), team_id integer NOT NULL CHECK(team_id>0), age integer NOT NULL CHECK(age>0), height integer NOT NULL CHECK(height>0), preferred_foot TEXT NOT NULL CHECK(preferred_foot = 'Left' OR preferred_foot = 'Right'),FOREIGN KEY (team_id) REFERENCES Teams(id) ON DELETE CASCADE)")
        conn.execute(
            "CREATE TABLE Stadiums(id integer PRIMARY KEY NOT NULL CHECK(id>0), capacity integer NOT NULL CHECK(capacity>0), belong_to integer UNIQUE, FOREIGN KEY (belong_to) REFERENCES Teams(id) ON DELETE CASCADE)")
        conn.execute(
            "CREATE TABLE Scores(match_id integer, player_id integer, goals integer CHECK(goals>=0),PRIMARY KEY(match_id, player_id), FOREIGN KEY (match_id) REFERENCES Matches(id) ON DELETE CASCADE, FOREIGN KEY (player_id) REFERENCES Players(id) ON DELETE CASCADE)")
        conn.execute(
            "CREATE TABLE Attendance(match_id integer UNIQUE, stadium_id integer, attendance integer CHECK(attendance>=0),PRIMARY KEY(match_id),  FOREIGN KEY (match_id) REFERENCES Matches(id) ON DELETE CASCADE, FOREIGN KEY (stadium_id) REFERENCES Stadiums(id) ON DELETE CASCADE)")
        conn.execute("CREATE VIEW AverageAttendance AS SELECT stadium_id, AVG(attendance) FROM Attendance GROUP BY stadium_id")
        # Scores JOIN Matches by match_id JOIN stadiums by belong_to
        conn.execute("CREATE VIEW TotalStadiumGoals AS  SELECT Attendance.stadium_id as s_id ,COALESCE(sum(Scores.goals), 0) as total_goals FROM Attendance LEFT JOIN Scores ON Attendance.match_id=Scores.match_id GROUP BY Attendance.stadium_id")
        conn.execute("CREATE VIEW TotalStadiumGoalsIncludingZeros AS SELECT stadiums.id as stad_id,COALESCE(total_goals, 0) as total_goals FROM stadiums LEFT JOIN TotalStadiumGoals ON stadiums.id=TotalStadiumGoals.s_id")
        conn.execute("CREATE VIEW TotalMatchGoals AS SELECT match_id as m_id, SUM(goals) as sum_goals FROM Scores GROUP BY match_id")
        conn.execute("CREATE VIEW Winners AS SELECT Scores.match_id, Scores.player_id FROM Scores WHERE Scores.goals >= 0.5 *(SELECT sum(sum_goals) FROM TotalMatchGoals WHERE TotalMatchGoals.m_id = Scores.match_id)")
        conn.execute(
            "CREATE VIEW ActiveTeams AS SELECT Matches.home_id as id FROM Matches UNION SELECT Matches.away_id as id FROM Matches")
        conn.execute(
            "CREATE VIEW ActiveTallTeams AS SELECT team_id, count(team_id) as total FROM players JOIN activeteams ON players.team_id = activeteams.id WHERE players.height>190 GROUP BY team_id")
        conn.execute(
            "CREATE VIEW RichTeams AS SELECT belong_to FROM Stadiums WHERE capacity > 55000")
        conn.execute(
            "CREATE VIEW ActiveTallRichTeams AS SELECT team_id, total FROM ActiveTallTeams JOIN RichTeams ON ActiveTallTeams.team_id = RichTeams.belong_to ")
        conn.execute("CREATE VIEW AttendanceTeams AS SELECT home_id, attendance FROM Attendance JOIN Matches ON Attendance.match_id = Matches.id")
        conn.execute("CREATE VIEW BadTeams AS Select Matches.home_id as team_id FROM Matches LEFT OUTER JOIN Attendance ON Matches.id = Attendance.match_id WHERE Attendance.Attendance is NULL OR Attendance.Attendance <= 40000")
        conn.execute("CREATE VIEW PopularTeams AS SELECT teams.id as team_id FROM Teams EXCEPT SELECT team_id FROM BadTeams")
        conn.execute("CREATE VIEW PlayerGoals AS SELECT players.id as player_id, players.team_id, COALESCE(sum(scores.goals),0) as goals FROM players LEFT JOIN scores ON scores.player_id=players.id GROUP BY players.id, players.team_id")
        conn.execute("CREATE VIEW PlayersMatches AS select players.id as player_id, COALESCE(scores.match_id, 0) as match_id, COALESCE(scores.goals,0) as goals from players left join scores on scores.player_id=players.id")
        conn.execute("CREATE VIEW SameMatchesByPlayer AS select A.player_id, count(A.player_id), B.player_id as b_player_id from PlayersMatches A, PlayersMatches B where A.match_id=B.match_id group by A.player_id, B.player_id")
        conn.execute("CREATE VIEW NoMatchesPlayers AS select A.player_id, count(A.player_id), B.player_id as b_player_id from PlayersMatches A, PlayersMatches B where B.match_id=0 group by A.player_id, B.player_id")
        conn.execute("CREATE VIEW AllMatchesPlayers AS select * from NoMatchesPlayers union select * from SameMatchesByPlayer")


    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DELETE FROM Teams CASCADE")
        conn.execute("DELETE FROM Matches CASCADE")
        conn.execute("DELETE FROM Players CASCADE")
        conn.execute("DELETE FROM Stadiums CASCADE")
        conn.execute("DELETE FROM Scores CASCADE")
        conn.execute("DELETE FROM Attendance CASCADE")

    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS Teams CASCADE")
        conn.execute("DROP TABLE IF EXISTS Matches CASCADE")
        conn.execute("DROP TABLE IF EXISTS Players CASCADE")
        conn.execute("DROP TABLE IF EXISTS Stadiums CASCADE")
        conn.execute("DROP TABLE IF EXISTS Scores CASCADE")
        conn.execute("DROP TABLE IF EXISTS Attendance CASCADE")
        conn.execute("DROP TABLE IF EXISTS Attendance CASCADE")
        conn.execute(
            "DROP VIEW AverageAttendance")
        conn.execute(
            "DROP VIEW TotalStadiumGoals")
        conn.execute(
            "DROP VIEW TotalStadiumGoalsIncludingZeros")
        conn.execute(
            "DROP VIEW TotalMatchGoals")
        conn.execute(
            "DROP VIEW Winners")
        conn.execute(
            "DROP VIEW ActiveTeams")
        conn.execute(
            "DROP VIEW ActiveTallTeams")
        conn.execute(
            "DROP VIEW RichTeams")
        conn.execute(
            "DROP VIEW ActiveTallRichTeams")
        conn.execute(
            "DROP VIEW AttendanceTeams")
        conn.execute(
            "DROP VIEW BadTeams")
        conn.execute(
            "DROP VIEW PopularTeams")
        conn.execute(
            "DROP VIEW PlayerGoals")
        conn.execute(
            "DROP VIEW PlayersMatches")
        conn.execute(
            "DROP VIEW SameMatchesByPlayer")
        conn.execute(
            "DROP VIEW NoMatchesPlayers")
        conn.execute(
            "DROP VIEW AllMatchesPlayers")

    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        pass
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        pass
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        pass
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        pass
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        pass
    except Exception as e:
        pass
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def addTeam(teamID: int) -> ReturnValue:
    r = ReturnValue.OK

    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Teams(id) VALUES({id})").format(id=sql.Literal(teamID))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        r = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        r = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return r


def addMatch(match: Match) -> ReturnValue:
    m_id, competition, home_id, away_id = match.getMatchID(), match.getCompetition(), match.getHomeTeamID(), match.getAwayTeamID()
    r = ReturnValue.OK

    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Matches(id, competition, home_id, away_id) VALUES({m_id}, {competition}, {home_id}, {away_id})").format(
            m_id=sql.Literal(m_id), competition=sql.Literal(competition), home_id=sql.Literal(home_id),
            away_id=sql.Literal(away_id))
        rows_effected, _ = conn.execute(query)

    except DatabaseException.ConnectionInvalid as e:
        r = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        r = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return r


def getMatchProfile(matchID: int) -> Match:
    conn = None
    m = Match.badMatch()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT * FROM Matches WHERE id = {id}").format(
            id=sql.Literal(matchID))
        rows_effected, res = conn.execute(query)
        m = Match(res.rows[0][0], res.rows[0][1], res.rows[0][2], res.rows[0][3])
    except DatabaseException.ConnectionInvalid as e:
        return m
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return m
    except DatabaseException.CHECK_VIOLATION as e:
        return m
    except DatabaseException.UNIQUE_VIOLATION as e:
        return m
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return m
    except Exception as e:
        return m
    finally:
        conn.close()
        return m

    pass


def deleteMatch(match: Match) -> ReturnValue:
    conn = None
    r = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "DELETE FROM Matches WHERE id = {id}").format(
            id=sql.Literal(match.getMatchID()))
        rows_effected, res = conn.execute(query)
        if rows_effected == 0:
            r = ReturnValue.NOT_EXISTS


    except DatabaseException.ConnectionInvalid as e:
        r = ReturnValue.ERROR

    except DatabaseException.NOT_NULL_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS

    except DatabaseException.CHECK_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS

    except DatabaseException.UNIQUE_VIOLATION as e:
        r = ReturnValue.ALREADY_EXISTS

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS

    except Exception as e:

        print(e)
    finally:
        conn.close()
        return r
    pass


def addPlayer(player: Player) -> ReturnValue:
    id, team_id, age, height, foot = player.getPlayerID(), player.getTeamID(), player.getAge(), player.getHeight(), player.getFoot()
    r = ReturnValue.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Players(id, team_id, age, height, preferred_foot) VALUES({id}, {team_id}, {age}, {height}, {preferred_foot})").format(
            id=sql.Literal(id), team_id=sql.Literal(team_id), age=sql.Literal(age), height=sql.Literal(height),
            preferred_foot=sql.Literal(foot))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        r = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        r = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return r
    return r


def getPlayerProfile(playerID: int) -> Player:
    conn = None
    m = Player.badPlayer()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT * FROM Players WHERE id = {id}").format(
            id=sql.Literal(playerID))
        rows_effected, res = conn.execute(query)
        m = Player(res.rows[0][0], res.rows[0][1], res.rows[0][2], res.rows[0][3], res.rows[0][4])
    except DatabaseException.ConnectionInvalid as e:
        return m
    except DatabaseException.NOT_NULL_VIOLATION as e:
        return m
    except DatabaseException.CHECK_VIOLATION as e:
        return m
    except DatabaseException.UNIQUE_VIOLATION as e:
        return m
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return m
    except Exception as e:
        return m
    finally:
        conn.close()
        return m
    return m
    pass



def deletePlayer(player: Player) -> ReturnValue:
    conn = None
    r = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "DELETE FROM Players WHERE id = {id}").format(
            id=sql.Literal(player.getPlayerID()))
        rows_effected, res = conn.execute(query)
        if rows_effected == 0:
            r = ReturnValue.NOT_EXISTS

    except DatabaseException.ConnectionInvalid as e:
        print(e)
        r = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        r = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        r = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        r = ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        r = ReturnValue.ERROR
    except Exception as e:
        print(e)
        r = ReturnValue.ERROR
    finally:
        conn.close()
        return r
    pass


def addStadium(stadium: Stadium) -> ReturnValue:
    id, capacity, belong_to = stadium.getStadiumID(), stadium.getCapacity(), stadium.getBelongsTo()
    conn = None
    r = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Stadiums(id, capacity, belong_to) VALUES({id}, {capacity}, {belong_to})").format(
            id=sql.Literal(id), capacity=sql.Literal(capacity), belong_to=sql.Literal(belong_to))
        rows_effected, _ = conn.execute(query)
        if rows_effected == 0:
            r = ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        r = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        r = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return r



def getStadiumProfile(stadiumID: int) -> Stadium:
    conn = None
    m = Stadium.badStadium()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT * FROM Stadiums WHERE id = {id}").format(
            id=sql.Literal(stadiumID))
        rows_effected, res = conn.execute(query)
        m = Stadium(res.rows[0][0], res.rows[0][1], res.rows[0][2])
    except DatabaseException.ConnectionInvalid as e:
        pass
    except DatabaseException.NOT_NULL_VIOLATION as e:
        pass
    except DatabaseException.CHECK_VIOLATION as e:
        pass
    except DatabaseException.UNIQUE_VIOLATION as e:
        pass
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        pass
    except Exception as e:
        pass
    finally:
        conn.close()
        return m
    return m
    pass


def deleteStadium(stadium: Stadium) -> ReturnValue:
    conn = None
    r = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "DELETE FROM Stadiums WHERE id = {id}").format(
            id=sql.Literal(stadium.getStadiumID()))
        rows_effected, res = conn.execute(query)
        if rows_effected == 0:
            r = ReturnValue.NOT_EXISTS

    except DatabaseException.ConnectionInvalid as e:
        print(e)
        r = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        r = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        r = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        r = ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        r = ReturnValue.ERROR
    except Exception as e:
        print(e)
        r = ReturnValue.ERROR
    finally:
        conn.close()
        return r
    pass



def playerScoredInMatch(match: Match, player: Player, amount: int) -> ReturnValue:

    r = ReturnValue.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Scores(match_id, player_id, goals) VALUES({match_id}, {player_id}, {goals})").format(
            match_id=sql.Literal(match.getMatchID()), player_id=sql.Literal(player.getPlayerID()), goals=sql.Literal(amount))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        r = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        r = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        r = ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return r
    return r

    pass


def playerDidntScoreInMatch(match: Match, player: Player) -> ReturnValue:
    conn = None
    r = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "DELETE FROM Scores WHERE match_id = {match_id} AND player_id = {player_id}").format(
            match_id=sql.Literal(match.getMatchID()), player_id=sql.Literal(player.getPlayerID()))
        rows_effected, res = conn.execute(query)
        if rows_effected == 0:
            r = ReturnValue.NOT_EXISTS

    except DatabaseException.ConnectionInvalid as e:
        print(e)
        r = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        r = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        r = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        r = ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        r = ReturnValue.ERROR
    except Exception as e:
        print(e)
        r = ReturnValue.ERROR
    finally:
        conn.close()
        return r
    pass


def matchInStadium(match: Match, stadium: Stadium, attendance: int) -> ReturnValue:

    r = ReturnValue.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "INSERT INTO Attendance(match_id, stadium_id, attendance) VALUES({match_id}, {stadium_id}, {am})").format(
            match_id=sql.Literal(match.getMatchID()), stadium_id=sql.Literal(stadium.getStadiumID()), am=sql.Literal(attendance))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        r = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        r = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        r = ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        r = ReturnValue.NOT_EXISTS
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return r
    return r

    pass
    pass


def matchNotInStadium(match: Match, stadium: Stadium) -> ReturnValue:
    conn = None
    r = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "DELETE FROM Attendance WHERE match_id = {match_id} AND stadium_id = {stadium_id}").format(
            match_id=sql.Literal(match.getMatchID()), stadium_id=sql.Literal(stadium.getStadiumID()))
        rows_effected, res = conn.execute(query)
        if rows_effected == 0:
            r = ReturnValue.NOT_EXISTS

    except DatabaseException.ConnectionInvalid as e:
        print(e)
        r = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        r = ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        r = ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        r = ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        r = ReturnValue.ERROR
    except Exception as e:
        print(e)
        r = ReturnValue.ERROR
    finally:
        conn.close()
        return r
    pass


def averageAttendanceInStadium(stadiumID: int) -> float:
    conn = None
    m = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT avg FROM AverageAttendance WHERE stadium_id = {stadiumID}").format(
            stadiumID=sql.Literal(stadiumID))
        rows_effected, res = conn.execute(query)
        if rows_effected == 0:
            m = 0
        else:
            m = res.rows[0][0]
    except DatabaseException.ConnectionInvalid as e:
        m=-1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        m=-1
    except DatabaseException.CHECK_VIOLATION as e:
        m=-1
    except DatabaseException.UNIQUE_VIOLATION as e:
        m=-1
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        m=-1
    except Exception as e:
        m=-1
    finally:
        conn.close()
        return m
    return m
    pass


def stadiumTotalGoals(stadiumID: int) -> int:
    conn = None
    m = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT total_goals FROM TotalStadiumGoalsIncludingZeros WHERE stad_id = {stadiumID}").format(
            stadiumID=sql.Literal(stadiumID))
        rows_effected, res = conn.execute(query)
        if rows_effected == 0:
            m = 0
        else:
            m = res.rows[0][0]
    except DatabaseException.ConnectionInvalid as e:
        m=-1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        m=-1
    except DatabaseException.CHECK_VIOLATION as e:
        m=-1
    except DatabaseException.UNIQUE_VIOLATION as e:
        m=-1
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        m=-1
    except Exception as e:
        m=-1
    finally:
        conn.close()
        return m
    return m
    pass



def playerIsWinner(playerID: int, matchID: int) -> bool:
    conn = None
    m = False
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT * FROM Winners WHERE player_id={pID} AND match_id = {mID}").format(pID=sql.Literal(playerID), mID=sql.Literal(matchID))
        rows_effected, res = conn.execute(query)
        if(rows_effected>0):
            m = True

    except DatabaseException.ConnectionInvalid as e:
        m = False
    except DatabaseException.NOT_NULL_VIOLATION as e:
        m = False
    except DatabaseException.CHECK_VIOLATION as e:
        m = False
    except DatabaseException.UNIQUE_VIOLATION as e:
        m = False
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        m = False
    except Exception as e:
        m = False
    finally:
        conn.close()
        return m


def getActiveTallTeams() -> List[int]:
    conn = None
    m = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT team_id FROM ActiveTallTeams WHERE total>=2 ORDER BY team_id DESC LIMIT 5")
        rows_effected, res = conn.execute(query)
        for i in range(len(res.rows)):
            m.append(res.rows[i][0])

    except DatabaseException.ConnectionInvalid as e:
        m = []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        m = []
    except DatabaseException.CHECK_VIOLATION as e:
        m = []
    except DatabaseException.UNIQUE_VIOLATION as e:
        m = []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        m = []
    except Exception as e:
        m = []
    finally:
        conn.close()
        return m


def getActiveTallRichTeams() -> List[int]:
    conn = None
    m = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT team_id FROM ActiveTallRichTeams WHERE total>=2 ORDER BY team_id LIMIT 5")
        rows_effected, res = conn.execute(query)
        for i in range(len(res.rows)):
            m.append(res.rows[i][0])

    except DatabaseException.ConnectionInvalid as e:
        m = []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        m = []
    except DatabaseException.CHECK_VIOLATION as e:
        m = []
    except DatabaseException.UNIQUE_VIOLATION as e:
        m = []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        m = []
    except Exception as e:
        m = []
    finally:
        conn.close()
        return m




def popularTeams() -> List[int]:
    conn = None
    m = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT team_id FROM PopularTeams ORDER BY team_id DESC LIMIT 10")
        rows_effected, res = conn.execute(query)
        for i in range(len(res.rows)):
            m.append(res.rows[i][0])

    except DatabaseException.ConnectionInvalid as e:
        m = []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        m = []
    except DatabaseException.CHECK_VIOLATION as e:
        m = []
    except DatabaseException.UNIQUE_VIOLATION as e:
        m = []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        m = []
    except Exception as e:
        m = []
    finally:
        conn.close()
        return m



# select s_id from totalstadiumgoals order by total_goals desc limit 5

def getMostAttractiveStadiums() -> List[int]:
    conn = None
    m = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT stad_id from TotalStadiumGoalsIncludingZeros ORDER BY total_goals DESC, stad_id ASC")
        rows_effected, res = conn.execute(query)
        for i in range(len(res.rows)):
            m.append(res.rows[i][0])

    except DatabaseException.ConnectionInvalid as e:
        m = []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        m = []
    except DatabaseException.CHECK_VIOLATION as e:
        m = []
    except DatabaseException.UNIQUE_VIOLATION as e:
        m = []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        m = []
    except Exception as e:
        m = []
    finally:
        conn.close()
        return m




def mostGoalsForTeam(teamID: int) -> List[int]:
    conn = None
    m = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "SELECT player_id from PlayerGoals WHERE team_id={tID} ORDER BY goals DESC, player_id DESC LIMIT 5").format(tID=sql.Literal(teamID))
        rows_effected, res = conn.execute(query)
        for i in range(len(res.rows)):
            m.append(res.rows[i][0])

    except DatabaseException.ConnectionInvalid as e:
        m = []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        m = []
    except DatabaseException.CHECK_VIOLATION as e:
        m = []
    except DatabaseException.UNIQUE_VIOLATION as e:
        m = []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        m = []
    except Exception as e:
        m = []
    finally:
        conn.close()
        return m


def getClosePlayers(playerID: int) -> List[int]:
    conn = None
    m = []
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "select player_id from AllMatchesPlayers where b_player_id={pID} and player_id <> {pID} and count >= (select count from SameMatchesByPlayer where player_id={pID} and b_player_id={pID})/2 ORDER BY player_id ASC LIMIT 10").format(
            pID=sql.Literal(playerID))
        rows_effected, res = conn.execute(query)
        for i in range(len(res.rows)):
            m.append(res.rows[i][0])

    except DatabaseException.ConnectionInvalid as e:
        m = []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        m = []
    except DatabaseException.CHECK_VIOLATION as e:
        m = []
    except DatabaseException.UNIQUE_VIOLATION as e:
        m = []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        m = []
    except Exception as e:
        m = []
    finally:
        conn.close()
        return m





