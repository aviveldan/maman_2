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
            "CREATE TABLE Matches(id integer PRIMARY KEY NOT NULL CHECK(id>0), competition TEXT NOT NULL CHECK(competition = 'International' OR competition = 'Domestic' ), home_id integer NOT NULL CHECK(home_id>0), away_id integer NOT NULL CHECK(away_id>0), FOREIGN KEY (home_id) REFERENCES Teams(id) ON DELETE CASCADE, FOREIGN KEY (away_id) REFERENCES Teams(id) ON DELETE CASCADE)")
        conn.execute(
            "CREATE TABLE Players(id integer PRIMARY KEY NOT NULL CHECK(id>0), team_id integer NOT NULL CHECK(team_id>0), age integer NOT NULL, height integer NOT NULL, preferred_foot TEXT NOT NULL CHECK(preferred_foot = 'Left' OR preferred_foot = 'Right'),FOREIGN KEY (team_id) REFERENCES Teams(id) ON DELETE CASCADE)")
        conn.execute(
            "CREATE TABLE Stadiums(id integer PRIMARY KEY NOT NULL CHECK(id>0), capacity integer NOT NULL CHECK(capacity>0), belong_to integer UNIQUE, FOREIGN KEY (belong_to) REFERENCES Teams(id))")
        conn.execute(
            "CREATE TABLE Scores(match_id integer, player_id integer, goals integer CHECK(goals>=0), FOREIGN KEY (match_id) REFERENCES Matches(id), FOREIGN KEY (player_id) REFERENCES Players(id))")
        conn.execute(
            "CREATE TABLE Attendance(match_id integer, stadium_id integer, attendance integer CHECK(attendance>=0), FOREIGN KEY (match_id) REFERENCES Matches(id) ON DELETE CASCADE, FOREIGN KEY (stadium_id) REFERENCES Players(id) ON DELETE CASCADE)")
        conn.execute("CREATE VIEW AverageAttendance AS SELECT stadium_id, AVG(attendance) FROM Attendance GROUP BY stadium_id")
        conn.execute("CREATE VIEW TotalStadiumGoals AS SELECT stadium_id, SUM(goals) FROM Scores GROUP BY stadium_id") #TODO fix it with join...
        conn.execute("CREATE VIEW TotalMatchGoals AS SELECT match_id as m_id, SUM(goals) as sum_goals FROM Scores GROUP BY match_id")
        conn.execute("CREATE VIEW Winners AS SELECT match_id, player_id, goals WHERE goals >= 0.5 *(SELECT sum(sum_goals) FROM TotalMatchGoals WHERE m_id = match_id)")

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
        r = ReturnValue.BAD_PARAMS
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
            "INSERT INTO Attendance(match_id, stadium_id, attendance) VALUES({match_id}, {stadium_id}, {count})").format(
            match_id=sql.Literal(match.getMatchID()), stadium_id=sql.Literal(stadium.getStadiumID()), count=sql.Literal(attendance))
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
            "SELECT * FROM AverageAttendance WHERE stadium_id = {stadiumID}").format(
            id=sql.Literal(stadiumID))
        rows_effected, res = conn.execute(query)
        if rows_effected == 0:
            m = 0
        else:
            m = res.rows[0][1]
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
    pass


def playerIsWinner(playerID: int, matchID: int) -> bool:
    pass


def getActiveTallTeams() -> List[int]:
    pass


def getActiveTallRichTeams() -> List[int]:
    pass


def popularTeams() -> List[int]:
    pass


def getMostAttractiveStadiums() -> List[int]:
    pass


def mostGoalsForTeam(teamID: int) -> List[int]:
    pass


def getClosePlayers(playerID: int) -> List[int]:
    pass





