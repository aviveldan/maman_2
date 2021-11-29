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
        conn.execute("CREATE TABLE Teams(id integer UNIQUE)")
        conn.execute("CREATE TABLE Matches(id integer UNIQUE NOT NULL, competition TEXT NOT NULL, home_id integer NOT NULL, away_id integer NOT NULL )")
        conn.execute("CREATE TABLE Players(id integer UNIQUE NOT NULL, team_id integer UNIQUE NOT NULL, age integer NOT NULL, height integer NOT NULL, preferred_foot TEXT NOT NULL)")
        conn.execute("CREATE TABLE Stadiums(id integer UNIQUE NOT NULL, capacity integer NOT NULL, belong_to integer UNIQUE)")



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
    if teamID <= 0:
        return ReturnValue.BAD_PARAMS


    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Teams(id) VALUES({id})").format(id=sql.Literal(teamID))
        rows_effected, _ = conn.execute(query)
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
        conn.close()
        return ReturnValue.OK


def addMatch(match: Match) -> ReturnValue:
    id, competition, home_id, away_id  = match.getMatchID(), match.getCompetition(), match.getHomeTeamID(), match.getAwayTeamID()
    if id <= 0 or home_id <= 0 or away_id <=0 or away_id == home_id:
        return ReturnValue.BAD_PARAMS
    if competition not in ['International', 'Domestic']:
        return ReturnValue.BAD_PARAMS

    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Matches(id, competition, home_id, away_id) VALUES({id}, {competition}, {home_id}, {away_id})").format(id=sql.Literal(id), competition=sql.Literal(competition), home_id=sql.Literal(home_id), away_id=sql.Literal(away_id))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return ReturnValue.OK



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


def addPlayer(player: Player) -> ReturnValue:
    pass


def getPlayerProfile(playerID: int) -> Player:
    pass


def deletePlayer(player: Player) -> ReturnValue:
    pass


def addStadium(stadium: Stadium) -> ReturnValue:
    pass


def getStadiumProfile(stadiumID: int) -> Stadium:
    pass


def deleteStadium(stadium: Stadium) -> ReturnValue:
    pass


def playerScoredInMatch(match: Match, player: Player, amount: int) -> ReturnValue:
    pass


def playerDidntScoreInMatch(match: Match, player: Player) -> ReturnValue:
    pass


def matchInStadium(match: Match, stadium: Stadium, attendance: int) -> ReturnValue:
    pass


def matchNotInStadium(match: Match, stadium: Stadium) -> ReturnValue:
    pass


def averageAttendanceInStadium(stadiumID: int) -> float:
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



if __name__ == '__main__':
    # print("0. Creating all tables")
    # createTables()
    deleteMatch(Match(1,'Domestic',3,9))
