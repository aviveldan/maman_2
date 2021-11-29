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
        # conn.execute("CREATE TABLE Users(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        conn.execute("CREATE TABLE Teams(id integer UNIQUE)")

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
    pass


def dropTables():
    pass


def addTeam(teamID: int) -> ReturnValue:
    pass


def addMatch(match: Match) -> ReturnValue:
    pass


def getMatchProfile(matchID: int) -> Match:
    pass


def deleteMatch(match: Match) -> ReturnValue:
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
