import sqlite3
from sqlalchemy import create_engine


def createConnection():
    """create a db connection"""
    connection = create_engine('sqlite:///weatherData.db')
    print('Established Connection...')
    return connection


def closeConnection(connection):
    """close the db connection"""
    connection.commit()
    connection.close()
