# -*- coding: utf-8 -*-
"""
dal.py - Data Access Layer

Create/read/write database. Should work with most databases supported by DBAPI2.
Tested with sqlite and Postgres.
"""

__author__ = "Jonas C"


import sys, aiorun, datetime, click
from aio_databases import Database
from colorama import Fore, Back, Style

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, TIMESTAMP
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text

import polars as pl

import config
import helpers

COLUMNS = ['path','folder','size','atime','mtime','ctime']


# Create Database tables using SqlAlchemy
# =======================================

Base = declarative_base()

class File(Base):
     __tablename__ = 'file'

     id = Column(Integer, primary_key=True, autoincrement=True)
     path = Column(String)
     folder = Column(String)
     size = Column(Integer)
     atime = Column(TIMESTAMP)
     mtime = Column(TIMESTAMP)
     ctime = Column(TIMESTAMP)

     def __repr__(self):
        return f"<File (id='{self.id}', path='{self.path}', folder='{self.folder}', size='{self.size}', atime='{self.atime}', mtime='{self.mtime}', ctime='{self.ctime}')>"

class Program(Base):
     __tablename__ = 'program'
     id = Column(String(4), primary_key=True)

     def __repr__(self):
        return f"<Progam (name='{self.id}')>"

class Status(Base):
     __tablename__ = 'status'

     id      = Column(Integer, primary_key=True, autoincrement=True)
     file    = Column(Integer)
     program = Column(String(4))
     status  = Column(String(1), default='N') # (N)ew, (P)rocessing, (F)inished

     def __repr__(self):
        return f"<Status (id='{self.id}', file='{self.file}', program='{self.program}')>"


def get_engine():
    return create_engine(config.CONNECT_STR, echo=config.ECHO)

def get_session():
    engine = get_engine()
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

def add(objs):
    s = get_session()
    s.add_all(objs)
    res = s.commit()
    return res
     
   
# Add test data
# -------------
   
def add_files(session_):
    # two ways of adding rows
    session_.add(File(path='test/file0', folder='test', size=50, atime=datetime.datetime.now()))
    session_.add_all([
         File(path='test/file2', folder='test', size=100, atime=datetime.datetime.now()),
         File(path='test/file3', folder='test', size=200, atime=datetime.datetime.now()),
         File(path='test/file4', folder='test', size=300, atime=datetime.datetime.now())])
    session_.commit()
    
def add_programs(session_):
    session_.add_all([
         Program(id='pgm1'),
         Program(id='pgm2')
    ])
    session_.commit()

def add_status(session_):
    session_.add_all([
         Status(file=0, program='pgm1'),
         Status(file=1, program='pgm1'),
         Status(file=0, program='pgm2'),
         Status(file=1, program='pgm2'),
    ])
    session_.commit()
    
def show_table(session_, table_):
    print(f'\n=== show_table {table_} ===')
    for row in session_.query(table_).order_by(table_.id):
         print(row)


# Async IO based read/write operations
# ====================================

db = Database(config.CONNECT_STR)

async def write(values):
    async with db.connection():
        sql = "insert into file(id,path,folder,size,atime,mtime,ctime) values (null, ?, ?, ?, ?, ?, ?);"
        res = await db.executemany(sql, *values)
    return res

async def write_new(df):
    df_cur = await read_all()
    if df_cur.shape[0]>0:
        df_cur = df_cur[:,1:]
        df = df.join(df_cur, on='path', how='anti')
    res = await write(df.iter_rows())
    return res

async def read_all():
    async with db.connection():
        records = await db.fetchall('select * from file')
    return to_pl(records)

def to_pl(recs):
    recs = list(map(lambda x: (x[0], x[1], x[2], x[3], x[4], x[5], x[6]), recs))
    df = pl.DataFrame(recs, orient='row', schema=['id']+COLUMNS)
    return df


# Test
# ----

async def atest0():
    dt1 = datetime.datetime.now()
    dt2 = datetime.datetime.now()
    dt3 = datetime.datetime.now()
    values = [
        ('geek/geek2','geek', 1123, dt1, dt2, dt3),
        ('geek/geek3','geek', 3124, dt1, dt2, dt3),
        ('geek/geek4','geek', 4567, dt1, dt2, dt3)
    ]
    df = pl.DataFrame(values, orient='row', schema=helpers.COLUMNS)
    print(df)
    
    await write_new(df)

    df = await read_all()
    print(df)


# Main CLI
# ========
         
@click.group()
def cli():
    pass

@click.command()
def create_db():
    click.echo('Create the the database')
    
    engine = create_engine(config.CONNECT_STR, echo=config.ECHO)

    print(Fore.RED + 'WARNING: DDL such as drop table will not work with Postgres. This must be performed manually at the moment (using psql)!' + Style.RESET_ALL)
    with engine.connect() as con:
        con.execute(text('drop table if exists file;'))
        con.execute(text('drop table if exists program;'))
        con.execute(text('drop table if exists status;'))
        
    Base.metadata.create_all(engine)

@click.command()
def create_test_data():
    click.echo('Create test data')
    
    engine = create_engine(config.CONNECT_STR, echo=config.ECHO)
    
    Session = sessionmaker(bind=engine)
    session = Session()

    add_files(session)
    add_programs(session)
    add_status(session)
    
    show_table(session, File)
    show_table(session, Program)
    show_table(session, Status)

@click.command()
def test_dal():
    aiorun.run(atest0(), use_uvloop=True)     
    
cli.add_command(create_db)
cli.add_command(create_test_data)
cli.add_command(test_dal)

if __name__ == '__main__':
    cli()
