# -*- coding: utf-8 -*-
"""
Created on Tue Jul 24 23:31:20 2018

@author: krishna.i
"""

from numpy import genfromtxt
from time import time
from datetime import datetime
from sqlalchemy import Column, Integer, Float, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def Load_Data(file_name):
    data = genfromtxt(file_name, delimiter=',', skip_header=1, converters={0: lambda s: str(s)})
    return data.tolist()

Base = declarative_base()

class Adult_History(Base):
    #Tell SQLAlchemy what the table name is and if there's any table-specific arguments it should know about
    __tablename__ = 'Adult_History'
    __table_args__ = {'sqlite_autoincrement': True}
    #tell SQLAlchemy the name of column and its attributes:
    id = Column(Integer, rimary_key=True, nullable=False)  [CAPITAL_GAIN] INTEGER,[CAPITAL_LOSS] Text,[HOURS_per_Week] Text, [NATIVE_COUNTRY] Text, [SALabove50K] Text)
    
    
    id = Column(Integer, primary_key=True, nullable=False) 
    AGE = Column(Integer)
    WORKCLASS = Column(Text)
    final_weight = Column(Float)
    EDUCATION = Column(Text)
    EDUCATION_NUM = Column(Text)
    MARITIAL_STATUS = Column(Text)
    OCCUPATION = Column(Text)
    RELATIONSHIP = Column(Text)
    RACE = Column(Text)
    SEX = Column(Text)
    CAPITAL_GAIN = Column(Integer)

if __name__ == "__main__":
    t = time()

    #Create the database
    engine = create_engine('sqlite:///csv_test.db')
    Base.metadata.create_all(engine)

    #Create the session
    session = sessionmaker()
    session.configure(bind=engine)
    s = session()

    try:
        file_name = "https://archive.ics.uci.edu/ml/machine-learning-databases/adult/adult.data" 
        data = Load_Data(file_name) 

        for i in data:
            record = Adult_History(**{
                'AGE' : i[0],
                'WORKCLASS' : i[1],
                'final_weight' : i[2],
                'EDUCATION' : i[3],
                'EDUCATION_NUM' : i[4],
                'MARTIAL_STATUS' : i[5],
                'OCCUPATION' : i[6],
                'RELATIONSHIP' : i[7],
                'RACE' : i[8],
                'SEX' : i[9],
                'CAPITAL_GAIN' : i[10],
                'CAPITAL_LOSS' : i[11],
                'HOURS_per_Week' : i[12],
                'NATIVE_COUNTRY' : i[13],
                'SALabove50K' : i[14]
            })
            s.add(record) #Add all the records

        s.commit() #Attempt to commit all the records
    except:
        s.rollback() #Rollback the changes on error
    finally:
        s.close() #Close the connection
    print "Time elapsed: " + str(time() - t) + " s." #0.091s