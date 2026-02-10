import sys
import os
from sqlalchemy import Column, Integer, String, create_engine, MetaData, Table
from sqlalchemy.orm import declarative_base, Session

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db import engine
from src.models import Base

class LookupState(Base):
    __tablename__ = 'lookup_state'
    code = Column(String, primary_key=True)
    name = Column(String)

class LookupSex(Base):
    __tablename__ = 'lookup_sex'
    code = Column(String, primary_key=True)
    description = Column(String)

class LookupRace(Base):
    __tablename__ = 'lookup_race'
    code = Column(String, primary_key=True)
    description = Column(String)

def create_lookups():
    print("Creating lookup tables...")
    Base.metadata.create_all(bind=engine)
    
    session = Session(bind=engine)
    
    # State Codes (Derived from PDF analysis)
    # Using a subset of common codes found in the PDF output for demonstration
    states = [
        {"code": "01", "name": "AL"}, {"code": "02", "name": "AK"}, {"code": "03", "name": "AZ"},
        {"code": "04", "name": "AR"}, {"code": "05", "name": "CA"}, {"code": "06", "name": "CO"},
        {"code": "07", "name": "CT"}, {"code": "08", "name": "DE"}, {"code": "09", "name": "DC"},
        {"code": "10", "name": "FL"}, {"code": "11", "name": "GA"}, {"code": "12", "name": "HI"},
        {"code": "13", "name": "ID"}, {"code": "14", "name": "IL"}, {"code": "15", "name": "IN"},
        {"code": "16", "name": "IA"}, {"code": "17", "name": "KS"}, {"code": "18", "name": "KY"},
        {"code": "19", "name": "LA"}, {"code": "20", "name": "ME"}, {"code": "21", "name": "MD"},
        {"code": "22", "name": "MA"}, {"code": "23", "name": "MI"}, {"code": "24", "name": "MN"},
        {"code": "25", "name": "MS"}, {"code": "26", "name": "MO"}, {"code": "27", "name": "MT"},
        {"code": "28", "name": "NE"}, {"code": "29", "name": "NV"}, {"code": "30", "name": "NH"},
        {"code": "31", "name": "NJ"}, {"code": "32", "name": "NM"}, {"code": "33", "name": "NY"},
        {"code": "34", "name": "NC"}, {"code": "35", "name": "ND"}, {"code": "36", "name": "OH"},
        {"code": "37", "name": "OK"}, {"code": "38", "name": "OR"}, {"code": "39", "name": "PA"},
        {"code": "41", "name": "RI"}, {"code": "42", "name": "SC"}, {"code": "43", "name": "SD"},
        {"code": "44", "name": "TN"}, {"code": "45", "name": "TX"}, {"code": "46", "name": "UT"},
        {"code": "47", "name": "VT"}, {"code": "49", "name": "VA"}, {"code": "50", "name": "WA"},
        {"code": "51", "name": "WV"}, {"code": "52", "name": "WI"}, {"code": "53", "name": "WY"},
        {"code": "54", "name": "AZ"},
    ]
    
    # Sex Codes (Standard CMS values)
    sexes = [
        {"code": "1", "description": "Male"},
        {"code": "2", "description": "Female"}
    ]
    
    # Race Codes (Standard CMS values)
    races = [
        {"code": "1", "description": "White"},
        {"code": "2", "description": "Black"},
        {"code": "3", "description": "Other"},
        {"code": "4", "description": "Asian/Pacific Islander"},
        {"code": "5", "description": "Hispanic"},
        {"code": "6", "description": "North American Native"}
    ]
    
    try:
        # Upsert logic or simple insert (ignoring duplicates for now)
        for s in states:
             session.merge(LookupState(**s))
        for x in sexes:
             session.merge(LookupSex(**x))
        for r in races:
             session.merge(LookupRace(**r))
             
        session.commit()
        print("Lookup tables populated successfully.")
    except Exception as e:
        session.rollback()
        print(f"Error populating lookups: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    create_lookups()
