# -*- coding: utf-8 -*-
#
# Author:   James Gardiner
#           www.jgardiner.co.uk
#           james.gardiner@nesta.org.uk
# Date:     2016
# License:  Apache License Version 2.0
#           http://www.apache.org/licenses/LICENSE-2.0

import fitbit
import json
import sqlalchemy
import time

from datetime import timedelta, date

from sqlalchemy import Column, exc, Integer, Date, desc
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class HeartRate(Base):
    """Define the HeartRate object schema"""
    __tablename__ = 'heart_rate'
    __table_args__ = {"schema": "fitbit"}
    date = Column(Date, primary_key=True)
    r_hr = Column(Integer)
    hr = Column(JSONB)

# Standard SQLAlchemy connection configs
connection_string = 'postgresql://james:@localhost:5432/fitbit'
db = sqlalchemy.create_engine(connection_string)
engine = db.connect()
meta = sqlalchemy.MetaData(engine, schema="fitbit")

session_factory = sessionmaker(engine)      # New SessionFactory object
Base.metadata.create_all(engine)            # Create a table if doesn't exist


def rate_limited(maxPerSecond):
    """Create a rate_limited decorator that limits function calls to x per second"""
    minInterval = 1.0 / float(maxPerSecond)

    def decorate(func):
        lastTimeCalled = [0.0]

        def rateLimitedFunction(*args, **kargs):
            elapsed = time.clock() - lastTimeCalled[0]
            leftToWait = minInterval - elapsed
            if leftToWait > 0:
                time.sleep(leftToWait)
            ret = func(*args, **kargs)
            lastTimeCalled[0] = time.clock()
            return ret
        return rateLimitedFunction
    return decorate


def add_hr_to_db(data):
    """Commits Fitbit intraday heart rate data to a database.
    Uses
    """
    session = session_factory()  # A new session
    date = data['activities-heart'][0].get('dateTime')
    hr = data.get('activities-heart-intraday').get('dataset')
    r_hr = data.get('activities-heart')[0].get('value').get('restingHeartRate')
    obj = HeartRate(
        date=date,
        hr=hr,
        r_hr=r_hr)

    session.add(obj)
    session.commit()


def set_client(tokens_file=None):

    if tokens_file is None:
        tokens_file = 'tokens.json'

    tokens = get_tokens(in_file=tokens_file)

    client = fitbit.Fitbit(
        tokens['CLIENT_ID'],
        tokens['CLIENT_SECRET'],
        oauth2=True,
        access_token=tokens['ACCESS_TOKEN'],
        refresh_token=tokens['REFRESH_TOKEN']
    )

    try:
        client.sleep()
    except:
        client = update_tokens(client, tokens)

    return client


def get_tokens(in_file='tokens.json'):
    """Returns the client and CLIENT_SECRET values from
    a json token file."""
    with open(in_file) as f:
        data = json.load(f)

    return {
        'CLIENT_ID': data['CLIENT_ID'],
        'CLIENT_SECRET': data['CLIENT_SECRET'],
        'REFRESH_TOKEN': data['REFRESH_TOKEN'],
        'ACCESS_TOKEN': data['ACCESS_TOKEN']
    }


def update_tokens(authd_client, tokens, outfile='tokens.json'):
    """Get new access token using refresh token and update the tokens
    json file"""
    params = authd_client.client.refresh_token()
    tokens["REFRESH_TOKEN"] = params["refresh_token"]
    tokens["ACCESS_TOKEN"] = params["access_token"]

    with open(outfile, 'w') as out:
        json.dump(tokens, out)

    return authd_client


def daterange(start_date, end_date):
    """Returns a generator object of dates. End date inclusive"""
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)


def get_most_recent_date(session, obj,):
    """Queries a database using current engine and returns the
    most recent date from a 'date' column. If none exists, defaults
    to 25th December, 2015"""
    try:
        d = session.query(obj).order_by(desc(obj.date)).first()
        return d.date
    except AttributeError:
        return date(2015, 12, 26)


@rate_limited(1)
def get_intra_hr(client, start_date=None, end_date=None, start_time=None, end_time=None):
    """Returns a list of fitbit intraday heart rate data entries by day.
    Default date range is yesterday."""
    val = []
    if all([start_date, end_date]):                         # If true then use custom date range
        dr = daterange(start_date, end_date)
        val = [get_daily_data(client, x) for x in dr]
    else:
        # If false then just get yesterday's data
        dr = date.today() - timedelta(days=1)
        dr = dr.strftime('%Y-%m-%d')
        val.append(get_daily_data(client, dr))

    # Return a list where each entry is a day
    return val


def get_daily_data(client, base_date=None, detail_level='1sec', start_time=None, end_time=None):
    """Returns a single day of intraday heart rate data"""
    return client.intraday_time_series(
        'activities/heart',
        base_date=base_date,
        detail_level='1sec',
        start_time=start_time,
        end_time=end_time)


def main():
    client = set_client()
    start_date = get_most_recent_date(session_factory(), HeartRate)
    end_date = date.today() - timedelta(days=1)
    x = get_intra_hr(client, start_date=start_date, end_date=end_date)
    for day in x:
        try:
            add_hr_to_db(day)
        except exc.IntegrityError as e:
            print("Integrity error raised.\n{}".format(e))

if __name__ == "__main__":
    main()
