from pyais.stream import UDPReceiver
from sqlalchemy import create_engine, Column, Integer, DateTime, Boolean, SmallInteger, String
from sqlalchemy.orm import sessionmaker, declarative_base
import datetime
import binascii

# Define the data model
Base = declarative_base()

class Messages(Base):
    __tablename__ = 'messages'
    id = Column(Integer, primary_key=True)
    received_at = Column(DateTime, default=datetime.datetime.utcnow)
    mmsi = Column(Integer, nullable=False)
    seqno = Column(SmallInteger, nullable=False)
    dest_mmsi = Column(Integer, nullable=False)
    retransmit = Column(Boolean, nullable=False)
    dac = Column(SmallInteger, nullable=False)
    fid = Column(SmallInteger, nullable=False)
    data = Column(String, nullable=False)
    repeat = Column(SmallInteger, nullable=False)

engine = create_engine('sqlite:///messages.db')
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

for msg in UDPReceiver('192.168.1.89', 4444):
    decoded_message = msg.decode()
    ais_content = decoded_message
    if ais_content.msg_type == 6:
        print(ais_content)
        new_record = Messages(
            mmsi = ais_content.mmsi, 
            seqno = ais_content.seqno, 
            dest_mmsi = ais_content.dest_mmsi,
            retransmit = ais_content.retransmit,
            dac = ais_content.dac,
            fid = ais_content.fid,
            data = binascii.hexlify(ais_content.data).decode('utf-8'),
            repeat = ais_content.repeat
        )
        session.add(new_record)
        session.commit()