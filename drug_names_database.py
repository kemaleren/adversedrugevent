from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker


class Base(object):
    def __repr__(self):
        name = self.__class__.__name__
        result = ", ".join(sorted("{}={}".format(a, v)
                                  for a, v in self.__dict__.iteritems()
                                  if a != '_sa_instance_state'))
        return "{}({})".format(name, result)


class Demographics(Base):
    pass


class Drugs(Base):
    pass


class Indications(Base):
    pass


class Outcomes(Base):
    pass


class Reactions(Base):
    pass


def load_session(username, password, db_path):
    full_path = 'mysql://{}:{}@{}/drugevent'.format(
        username, password, db_path)
    print full_path
    engine = create_engine(full_path, pool_recycle=3600)

    metadata = MetaData(engine)

    # auto map existing tables
    demo_table = Table('aers_ascii_demo', metadata, autoload=True)
    mapper(Demographics, demo_table)

    drug_table = Table('aers_ascii_drug', metadata, autoload=True)
    mapper(Drugs, drug_table)

    indi_table = Table('aers_ascii_indi', metadata, autoload=True)
    mapper(Indications, indi_table)

    outc_table = Table('aers_ascii_outc', metadata, autoload=True)
    mapper(Outcomes, outc_table)

    reac_table = Table('aers_ascii_reac', metadata, autoload=True)
    mapper(Reactions, reac_table)

    Session = sessionmaker(bind=engine)
    session = Session()
    return session
