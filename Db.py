from sqlalchemy import create_engine, Column, Integer, Text
from sqlalchemy.orm import declarative_base, sessionmaker

DATABASE_URL = "postgresql://newsuser:news123@postgres:5432/newsdb"

Base = declarative_base()
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)

class Article(Base):
    __tablename__ = "Articles"

    id = Column(Integer, primary_key=True)
    title = Column(Text)
    description = Column(Text)

Base.metadata.create_all(bind=engine)
