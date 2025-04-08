from typing import Optional, List, Annotated
from datetime import date
from sqlalchemy import create_engine, MetaData, ForeignKey, Table, ARRAY, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Mapped, mapped_column
#import os


class NotFoundError(Exception):
    pass


class Base(DeclarativeBase):
    type_annotation_map = {
        list[str]: ARRAY(String),
    }


#list_str: TypeAlias = Annotated[list[str], mapped_column(index=True)]
#type list_str = Annotated[list(str), mapped_column()]


class DBFilm(Base):
    ''' '''
    __tablename__ = "films"
    film_id: Mapped[int] = mapped_column(primary_key=True, index=True)
    title: Mapped[str]
    year: Mapped[date]
    duration: Mapped[int]
    genres: Mapped[list[str]]
    rating: Mapped[float]
    vote_count: Mapped[int]
    persons: Mapped[list[str]]
    def __repr__(self) -> str:
        return f"Film(film_id={self.film_id!r}, title={self.title!r} ({self.year!r}))"


class DBFilmCosSim(Base):
    ''' '''
    __tablename__ = "films_cos_sim"
    film_id: Mapped[int] = mapped_column(primary_key=True, unique=False, nullable=False,index=True)
    other_id: Mapped[int] = mapped_column(ForeignKey("films.film_id"), nullable=False)
    similarity: Mapped[float]


engine = create_engine(
    #'duckdb:///:memory:',
    'duckdb:///duckdb/films.duckdb',
    connect_args = {
        'read_only': True,
        'config': {
            'memory_limit': '1gb'
        }
    },
    echo=True
)

metadata_obj = MetaData()
tab_films_table = Table("films", metadata_obj, autoload_with=engine)
tab_films_cos_sim = Table("films_cos_sim", metadata_obj, autoload_with=engine)
# Emit the DDL to create the two tables by calling create_all()
metadata_obj.create_all(engine)

session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base.metadata.create_all(bind=engine)

# Dependency to get the database session
def get_db():
    database = session_local()
    try:
        yield database
    finally:
        database.close()

