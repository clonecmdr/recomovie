from datetime import date, datetime, time, timedelta
from decimal import Decimal
#from typing import Optional, List
from typing_extensions import Annotated

from pydantic import BaseModel, Field, RootModel, WithJsonSchema
from sqlalchemy import text, select
from sqlalchemy.orm import Session, Bundle

from .core import DBFilm, DBFilmCosSim, NotFoundError


#class Model(BaseModel):
#    name: Annotated[str, Field(strict=True), WithJsonSchema({'extra': 'data'})]

# pydantic
class Film(BaseModel):
    film_id: int = Field(example=99785, description='The UID')
    title: str = Field(min_length=3, max_length=128, repr=True,
                       example='Home Alone', description="The film's title")
    year: date = Field(example='1990-01-01', description="Film's release date")
    duration: int = Field(example=104, description='The length in minutes', ge=1, lt=300)
    genres: list[str] = Field(max_length=5, #pattern=r"^[A-Za-z\-]{3,}$",
                            description='A list of one or more genres',
                            example=["Comedy", "Family"])
    rating: Decimal = Field(example=7.7, max_digits=4, decimal_places=2, le=10.0,
                            description="IMDb's rating")
    vote_count: int = Field(example=675425, description='Number of rating scores', ge=0)
    persons: list[str] = Field(max_length=50, #pattern=r"^[A-Za-z ]+$",
                            example=["Macaulay Culkin", "Raja Gosnell"],
                            description='Names of actors, directors, writers, principals, etc')
    similarity: float = Field(default=1.0, description='The cosine similarity distance')


class Search(BaseModel):
    title: str = Field(default=None, max_length=128, repr=True,
                       #pattern=r"^[A-Za-z0-9'-.,\"# ]*$",
                       examples=['Look.*!', '.*up'],
                       description="A substring of the film's title")
    earliest_date: date = Field(default=None, ge=date(1880,1,1),
                                example="1977-01-01",
                                description="Earliest released date")
    latest_date: date = Field(default=None, le=date(2030,1,1),
                            example="2026-01-01",
                            description="Latest released date")
    min_duration: int = Field(default=None, example=30,
                            description='Minimum legth in minutes', ge=1, lt=240)
    max_duration: int = Field(default=None, example=240,
                            description='Maximum legth in minutes', ge=10, lt=300)
    genre: str = Field(default=None, max_length=24, repr=True,
                       #pattern=r"^[A-Za-z -'.]{3,12}$",
                       description="A substring match for genre")
    genres: list[str] = Field(default=None, max_length=5,
                            example=["Drama", "Sci-Fi"],
                            description='A list of any genre constraints')
    min_rating: Decimal = Field(default=None, example=5.5,
                            max_digits=4, decimal_places=2,
                            ge=0.0, le=10.0,
                            description="A minimum rating")
    max_rating: Decimal = Field(default=None, example=9.9, 
                            max_digits=4, decimal_places=2, le=10.0,
                            description="The highest rating")
    vote_count: int = Field(default=None, example=100,
                            description='Minimum number of ratings', ge=0)
    person: str = Field(default=None, max_length=32, repr=True,
                        examples=['Di.*Capri', 'John Williams'],
                        description="A substring match for a person's name")
    persons: list[str] = Field(default=None,  max_length=5,
                            example=["Jennifer Lawrence", "Meryl Streep"],
                            description='Names of any actors, directors, writers, principals, etc')
    # Field(examples=['']) does appear in the Swagger UI
    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "title": "wars.*i",
                    "earliest_date": "1970-01-01",
                    "latest_date": "2020-01-01",
                    "min_duration": 60,
                    "max_duration": 240,
                    "genre": 'Adventure',
                    "genres": ["Sci-Fi", "Action"],
                    "min_rating": 3.2,
                    "max_rating": 10.0,
                    "vote_count": 300,
                    "person": 'Lucas',
                    "persons": ["George Lucas", "John Williams"]
                },
                {
                    "earliest_date": "1970-01-01",
                    "latest_date": "2027-01-01",
                    "max_duration": 240,
                    "max_rating": 10,
                    "min_duration": 60,
                    "min_rating": 3.2,
                    "person": "John",
                    "title": "wars.*",
                    "vote_count": 300
                }
            ]
        }
    }


def db_get_film(film_id: int, session: Session) -> DBFilm:
    result = session.query(DBFilm).filter(DBFilm.film_id == film_id).first()
    if result is None:
        raise NotFoundError(f"Film with id {film_id} not found.")
    return result


def db_similar_films(film_id: int, threshold: float, session: Session) -> list[DBFilm]:
    try:
        db_film = db_get_film(film_id, session)
    except NotFoundError as e:
        e.add_note('Looking for similar films')
        raise

    stmt = text(f'''
          SELECT cs.similarity AS similarity, f.*
            FROM films_cos_sim AS cs
            JOIN films AS f ON cs.other_id == f.film_id
           WHERE cs.film_id == :id
             AND cs.similarity >= :cos
        ORDER BY cs.similarity DESC
           LIMIT 10
    ''')

    results = session.execute(stmt, {'id': film_id, 'cos': threshold})
    #results.columns(1)
    #rows = results.partitions(10)
    rows = results.fetchall()
    if len(rows) < 1:
        raise NotFoundError("Try lowering the threshold value")

    return rows


def db_search_films(search: Search, session: Session) -> list[DBFilm]:
    """
    """
    # build a WHERER clause (filter) to search the films table
    constraints = []

    if search.title:
        constraints.append(f"regexp_matches(title, '(?i){search.title}')")

    if search.earliest_date:
        constraints.append(f"year >= '{search.earliest_date}'")

    if search.latest_date:
        constraints.append(f"year <= '{search.latest_date}'")

    if search.person:
        #constraints.append(f"list_filter(persons, x -> regexp_matches(x, '(?i){search.person}'))")
        constraints.append(f"regexp_matches( array_to_string(persons, ','), '(?i){search.person}')")

    if search.persons:
        constraints.append(f"list_has_all(persons, {[*search.persons,]})")

    if search.genre:
        constraints.append(f"regexp_matches( array_to_string(genres, ','), '(?i){search.genre}')")

    if search.genres:
        constraints.append(f"list_has_all(genres, {[*search.genres,]})")

    if search.min_rating:
        constraints.append(f"rating >= '{search.min_rating}'")

    if search.max_rating:
        constraints.append(f"rating <= '{search.max_rating}'")

    if search.vote_count:
        constraints.append(f"vote_count >= '{search.vote_count}'")

    if search.min_duration:
        constraints.append(f"duration >= '{search.min_duration}'")

    if search.max_duration:
        constraints.append(f"duration <= '{search.max_duration}'")

    if constraints:
        where_clause = "WHERE " + (" AND ".join(constraints))
    else:
        return Film(film_id = '99785' )

    # create the query with all relevant parts
    stmt = text(f"SELECT * FROM films {where_clause} ORDER BY title LIMIT 100")
    results = session.execute(stmt).all()

    if results is None:
        raise NotFoundError("No matching films found; try using fewer search criteria.")

    return results
