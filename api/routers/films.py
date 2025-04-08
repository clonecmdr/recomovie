from datetime import date
from fastapi import APIRouter, HTTPException, Request, Response, Body
from fastapi.params import Depends
from sqlalchemy.orm import Session
from pydantic import create_model

from ..db.core import NotFoundError, get_db
from ..db.films import (
    Film,
    Search,
    db_get_film,
    db_similar_films,
    db_search_films
)

from .limiter import limiter

router = APIRouter(
    prefix="/film",
)


@router.post("/search")
@limiter.limit("3/minute")
def search_films(request: Request, search: Search,
                db: Session = Depends(get_db)) -> list[Film]:
    try:
        films = db_search_films(search, db)
    except NotFoundError as e:
        raise HTTPException(status_code=404) from e

    #return [Film(**row.__dict__) for row in films]
    return films


@router.get("/{film_id}/similar/{threshold}")
@limiter.limit("5/minute")
def similar_films(request: Request, film_id: int, threshold: float=0.1,
                 db: Session = Depends(get_db)) -> list[Film]:
    try:
        films = db_similar_films(film_id, threshold, db)
    except NotFoundError as e:
        #raise RuntimeError(f'something went wrong: {type(df_films)}')
        raise HTTPException(status_code=404) from e

    return films


# Put your query arguments in this dict
search_params = {'title': (str, "Home Alone"),
                'year': (date, "1990-01-01"),
                'duration': (int, 60),
                'genres': (list[str], ("Family", "Comedy")),
                'rating': (float, 5.0),
                'vote_count': (int, 500),
                'persons': (str, "Home Alone"),
                }
# This is subclass of pydantic BaseModel
search_model = create_model("Query", **search_params)


@router.get("/{film_id}")
@limiter.limit("2/second")
def get_film(request: Request, film_id: int, db: Session=Depends(get_db)) -> Film:
    try:
        df_film = db_get_film(film_id, db)
    except NotFoundError as e:
        raise HTTPException(status_code=404) from e

    return Film(**df_film.__dict__)


# Create a route
@router.get("/find")
async def find_film(search: search_model=Depends()):
    #params_as_dict = params.dict()
    pass


#@router.post("/do_something")
#def do_something(process_id: int = Body(..., embed=True)):
#    return process_id
