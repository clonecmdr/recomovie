from contextlib import asynccontextmanager
from fastapi import FastAPI, APIRouter
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from api.routers.films import router as films_router
from api.routers.limiter import limiter

test_router = APIRouter()

@asynccontextmanager
async def lifespan(app: FastAPI):
    """ Do stuff on startup
    """
    yield
    # Clean up stuff on shutdown and release the resources
    print("bye!")


app = FastAPI(lifespan=lifespan)
app.title="Films"


#include router end-points
app.include_router(films_router)
app.include_router(test_router)

# add a rate limiter, by adding an exception handler
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


@app.get("/")
def read_root():
    """  API server root URI
    """
    return "Server is running."
