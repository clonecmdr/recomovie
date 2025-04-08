from fastapi.testclient import TestClient
from sqlalchemy import StaticPool, create_engine
from sqlalchemy.orm import sessionmaker
from main import app


engine = create_engine(
    #'duckdb:///:memory:',
    'duckdb:///duckdb/films.duckdb',
    connect_args = {
        'read_only': True,
        'config': {
            'memory_limit': '1gb'
        },
    },
    poolclass=StaticPool,
    echo=True
)

test_session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

client = TestClient(app)

def override_get_db():
    database = test_session()
    try:
        yield database
    finally:
        database.close()

# override the original get_db() with the above func, 
# which uses a test database session
#app.dependency_overrides = {'get_db': "override_get_db"}
app.dependency_overrides['get_db'] = override_get_db


def test_read_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == "Server is running."


def test_get_film():
    response = client.get("/film/99785")
    assert response.status_code == 200, response.text
    result = response.json()
    assert result['title'] == "Home Alone"
    assert result['film_id'] == 99785


def test_similar_films():
    response = client.get("/film/99785/similar/0.3")
    assert response.status_code == 200, response.text
    result = response.json()
    assert result[0]['title'] == "Home Alone 2: Lost in New York"
    assert result[0]['film_id'] == 104431
    assert result[1]['title'] == "Only the Lonely"
    assert result[1]['film_id'] == 102598


def test_search_films():
    response = client.post(
        "/film/search/", 
        json={
            "earliest_date": "1970-01-01",
            "genre": "Adventure",
            "genres": [
                "Sci-Fi",
                "Action"
            ],
            "latest_date": "2020-01-01",
            "max_duration": 240,
            "max_rating": 10,
            "min_duration": 60,
            "min_rating": 3.2,
            "person": "Lucas",
            "persons": [
                "George Lucas",
                "John Williams"
            ],
            "title": "wars.*i",
            "vote_count": 300
            }
    )
    assert response.status_code == 200, response.text
    result = response.json()
    assert result[0]["title"] == "Star Wars: Episode VII - The Force Awakens"
    assert result[0]['film_id'] == 2488496


def setup():
    metadata_obj = MetaData()
    tab_films_table = Table("films", metadata_obj, autoload_with=engine)
    tab_films_cos_sim = Table("films_cos_sim", metadata_obj, autoload_with=engine)
    # Emit the DDL to create the two tables by calling create_all()
    #metadata_obj.create_all(engine)

def teardown():
    pass
    #Base.metadata.drop_all(bind=engine)
