import pytest
from fastapi.testclient import TestClient
from sqlalchemy import StaticPool, create_engine
from sqlalchemy.orm import sessionmaker

from cnts_messaging_svc.app import app
from cnts_messaging_svc.models.base import Base, get_db
from cnts_messaging_svc.models.factories import create_all_tables


# DO NOT MODIFY SECTION START
# modifying this section will cause many tests to fail.
# this section is protected by the system.
@pytest.fixture
def session_local():
    engine = create_engine('sqlite:///:memory:',
                          connect_args={'check_same_thread': False},
                          poolclass=StaticPool)
    create_all_tables(engine)
    return sessionmaker(bind=engine)

@pytest.fixture
def db_session(session_local):
    session = session_local()
    try:
        yield session
    finally:
        session.close()

@pytest.fixture
def client(session_local):
    def override_session():
        session = session_local()
        try:
            yield session
        finally:
            session.close()

    app.dependency_overrides[get_db] = override_session
    with TestClient(app) as c:
        yield c
    app.dependency_overrides[get_db] = get_db
# DO NOT MODIFY SECTION END