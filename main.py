from fastapi import FastAPI
from services.db_services import test_database,database
from services.rabbit_service import test_rabbitmq
from services.teams_services import send_teams_message
from routes.file_routes import files
from contextlib import asynccontextmanager


#TODO: Always use logs NO PRINTS
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Connecting to DB...")
    await database.connect()
    print("DB connected!")
    yield
    await database.disconnect()
    print("DB disconnected!")

app = FastAPI(title="FileFlow API",lifespan=lifespan)

@app.get("/healthcheck")
async def health_check():
    db_status = test_database()
    rabbit_status = await test_rabbitmq()
    return {
        "database": db_status,
        "rabbitmq": rabbit_status
    }

@app.get("/test-teams")
def test_teams(message: str = "Hello from FastAPI!"):
    """Send a test message to Teams"""
    result = send_teams_message(message)
    return {"teams": result}

app.include_router(files, prefix="/files", tags=["Files"])

