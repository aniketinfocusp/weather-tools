from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from routers import license, download, queues


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Boot up
    # TODO: Replace hard-coded collection name by read a server config.
    print("TODO: Create database if not already exists.")
    print("TODO: Retrieve license information & create license deployment if needed.")
    yield
    # Clean up

app = FastAPI(lifespan=lifespan)

app.include_router(license.router)
app.include_router(download.router)
app.include_router(queues.router)


@app.get("/")
async def main():
    content = """
<body>
Greetings from weather-dl v2 !!
</body>
    """
    return HTMLResponse(content=content)