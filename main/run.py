import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from src.api.bq_read import bq_router
from src.api.routes import router
from src.api.pg_read import pg_router
from src.api.auth_api import auth_router

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Change this to specific domains in prod
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*", "Authorization", "Accept", "X-Requested-With"],
    expose_headers=["Content-Type", "X-Total-Count"],
    max_age=3600
)

app.include_router(auth_router)
app.include_router(router)
app.include_router(bq_router)
app.include_router(pg_router)

@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "code": exc.status_code,
            "name": exc.__class__.__name__,
            "description": exc.detail,
        },
    )



def main():
    uvicorn.run("main.run:app", host="0.0.0.0", port=8000, reload=True)

if __name__ == "__main__":
    main()

