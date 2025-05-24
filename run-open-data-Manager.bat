@echo off
echo Building Docker image...
docker build -t open_data_manager .

if %errorlevel% neq 0 (
    echo Docker build failed.
    exit /b %errorlevel%
)

echo Running Docker container...
docker run -it --env-file .env -v %cd%\Data:/app/Data open_data_manager

if %errorlevel% neq 0 (
    echo Docker run failed.
    exit /b %errorlevel%
)

echo Docker container has exited.