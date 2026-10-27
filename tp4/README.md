# TP04 — Containerization and Big Data Environment Deployment Using Docker

## 1. Objective

This practical demonstrates Docker images, containers, volumes, networks, port mapping, a custom Python image, Docker Hub, and Docker Compose. The Python application reads CSV files from `data/` and reports a sample of the dataset's columns, data types, numeric summary statistics, and missing values.

## 2. Project structure

```text
tp4/
├── app.py
├── requirements.txt
├── Dockerfile
├── compose.yaml
├── .dockerignore
├── data/
│   └── .gitkeep
└── screenshots/
```

## 3. Prerequisites

- Docker Desktop running with Linux containers.
- A Docker Hub account.
- Python is not required on the host to run the container.
- Copy the CSV dataset used in TP03 into `tp4/data/`. Do not commit a very large dataset to GitHub unless your instructor requires it and repository limits permit it.

## 4. Task 1 — Verify Docker

In PowerShell:

```powershell
docker --version
docker compose version
docker run --rm hello-world
docker login
```

`docker login` asks you to authenticate with Docker Hub. Never put your password or access token in this README or in source code.

## 5. Task 2 — Build, push, and pull an image

Replace `YOUR_DOCKERHUB_USERNAME` with your Docker Hub username.

Run these commands from the `tp4` directory:

```powershell
docker build -t YOUR_DOCKERHUB_USERNAME/tp04-data-analysis:1.0 .
docker push YOUR_DOCKERHUB_USERNAME/tp04-data-analysis:1.0
docker pull YOUR_DOCKERHUB_USERNAME/tp04-data-analysis:1.0
```

The `docker build` command builds the image from the Dockerfile. `push` uploads it to Docker Hub, and `pull` downloads it again from the registry.

### Use the downloaded image

To test the image from Docker Hub rather than building it locally:

```powershell
docker run --rm -v "${PWD}/data:/app/data:ro" YOUR_DOCKERHUB_USERNAME/tp04-data-analysis:1.0
```

## 6. Task 3 — Create a network and three containers

First make sure the image was pulled successfully. Then run:

```powershell
docker network create tp04-network
docker run -d --name tp04-worker1 --network tp04-network YOUR_DOCKERHUB_USERNAME/tp04-data-analysis:1.0 tail -f /dev/null
docker run -d --name tp04-worker2 --network tp04-network YOUR_DOCKERHUB_USERNAME/tp04-data-analysis:1.0 tail -f /dev/null
docker run -d --name tp04-worker3 --network tp04-network YOUR_DOCKERHUB_USERNAME/tp04-data-analysis:1.0 tail -f /dev/null
docker ps
docker network inspect tp04-network
```

These three containers are kept running with `tail -f /dev/null` so that you can inspect them and verify network membership. They are container instances of the same downloaded image. The image's default `CMD` is intentionally overridden for this task.

Execute the application inside each container:

```powershell
docker exec tp04-worker1 python app.py
docker exec tp04-worker2 python app.py
docker exec tp04-worker3 python app.py
```

The containers need access to the dataset. The above commands will report that no CSV is present unless the dataset is copied into the image or mounted into each container. To mount the local data directory when creating them, use these commands instead of the three `docker run` commands above:

```powershell
docker rm -f tp04-worker1 tp04-worker2 tp04-worker3
docker run -d --name tp04-worker1 --network tp04-network -v "${PWD}/data:/app/data:ro" YOUR_DOCKERHUB_USERNAME/tp04-data-analysis:1.0 tail -f /dev/null
docker run -d --name tp04-worker2 --network tp04-network -v "${PWD}/data:/app/data:ro" YOUR_DOCKERHUB_USERNAME/tp04-data-analysis:1.0 tail -f /dev/null
docker run -d --name tp04-worker3 --network tp04-network -v "${PWD}/data:/app/data:ro" YOUR_DOCKERHUB_USERNAME/tp04-data-analysis:1.0 tail -f /dev/null
```

The app is a command-line program, so it does not listen on a network port. Therefore, port mapping (`-p`) is not required for this particular application. Port mapping is used when a container runs a network service, for example `-p 8080:80` for a web server listening on container port 80.

Clean up the Task 3 containers and network when finished:

```powershell
docker rm -f tp04-worker1 tp04-worker2 tp04-worker3
docker network rm tp04-network
```

## 7. Task 4 — Run with Docker Compose

Place the TP03 CSV dataset in `data/`, then run from the directory containing `compose.yaml`:

```powershell
docker compose up --build
docker compose ps -a
docker compose down
```

This assignment's Compose service mounts `./data` into `/app/data` as read-only (`:ro`). The application exits after analysis, so its container may show `Exited (0)` after completion; that is expected.

## 8. Questions — Task 4

### What problem does Docker Compose solve?

Docker Compose defines an application's services, networks, volumes, configuration, and lifecycle in a YAML file. It allows a multi-container environment to be started and stopped consistently with a few commands instead of repeating long `docker run` commands.

### Compare `docker run` with `docker compose up`.

- `docker run` creates and starts an individual container using options supplied on the command line.
- `docker compose up` reads `compose.yaml`, builds images when requested, and creates/starts the services defined by the project. It is easier to repeat and maintain when an application has multiple services.

### How could the Compose project be extended to include a database service?

Add a database service such as PostgreSQL to `compose.yaml`, configure its database name and credentials through environment variables, and add a named volume to persist database files. The application service can connect to the database using the Compose service name (for example, `db`) as its hostname. Credentials should be stored safely and not committed to GitHub. For a real application, add a database driver and code that actually connects to the database.

Example extension (illustrative; the current `app.py` does not connect to PostgreSQL):

```yaml
services:
  data-analysis:
    build: .
    volumes:
      - ./data:/app/data:ro
    depends_on:
      - db
  db:
    image: postgres:16
    environment:
      POSTGRES_DB: analysis
      POSTGRES_USER: analyst
      POSTGRES_PASSWORD: change_me_for_local_use
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

## 9. Task 5 — Short report

### What is Docker, and why is it useful in data-science projects?

Docker is a platform that packages an application and its dependencies into an image that can be run as an isolated container. In data science, it helps keep Python and library versions consistent, makes experiments easier to reproduce, and reduces differences between development computers and deployment environments.

### Explain the difference between a Docker image and a container.

An image is a read-only template containing the application, runtime, libraries, and configuration. A container is a running (or stopped) instance created from an image. Multiple containers can be created from the same image, each with its own runtime state.

### Explain `FROM`, `WORKDIR`, `COPY`, `RUN`, and `CMD`.

- `FROM`: selects the base image, here `python:3.11-slim`.
- `WORKDIR`: sets the working directory inside the image/container.
- `COPY`: copies files from the build context into the image.
- `RUN`: executes commands while building the image, here installing Python dependencies.
- `CMD`: defines the default command executed when a container starts, here running `app.py`.

## 10. Screenshots checklist

Save screenshots in `screenshots/` and reference them here before submission:

1. Docker version and successful `hello-world`.
2. Successful `docker build`.
3. Docker Hub repository showing the `1.0` tag.
4. Successful `docker pull`.
5. `docker ps` showing the three Task 3 containers.
6. `docker network inspect tp04-network` showing the containers attached.
7. `docker compose up --build` showing the dataset analysis.
8. `docker compose ps -a` showing the service status.

Use genuine screenshots from your own computer; do not fabricate evidence.

## 11. GitHub submission

From the root of the existing `D:\big-data` repository:

```powershell
git status
git add tp4
git commit -m "Add TP04 Docker containerization lab"
git push
```

If your CSV dataset is large, keep it outside Git and copy it into `tp4/data/` locally before running the lab. `.dockerignore` excludes the dataset from the image build context, while Compose mounts it at runtime.

## 12. Troubleshooting

- `no configuration file provided: not found`: run `docker compose` from the directory containing `compose.yaml`.
- `docker: command not found`: reopen PowerShell after updating PATH, or verify Docker Desktop is installed and running.
- `port is already allocated`: this CLI analysis application does not need a published port; remove unnecessary `-p` flags.
- No CSV found: put your TP03 CSV file in `tp4/data/`.
- Container exits with code 0: expected for a batch analysis program that finishes its work and exits.
