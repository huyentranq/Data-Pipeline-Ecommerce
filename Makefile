include env

install:
	python -m venv venv
	source venv/bin/activate
	pip install -r requirements.txt --timeout 300


build-dagster:
	docker build -t de_dagster:latest ./dockerimages/dagster

build-spark:
	docker build -t spark_master:latest ./dockerimages/spark

build-pipeline:
	docker build -t etl_pipeline:latest ./etl_pipeline


build:
	docker-compose build
	
up:
	docker-compose up -d

down:
	docker-compose down	

restart:
	make down && make up

rebuild:
	make down && make build && make up
to_psql:
	docker exec -ti de_psql psql postgres://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}

# Connect to MySQL container with appropriate environment variables
to_mysql:
	docker exec -it de_mysql mysql --local-infile=1 -u"${MYSQL_USER}" -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE}

# Connect to MySQL as root with root password and database
to_mysql_root:
	docker exec -it de_mysql mysql -u"root" -p"${MYSQL_ROOT_PASSWORD}" ${MYSQL_DATABASE}
