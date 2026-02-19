SHELL := /bin/bash

include .env

.EXPORT_ALL_VARIABLES:

.PHONY: upload-dags-to-bucket
upload-dags-to-bucket:
	@echo "Uploading dags to $(S3_BUCKET_NAME)..."
	s3cmd put --recursive dags/ s3://$(S3_BUCKET_NAME)/dags/
	@echo "DAGs uploaded successfully"

.PHONY: upload-src-to-bucket
upload-src-to-bucket:
	@echo "Uploading src to $(S3_BUCKET_NAME)..."
	s3cmd put --recursive src/ s3://$(S3_BUCKET_NAME)/src/
	@echo "Src uploaded successfully"

.PHONY: upload-vars-to-bucket
upload-vars-to-bucket:
	@echo "Uploading vars to $(S3_BUCKET_NAME)..."
	s3cmd put infra/variables.json s3://$(S3_BUCKET_NAME)/vars/
	@echo "vars uploaded successfully"

.PHONY: upload-data-to-bucket
upload-data-to-bucket:
	@echo "Uploading data to $(S3_BUCKET_NAME)..."
	s3cmd put --recursive data/input_data/*.csv s3://$(S3_BUCKET_NAME)/input_data/
	@echo "Data uploaded successfully"

upload-all: upload-data-to-bucket upload-src-to-bucket upload-dags-to-bucket

.PHONY: clean-s3-bucket
clean-s3-bucket:
	@echo "Cleaning S3 bucket $(S3_BUCKET_NAME)..."
	s3cmd del --force --recursive s3://$(S3_BUCKET_NAME)/
	@echo "S3 bucket cleaned"

.PHONY: remove-s3-bucket
remove-s3-bucket:
	@echo "Removing S3 bucket $(S3_BUCKET_NAME)..."
	s3cmd rb s3://$(S3_BUCKET_NAME)
	@echo "S3 bucket removed"

.PHONY: download-output-data-from-bucket
download-output-data-from-bucket:
	@echo "Downloading output data from $(S3_BUCKET_NAME)..."
	s3cmd get --recursive s3://$(S3_BUCKET_NAME)/output_data/ data/output_data/
	@echo "Output data downloaded successfully"

.PHONY: instance-list
instance-list:
	@echo "Listing instances..."
	yc compute instance list

.PHONY: git-push-secrets
git-push-secrets:
	@echo "Pushing secrets to github..."
	python3 utils/push_secrets_to_github_repo.py


.PHONY: airflow-cluster-mon
airflow-cluster-mon:
	yc logging read --group-name=default --follow

.PHONY: create-venv-archive
create-venv-archive:
	@echo "Creating .venv archive..."
	mkdir -p venvs
	chmod +x ./scripts/create_venv_archive.sh
	bash ./scripts/create_venv_archive.sh
	@echo "Archive created successfully"

.PHONY: create-venv-archive-wsl
create-venv-archive-wsl:
	@echo "Creating .venv archive... wsl"
	wsl.exe -d Ubuntu -e bash -lc "cd /mnt/c/MLOps/DZ_6 && make create-venv-archive"
	@echo "Archive created wsl successfully"


.PHONY: upload-venv-to-bucket
upload-venv-to-bucket:
	@echo "Uploading virtual environment archive to $(S3_BUCKET_NAME)..."
	s3cmd put venvs/venv38.tar.gz s3://$(S3_BUCKET_NAME)/venvs/venv38.tar.gz
	@echo "Virtual environment archive uploaded successfully"

.PHONY: deploy-full
deploy-full: create-venv-archive-wsl upload-venv-to-bucket upload-src-to-bucket upload-dags-to-bucket upload-data-to-bucket
	@echo "Full deployment completed sumake ccessfully"
	@echo "Virtual environment, source code, DAGs, and data have been uploaded to S3"
	@echo "You can now run the pipeline in Airflow"
