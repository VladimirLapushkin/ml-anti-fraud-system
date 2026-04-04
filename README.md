deploy-full: create-s3cmd  upload-vars-to-bucket upload-venv-to-bucket upload-src-to-bucket

добавить в /infra/terraform.tfvars репозиторий dags

git_repo                 = ""
git_ssh_private_key      = "~/.ssh/github_deploy_dags"
    
