### Create docker image for partition job:
```
export PROJECT_ID=<your-project-here>
export REPO=<repo> eg:weather-tools

gcloud builds submit . --tag "gcr.io/$PROJECT_ID/$REPO:weather-dl-v2-partition_job" --timeout=79200 --machine-type=e2-highcpu-32
```