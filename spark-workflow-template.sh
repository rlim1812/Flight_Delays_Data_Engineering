template_name="ryan-etl"
cluster_name="spark-job-flights"
relevant_date="2019-05-10"
bucket=gs://ryan-etl

gcloud beta dataproc workflow-templates delete -q $template_name --region "us-west1" &
gcloud beta dataproc workflow-templates create $template_name --region "us-west1" &&
gcloud beta dataproc workflow-templates set-managed-cluster $template_name --region "us-west1" --cluster-name=$cluster_name --scopes=default --master-machine-type n1-standard-2 --master-boot-disk-size 20 --num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 20 --image-version 1.3 &&

gcloud dataproc workflow-templates add-job pyspark $bucket/spark-job/flights_data_etl.py --step-id flight_delays_etl --workflow-template="projects/data-engineering-263000/regions/us-west1/workflowTemplates/ryan-etl" &&
gcloud beta dataproc workflow-templates instantiate 'projects/data-engineering-263000/regions/us-west1/workflowTemplates/ryan-etl' --region "us-west1" &&
bq load --source_format=NEWLINE_DELIMITED_JSON \
  data_analysis.avg_delays_by_distance \
  gs://flights_data_transformed/2019-05-10/distance_category_output/*.json &&

bq load --source_format=NEWLINE_DELIMITED_JSON \
  data_analysis.avg_delays_by_flight_nums \
  gs://flights_data_transformed/2019-05-10/flight_nums_output/*.json
