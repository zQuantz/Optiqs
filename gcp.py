from google.cloud import monitoring_v3
from const import CREDS
import json
import time

###################################################################################################

METRIC_CLIENT = monitoring_v3.MetricServiceClient(credentials=CREDS)

###################################################################################################

def send_metric(CONFIG, metric_name, metric_type, metric):

	series = monitoring_v3.types.TimeSeries()

	series.metric.type = f"custom.googleapis.com/{metric_name}"
	series.resource.type = 'global'

	point = series.points.add()
	setattr(point.value, metric_type, metric)

	now = time.time()
	point.interval.end_time.seconds = int(now)
	point.interval.end_time.nanos = int(
	    (now - point.interval.end_time.seconds) * 10**9)

	project_name = METRIC_CLIENT.project_path(CONFIG['GCP']['PROJECT_ID'])
	METRIC_CLIENT.create_time_series(project_name, [series])

def delete_gcp_metric():

	project_id = "XXX"
	metric_name = "XXX"
	name = f"projects/{project_id}/metricDescriptors/custom.googleapis.com/{metric_name}"	

	METRIC_CLIENT.delete_metric_descriptor(name)
	print('Deleted metric descriptor {}.'.format(name))

def create_gcp_metric(metric_name, value_type):

	with open("optiqs_config.json", "r") as file:
		CONFIG = json.loads(file.read())

	project_name = METRIC_CLIENT.project_path(CONFIG['GCP']['PROJECT_ID'])

	descriptor = monitoring_v3.types.MetricDescriptor()
	descriptor.type = f'custom.googleapis.com/{metric_name}'

	descriptor.metric_kind = (monitoring_v3.enums.MetricDescriptor.MetricKind.GAUGE)
	descriptor.value_type = (monitoring_v3.enums.MetricDescriptor.ValueType[value_type])

	descriptor.description = 'This is a simple example of a custom metric.'
	descriptor = METRIC_CLIENT.create_metric_descriptor(project_name, descriptor)

	print('Created {}.'.format(descriptor.name))

if __name__ == '__main__':

	# bucket_backup()

	## CBOE
	# create_gcp_metric("cboe_options_dump_size", "DOUBLE")
	create_gcp_metric("cboe_options_indicator", "INT64")

	pass