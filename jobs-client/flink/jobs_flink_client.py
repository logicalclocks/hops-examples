import os
import requests
import argparse
import urllib.parse

parser = argparse.ArgumentParser()
parser.add_argument("-u", "--hopsworks-url", help="Hopsworks url (domain and port")
parser.add_argument("-appid", "--application_id", help="The Flink job application id")
parser.add_argument("-jar", help="The Flink job jar file")
parser.add_argument("-m", "--main", help="The entry point to the application, file with main function")
parser.add_argument("-a", "--apikey", help="The file containing the API key to be used to "
                                                                 "submit the job")
parser.add_argument("-parallelism", "--parallelism", default="1", help="Flink job parallelimsThe file containing the API key to be used to submit the job")
parser.add_argument("-args", "--job-arguments", help="Flink job runtime arguments")
args = parser.parse_args()
print(args)
base_url = "https://" + args.hopsworks_url + "/hopsworks-api/flinkmaster/" + args.application_id

# Upload Flink job jar to job manager
response = requests.post(
    base_url + "/jars/upload",
    files={
        "jarfile": (
            os.path.basename(args.jar),
            open(args.jar, "rb"),
            "application/x-java-archive"
        )
    },
    headers={"Authorization": "Apikey " + args.apikey}
)
print(response.json())
print(response.json()["filename"])

# Request URL: https://8a4c06f0-c508-11ea-ac91-19f73699d396.aws.hopsworks.ai/hopsworks-api/flinkmaster/application_1594645575168_0002/jars/ca3e0287-e0f8-427b-971b-cc6314323346_WordCount.jar/run?entry-class=org.apache.flink.streaming.examples.wordcount.WordCount&parallelism=1&program-args=--arg1%20a1%20--arg2%20a2

# Run job
jar_id = response.json()["filename"].split("/")[-1]
print(jar_id)
job_args = urllib.parse.quote(args.job_arguments)
print("jobs_args:" + job_args)
response = requests.post(
    base_url + "/jars/" + jar_id + "/run?entry-class=" + args.main + "&program-arg=" + job_args,
    headers={"Content-Type" : "application/json", "Authorization": "Apikey " + args.apikey}
)
print(response)
