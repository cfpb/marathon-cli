import os
import sys
import json
import time
import logging
import pprint

import requests

from marathon import (
    MarathonApp, MarathonClient, MarathonError, MarathonHttpError
)

TASK_STATUS_URL = "{}/tasks.json"
STDOUT_URL = "{}/files/read.json?path=/opt/mesos/slaves/{}/frameworks/{}/executors/{}/runs/{}/stdout"  # noqa
STDERR_URL = "{}/files/read.json?path=/opt/mesos/slaves/{}/frameworks/{}/executors/{}/runs/{}/stderr"  # noqa
OFFSET = "&offset={}&length={}"
MARATHON_APP_ID = os.getenv("MARATHON_APP_ID", "test-app")

# Setup Logging
logger = logging.getLogger('marathon')
logger.setLevel(logging.INFO)


def get_task_by_version(client, app_id, version):
    """
    Gets the Mesos task using the Marathon version of the deployment.
    """
    logger.debug("Attempting to get task for app version {}".format(version))
    tasks = client.list_tasks(app_id=marathon_app_id)
    new_task = None
    for task in tasks:
        logger.debug("Found task: {}".format(task))
        if task.version == version:
            logger.debug("Task with version {} found!".format(version))
            new_task = task
    if not new_task:
        logger.debug("Failed to find task for version {}".format(version))
    return new_task


def print_file_chunk(url, offset, auth):
    """
    Takes a URL pointing to a Mesos file, and an offset, and prints
    the file contents from offset to the end, then returns the new offset.
    """
    response = requests.get(url, auth=auth, verify=False)
    try:
        length = response.json()['offset'] - offset
    except ValueError:
        logger.debug(
            "Invalid JSON response received: "
            f"{response} from URL {url}, skipping..."
        )
        length = 0
    
    offset_params = OFFSET.format(offset, length)
    response = requests.get(url + offset_params, auth=auth, verify=False)
    try:
        data = response.json()['data']
    except ValueError:
        logger.debug(
            "Invalid JSON response received: "
            f"{response} from URL {url}, skipping..."
        )
        data = ""
    
    if data != "":
        for line in data.split('\n')[:-1]:
            logger.info("CONTAINER LOG: {}".format(line))

    return offset + length


def get_marathon_json():
    """
    Return the json configuration for the Mesos app.
    Assumes a Jenkins job is providing the following vars, some sensitive:

    VAR   | example
    :---- | :--------
    MARATHON_JENKINS      | true (flag that Jenkins is delivering these vars)
    MARATHON_FORCE_DEPLOY | true
    MARATHON_APP_ID       | "/complaint-search/search-tool-staging" 
    ATTACHMENTS_ROOT      | "/home/dtwork/"
    DOCKER_USER           | (sensitive)
    MESOS_MASTER_URLS     | (sensitive)
    MESOS_AGENT_MAP       | (sensitive)
    ES_USERNAME           | (sensitive)
    ES_PASSWORD           | (sensitive)
    LDAP_HOST             | (sensitive)
    LDAP_USERNAME         | (sensitive)
    LDAP_PASSWORD         | (sensitive)
    LDAP_BASE_DN          | (sensitive)
    PG_USERNAME           | (sensitive)
    PG_PASSWORD           | (sensitive)
    HOST_BULK             | (sensitive)
    
    """
    ATTACHMENTS_ROOT = os.getenv("ATTACHMENTS_ROOT")

    app_data = {
        "id": MARATHON_APP_ID,
        "container": {
            "docker": {
                "image": os.getenv("FQDI"),
                "forcePullImage": True
            },
            "volumes": [
                {
                    "containerPath": f"{ATTACHMENTS_ROOT}/mosaic",
                    "hostPath": "/home/dtwork/mosaic",
                    "mode": "RO"
                },
                {
                    "containerPath": f"{ATTACHMENTS_ROOT}/rightnow",
                    "hostPath": "/home/dtwork/rightnow",
                    "mode": "RO"
                },
                {
                    "containerPath": "/etc/pki/ca-trust",
                    "hostPath": "/etc/pki/ca-trust",
                    "mode": "RO"
                }
            ],
            "type": "MESOS"
        },
        "cmd": "/docker-entrypoint.sh",
        "cpus": 2,
        "mem": 8000,
        "disk": 5000,
        "instances": 2,
        "user": f'{os.getenv("DOCKER_USER")}',
        "env": {
            "ES_SERVER_ENV": "staging",
            "DJANGO_SETTINGS_MODULE": "search_tool.mesos",
            "ES_HOST": "https://es2.data.cfpb.local/",
            "ES_INDEX_ATTACHMENT": "complaint-crdb-attachment-staging",
            "ES_INDEX_COMPLAINT": "complaint-crdb-staging",
            "ES_USERNAME": os.getenv("ES_USERNAME"),
            "ES_PASSWORD": os.getenv("ES_PASSWORD"),
            "LDAP_HOST": os.getenv("LDAP_HOST"),
            "LDAP_USERNAME": os.getenv("LDAP_USERNAME"),
            "LDAP_PASSWORD": os.getenv("LDAP_PASSWORD"),
            "LDAP_BASE_DN": os.getenv("LDAP_BASE_DN"),
            "PGUSER": os.getenv("PG_USERNAME"),
            "PGPASSWORD": os.getenv("PG_PASSWORD"),
            "STATIC_URL": "/static/",
            "HOST_BULK": os.getenv("HOST_BULK"),
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),  # ECR keys
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        },
        "healthChecks": [
            {
                "path": "/",
                "protocol": "HTTP",
                "portIndex": 0,
                "gracePeriodSeconds": 600,
                "intervalSeconds": 60,
                "timeoutSeconds": 60,
                "maxConsecutiveFailures": 3,
                "ignoreHttp1xx": False
            }
        ]
    }
    return json.dumps(app_data, indent=2)


if __name__ == '__main__':
    """
    This script reads in values from environment variables, then deploys a
    Marathon application as defined in get_marathon_json().

    Vars used outside of the json config:

    MARATHON_URLS:              One or more URLs to try when communication with
                                Marathon, separated by commas.
    MARATHON_APP_ID:            The identifier of the application in Marathon.
    MARATHON_USER:              The user to use, if needed.
    MARATHON_PASSWORD:          The password to use, if needed.
    MARATHON_FORCE_DEPLOY:      Use the Force, if necessary (i.e. when a
                                deployment is failing.)
    MARATHON_FRAMEWORK_NAME:    The name of the framework (usually 'marathon')
    MARATHON_APP:               The JSON formatted app definition.
    MARATHON_RETRIES:           The number of task failures to tolerate.
    MESOS_AGENT_MAP:            This is used when Mesos is behind a proxy.  The
                                API will return the Mesos Agent IP address,
                                but that may need to be mapped to a URL.  The
                                map is defined like:
                                10.0.1.34|https://mesos.test.dev,...
    MESOS_MASTER_URLS:          One or more urls for Mesos communication,
                                separated by commas.
    """

    marathon_app_id = MARATHON_APP_ID
    marathon_urls = os.getenv(
        "MARATHON_URLS", "http://localhost:8080").split(',')
    marathon_user = os.getenv("MARATHON_USER", None)
    marathon_password = os.getenv("MARATHON_PASSWORD", None)
    marathon_force = True if os.getenv(
        "MARATHON_FORCE_DEPLOY", "false") == "true" else False
    marathon_framework_name = os.getenv("MARATHON_FRAMEWORK_NAME", "marathon")
    marathon_retries = int(os.getenv("MARATHON_RETRIES", 3))
    if os.getenv("MARATHON_JENKINS"):
        # Jenkins is supplying env vars
        marathon_app = get_marathon_json()
    else:
        # we fall back to default app json config
        marathon_app = """
        {
            "id": "/test-app",
            "cmd": "mv *.war apache-tomcat-*/webapps && cd apache-tomcat-* && sed \\"s/8080/$PORT/g\\" < ./conf/server.xml > ./conf/server-mesos.xml && sleep 15 && ./bin/catalina.sh run -config ./conf/server-mesos.xml",
            "cpus": 1,
            "mem": 128,
            "disk": 0,
            "instances": 1,
            "user": "mesagent",
            "uris": [
                "http://mirrors.gigenet.com/apache/tomcat/tomcat-7/v7.0.73/bin/apache-tomcat-7.0.73.tar.gz",
                "https://storage.googleapis.com/google-code-archive-downloads/v2/code.google.com/gwt-examples/Calendar.war"
            ],
            "healthChecks": [
                {
                  "protocol": "HTTP",
                  "path": "/",
                  "portIndex": 0,
                  "gracePeriodSeconds": 300,
                  "intervalSeconds": 60,
                  "timeoutSeconds": 20,
                  "maxConsecutiveFailures": 3
                }
            ]
        }"""
    pp = pprint.PrettyPrinter(indent=2, width=120, depth=4)
    print("marathon json config:")
    pp.pprint(json.loads(marathon_app))

    app_definition = MarathonApp.from_json(json.loads(marathon_app))

    mesos_agent_map_string = os.getenv("MESOS_AGENT_MAP", None)
    mesos_master_urls = os.getenv(
        "MESOS_MASTER_URLS", "http://localhost:5050").split(',')

    exit_code = 0
    auth = None
    if marathon_user and marathon_password:
        auth = (marathon_user, marathon_password)

    try:
        logger.info("Connecting to Marathon...")
        client = MarathonClient(
            marathon_urls,
            username=marathon_user,
            password=marathon_password,
            verify=False
        )
    except MarathonError as e:
        logger.error("Failed to connect to Marathon! {}".format(e))
        exit_code = 1
        sys.exit(exit_code)

    logger.info("Deploying application...")
    try:
        app = client.get_app(marathon_app_id)
    except MarathonHttpError:
        response = client.create_app(marathon_app_id, app_definition)
        version = response.version
        depolyment_id = response.deployments[0].id
    else:
        response = client.update_app(
            marathon_app_id,
            app_definition,
            force=marathon_force
        )
        version = response['version']
        deployment_id = response['deploymentId']

    logger.info("New version deployed: {}".format(version))

    if app_definition.instances == 0:
        logger.info(
            "Deactivated application by setting instances "
            "to 0, deployment complete."
        )
        exit_code = 0
        sys.exit(exit_code)

    # Get newly created Mesos task

    time.sleep(5)
    new_task = get_task_by_version(client, marathon_app_id, version)

    if not new_task:
        logger.warning(
            "New task did not start automatically, probably because "
            "the application definition did not change, forcing restart..."
        )
        response = None
        for hostname in marathon_urls:
            try:
                headers = {"content-type": "application/json"}
                force_string = "true" if marathon_force else "false"
                _url = "{}/v2/apps/{}/restart?force={}".format(
                    hostname, marathon_app_id, force_string
                ) 
                response = requests.post(
                    _url,
                    auth=auth,
                    verify=False,
                    headers=headers
                )
            except requests.exceptions.ConnectionError as e:
                logger.warning(
                    "Marathon connection error, ignoring: {}".format(e))
                pass
            else:
                break

        if response.status_code != 200:
            logger.error(
                "Failed to force application restart, "
                "received response {}, exiting...".format(response.text)
            )
            exit_code = 1
            sys.exit(exit_code)

        attempts = 0
        while not new_task and attempts < 10:
            time.sleep(2)
            new_task = get_task_by_version(
                client, marathon_app_id, response.json()["version"]
            )
            attempts += 1

        if not new_task:
            logger.error(
                "Unable to retrieve new task from Marathon, "
                "there may be a communication failure with Mesos."
            )
            exit_code = 1
            sys.exit(exit_code)

        deployment_id = response.json()["deploymentId"]
        logger.info(
            f"New version created by restart: {response.json()['version']}"
        )

    # Get Framework ID

    marathon_info = client.get_info()
    framework_id = marathon_info.framework_id

    # Query Mesos API to discover Container ID
    agent_hostname = new_task.host
    mesos_agent_map = {}
    if mesos_agent_map_string:
        for mapping in mesos_agent_map_string.split(','):
            mapping = mapping.split('|')
            mesos_agent_map[mapping[0]] = mapping[1]
        agent_hostname = mesos_agent_map[agent_hostname]
    else:
        agent_hostname = "http://{}:5051".format(agent_hostname)

    logger.info(
        f'SSH command: ssh -t {new_task.host} '
        f'"cd /opt/mesos/slaves/*/frameworks/*/executors/{new_task.id}/runs/latest; exec \\$SHELL -l"'  # noqa
    )

    mesos_tasks = requests.get(
        "{}/state.json".format(agent_hostname), auth=auth, verify=False
    )
    marathon_framework = None
    container_id = None

    # TODO: User framework_id instead of marathon_framework_name
    try:
        mesos_tasks = mesos_tasks.json()
    except ValueError as e:
        logger.error("Error {} from response {}".format(e, mesos_tasks.text))
        logger.error(
            "Deployment may have started, but cannot confirm with Mesos."
        )
        exit_code = 1
        sys.exit(exit_code)

    for framework in mesos_tasks['frameworks']:
        if framework['name'] == marathon_framework_name:
            marathon_framework = framework
            break

    if not marathon_framework:
        logger.error("Marathon Framework not discoverable via Mesos API.")

    for executor in framework['executors']:
        if executor['source'] == new_task.id:
            container_id = executor['container']
            break

    if not container_id:
        logger.error("Executor for task {} not found.".format(new_task.id))

    # Stream STDOUT and STDERR from Mesos until the deployment has completed
    logger.info("Streaming logs from Mesos...\n")

    attempts = 0

    # Allow up to marathon_retries tasks to fail
    while attempts < marathon_retries:

        stdout_offset = 0
        stderr_offset = 0
        done = False
        failed = False

        # Stream stdout and stderr to the console
        while not done:
            deployments = client.get_app(marathon_app_id).deployments

            if deployments == []:
                logger.debug("No deployments remaining, set done=True.")
                time.sleep(3)
                done = True
            else:
                mesos_tasks = None
                logger.debug("Getting Mesos task state...")

                for host in mesos_master_urls:
                    logger.debug("Trying Mesos host {}...".format(host))
                    try:
                        response = requests.get(
                            TASK_STATUS_URL.format(host),
                            auth=auth,
                            verify=False,
                            timeout=1
                        )
                    except requests.exceptions.ConnectionError:
                        logger.debug(
                            f"Failed to connect to Mesos host {host}, "
                            "trying next host..."
                        )
                        continue
                    except requests.exceptions.ReadTimeout:
                        logger.debug(
                            f"Read timeout from Mesos host {host}, "
                            "trying next host..."
                        )
                        continue

                    if response.status_code == 200:
                        mesos_tasks = response.json()
                        break
                    else:
                        logger.warning(
                            "Response code != 200: {}".format(
                                pp.pprint(response))
                        )

                if mesos_tasks:
                    for task in mesos_tasks['tasks']:
                        #  print task
                        if task['id'] == new_task.id:
                            if task['state'] in [
                                "TASK_FAILED",
                                "TASK_KILLED",
                                "TASK_FINISHED"
                            ]:
                                logger.warning(
                                    "task failed: {}".format(pp.pprint(task))
                                )
                                failed = True
                                done = True

                else:
                    logger.warning(
                        "Failed to connect to Mesos API, "
                        "task status not available."
                    )

            # Get STDOUT

            stderr_url = STDERR_URL.format(
                agent_hostname,
                new_task.slave_id,
                framework_id,
                new_task.id,
                container_id
            )
            stderr_offset = print_file_chunk(stderr_url, stderr_offset, auth)

            # Get STDERR

            stdout_url = STDOUT_URL.format(
                agent_hostname,
                new_task.slave_id,
                framework_id,
                new_task.id,
                container_id
            )
            stdout_offset = print_file_chunk(stdout_url, stdout_offset, auth)

            # Small rate limiting factor
            time.sleep(0.1)

        if failed:
            logger.warning("Deployment task failed, trying again...")
            attempts += 1
        else:
            break

    print()

    # Wait for logs to print
    time.sleep(5)

    logger.info("End of log stream from Mesos.")

    if failed:
        logger.error(
            "Failure deploying new app configuration, aborting deployment!"
        )
        for hostname in marathon_urls:
            try:
                headers = {"content-type": "application/json"}
                response = requests.delete(
                    f"{hostname}/v2/deployments/{deployment_id}?force=true",
                    auth=auth,
                    verify=False,
                    headers=headers
                )
            except requests.exceptions.ConnectionError as e:
                logger.warn(
                    "Marathon connection error, ignoring: {}".format(e))
                pass
            else:
                break

        if response.status_code in [200, 202]:
            logger.warn("Successfully cancelled failed deployment.")
        else:
            logger.error(
                f"Failed to force stop deployment: {response.text}, "
                "you may need to try again with MARATHON_FORCE_DEPLOY=true, "
                "exiting..."
            )

        exit_code = 1
    else:
        logger.info("All deployments completed sucessfully!")
    sys.exit(exit_code)
