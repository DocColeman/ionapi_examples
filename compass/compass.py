import json, requests, urllib3, time, io

urllib3.disable_warnings()
headers = {}


def get_ionapi(ionapi_file):
    tokens = {}
    with open(ionapi_file, "r") as f:
        line = f.readline()
    tokens = json.loads(line)
    return tokens


def get_authToken(ionapi):
    token_url = ionapi["pu"] + ionapi["ot"]

    # Resource owner (enduser) credentials
    RO_user = ionapi["saak"]
    RO_password = ionapi["sask"]

    # client (application) credentials
    client_id = ionapi["ci"]
    client_secret = ionapi["cs"]

    data = {"grant_type": "password", "username": RO_user, "password": RO_password}
    access_token_response = requests.post(
        token_url,
        data=data,
        verify=False,
        allow_redirects=False,
        auth=(client_id, client_secret),
    )
    tokens = json.loads(access_token_response.text)
    headers["Authorization"] = "Bearer " + tokens["access_token"]
    return tokens


def get_baseUrl(ionapi):
    return ionapi["iu"] + "/" + ionapi["ti"]


def get_locUrl(ionapi):
    return ionapi["iu"]


# Put your IONAPI file in same folder as this and change the string below
ionapiTokens = get_ionapi("STARGATE_DEM.ionapi")
baseUrl = get_baseUrl(ionapiTokens)
locUrl = get_locUrl(ionapiTokens)
tenant = ionapiTokens['ti']


def createCompassQuery(queryString, resultFormat):
    _ = get_authToken(ionapiTokens)
    url = baseUrl + "/IONSERVICES/datalakeapi/v1/compass/jobs"
    params = {"resultFormat": resultFormat}
    response = requests.post(url, headers=headers, params=params, data=queryString)
    body = response.json()
    status = location = queryId = ""
    if(response.status_code == 202):
        status = body['status']
        location = body['location']
        queryId = body['queryId']
        print(f'Created Compass Job. status: {status}, queryId: {queryId} location: {location}')
    else:
        print(f'Error Creating Compass Job. {response.text}')
    return status, location, queryId


def getCompassJobStatus(statusLocation):
    url = locUrl + statusLocation
    response = requests.get(url, headers=headers)
    location = ""
    if(response.status_code == 200):
        print(f'Compass Job CANCELLED')
    elif(response.status_code == 202):
        print(f'Compass Job RUNNING')
    elif(response.status_code == 201):
        body = response.json()
        status = body['status']
        location = body['location']
        queryId = body['queryId']
        columns = body['columns']
        print(f'Compass Query Job Status. status_code: {response.status_code} status: {status}, queryId: {queryId} location: {location} columns: {columns}')
    else:
        print(f'Error Getting Compass Job Status.')
    return response.status_code, location


def getCompassJobResults(resultsLocation):
    url = locUrl + resultsLocation
    response = requests.get(url, headers=headers)
    if(response.status_code == 200):
        print(f'Retreived Compass Query Results.')
    else:
        print(f'Error Retrieving Compass Job Results.')
    return response.content.decode("utf-8")


def getCompassQueryResults(statusLocation):
    while True:
        status_code, location = getCompassJobStatus(statusLocation)
        if(status_code == 201):
            return getCompassJobResults(location)
        time.sleep(1)


if __name__ == '__main__':
    queryString = "select EmployeeName, EmployeeWorkEmailAddress from HCM_Employee order by EmployeeName"
    status, location, _ = createCompassQuery(queryString, "text/csv")
    if(status):
        resultString = getCompassQueryResults(location)
        print(resultString)