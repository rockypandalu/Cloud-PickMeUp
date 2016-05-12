from flask import render_template, jsonify, Flask, request
import json

import urllib
from collections import OrderedDict
import time
import boto3
import botocore.session
import boto.sns
import logging

import math
import requests
import uuid
import string
from apns import APNs, Frame, Payload

dynamodb = boto3.resource('dynamodb')
apns = APNs(use_sandbox=True, cert_file='aps_development-6.pem', key_file='Certificates.pem')

application = app = Flask(__name__)

# Add to the dynamodb
def add_to_db(db, data, driver_user, driver_phone, driver_token, drivers_lat, drivers_lon, driverd_lat, driverd_lon):
    for info in data["routes"][0]["legs"][0]["steps"]:
        global driver_time
        print "Info updated"
        db.put_item(Item={
          'username_time': str(driver_user)+str(driver_time),
          'username': str(driver_user),
          'time': driver_time,
          'location': str(info["start_location"]["lng"])+'+'+str(info["start_location"]["lat"]),
          'driver_phone': str(driver_phone),
          'driver_token': str(driver_token),
          'drivers_lat': str(drivers_lat),
          'drivers_lon': str(drivers_lon),
          'driverd_lat':str(driverd_lat),
          'driverd_lon':str(driverd_lon)
        })
        time_duration = int(info["duration"]["value"])
        driver_time = driver_time+int(time_duration)

def add_to_client_db(db, client_source, client_destination, client_time, client_phone, client_token):
    unique_id = str(uuid.uuid1())
    db.put_item(Item={
          'time': unique_id,
          'client_source': str(client_source),
          'client_destination': str(client_destination),
          'client_time': str(client_time),
          'client_phone': str(client_phone),
          'client_token': str(client_token)
        })
    return unique_id

# Initialize the table in Dynamodb
def init_client_db():    
    try:
        user=dynamodb.create_table(
            TableName = 'clientinfo', 
            KeySchema = [
                {
                    'AttributeName': 'time', 
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName':'time',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
    except botocore.exceptions.ClientError:
        user=dynamodb.Table('clientinfo')
    return user

def init_db():
    #Basic Account info
    # ACCOUNT_ID = '006771170833'   
    # IDENTITY_POOL_ID = 'us-east-1:83f9c923-c638-4349-933f-9a3e1cdd58a1'       
    # ROLE_ARN = 'arn:aws:iam::006771170833:role/Cognito_friday410Unauth_Role'      
    # dynamodb = boto3.resource('dynamodb')                                         
    
    try:
        user=dynamodb.create_table(
            TableName = 'driverRouteInfo', 
            KeySchema = [
                {
                    'AttributeName': 'username_time', 
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'username_time',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
    except botocore.exceptions.ClientError:
        user=dynamodb.Table('driverRouteInfo')
    return user

driver_time = 0

# Indicate driver location to user
@app.route('/driverlocation', methods = ['GET', 'POST', 'PUT'])
def driverlocation():
    location = request.form
    dic = dict(location)
    posted_data = None
    for each in dic:
        posted_data = each
        posted_data = posted_data.split(',')
        break

    lat_now = float(posted_data[0])
    #print 'LAT_NOW******', lat_now
    lng_now = float(posted_data[1])
    #print 'LNG NOW******', lng_now
    payload = Payload(custom = {"driver_lat":lat_now, "driver_lng":lng_now})
    apns.gateway_server.send_notification("######################################################", payload)
    return 'Hello world!'

# Post the driver info
@app.route('/postdriver', methods = ['GET', 'POST', 'PUT'])
def post_driver_info():
    # Get data from the POST request
    print request.form
    dic = dict(request.form)
    posted_data = None
    for each in dic: 
        posted_data = each
        break
    global driver_time
    posted_data = json.loads(posted_data)
    driver_user = posted_data['user']
    destination = posted_data['dest']
    source = posted_data['source']

    driver_phone = posted_data['phone'] # NOTE: format 1-###-###-####
    driver_time = posted_data['time']
    driver_token = posted_data['driver_token'].replace(" ", "")
    driver_token = driver_token.replace("<", "")
    driver_token = driver_token.replace(">", "")

    # Process the destination and source format
    destination = destination.split(' ')
    destination = "+".join(destination) + '+New+York,+NY'
    source = source.split(' ')
    source = "+".join(source) + "+New+York,+NY"

    destination_response = requests.get('https://maps.googleapis.com/maps/api/geocode/json?address=' + destination)
    resp_json_payload_dest = destination_response.json()
    driver_ddic = {"lat": resp_json_payload_dest['results'][0]['geometry']['location']['lat'], "lon":resp_json_payload_dest['results'][0]['geometry']['location']['lng']}
    source_response = requests.get('https://maps.googleapis.com/maps/api/geocode/json?address=' + source)
    resp_json_payload_source = source_response.json()
    driver_sdic = {"lat": resp_json_payload_source['results'][0]['geometry']['location']['lat'], "lon":resp_json_payload_source['results'][0]['geometry']['location']['lng']}

    # Obtain all geo coordinates along the way
    url = "https://maps.googleapis.com/maps/api/directions/json?origin=" + source + "&destination=" + destination + "&key=####################################"
    response = urllib.urlopen(url)
    data = json.loads(response.read())

    # Add the driver's start time
    driver_time = driver_time.split(" ")[1].split(":")
    driver_time = (int(driver_time[0])-4)*60*60 + int(driver_time[1])*60

    # Put all data to Dynamodb
    user = init_db()
    try:
        add_to_db(user,data,driver_user, driver_phone, driver_token, driver_sdic["lat"], driver_sdic["lon"], driver_ddic["lat"], driver_ddic["lon"])
    except KeyboardInterrupt:
        exit

    return 'Hello World'

# According to the given customer info, SNS message to user
@app.route('/postclient', methods = ['GET', 'POST', 'PUT'])
def post_client_info():

    dic = dict(request.form)
    posted_string_data = None
    for each in dic: 
        posted_string_data = each
        break

    # print 'string is', str(posted_client_data).split(",")

    # client_list = str(posted_client_data).split(",")

    posted_client_data = json.loads(posted_string_data)

    # Get the posted customer info
    # Three info : source, dest, start time
    client_source = posted_client_data['source']
    client_destination = posted_client_data['dest']
    client_time = posted_client_data['time']
    #add client phone number 1-###-###-####
    client_phone = posted_client_data['phone']
    client_token = posted_client_data['client_token']
    client_token = posted_client_data['client_token'].replace(" ", "")
    client_token = client_token.replace("<", "")
    client_token = client_token.replace(">", "")

    user = init_client_db()
    unique_id = None
    try:
        unique_id = add_to_client_db(user, client_source, client_destination, client_time, client_phone, client_token)
    except KeyboardInterrupt:
        exit

    # Get the longtitude and latitude of customer's source and dest
    # Get the source lng and lat if source exists, otherwise use passed in lng lat
    client_sdic = None
    if posted_client_data['source'] != "":
        client_source = client_source.split(' ')
        client_source = "+".join(client_source)
        source_response = requests.get('https://maps.googleapis.com/maps/api/geocode/json?address=' + client_source)
        resp_json_payload_source = source_response.json()
        client_sdic = {"lat": resp_json_payload_source['results'][0]['geometry']['location']['lat'], "lon": resp_json_payload_source['results'][0]['geometry']['location']['lng']}
    else:
        client_sdic = {"lat": float(posted_client_data['Lat']), "lon": float(posted_client_data['Lng'])}

    # Get destination from the input string
    client_destination = client_destination.split(' ')
    client_destination = "+".join(client_destination)
    destination_response = requests.get('https://maps.googleapis.com/maps/api/geocode/json?address=' + client_destination)
    resp_json_payload_dest = destination_response.json()
    client_ddic = {"lat": resp_json_payload_dest['results'][0]['geometry']['location']['lat'], "lon":resp_json_payload_dest['results'][0]['geometry']['location']['lng']}

    # Calculate the start time, format hh:mm
    client_time = client_time.split(" ")[1].split(":")
    client_time = (int(client_time[0])-4)*60*60 + int(client_time[1])*60

    # Get the driver route info table from Dynamodb, return the potential drivers in "ret" array
    # ret = {}
    final_retdic = {"unique_id":str(unique_id), "clients_lat":str(client_sdic["lat"]), "clients_lon": str(client_sdic["lon"]),"clientd_lat":str(client_ddic["lat"]), "clientd_lon": str(client_ddic["lon"]),"client_time":str(client_time)}
    ret_token = {}
    user=dynamodb.Table('driverRouteInfo')
    response = user.scan()
    candidate = []

    for item in response["Items"]:
        if abs(int(item["time"])-client_time) < 10*60:
            lon = float(item["location"].split("+")[0].encode('utf8'))
            lat = float(item["location"].split("+")[1].encode('utf8'))
            print lon
            print client_sdic["lon"]
            print lat
            print client_sdic["lat"]
            print " "
            if abs(lon - client_sdic["lon"]) < 0.03 and abs(lat - client_sdic["lat"]) < 0.03:
                candidate.append({'username':item["username"],'driver_token': item["driver_token"],'time':int(item["time"]),'drivers_lat':item["drivers_lat"], 'drivers_lon': item["drivers_lon"], 'driverd_lat':item["driverd_lat"], 'driverd_lon': item["driverd_lon"]})
    print candidate
    for can in candidate:    
        for item in response["Items"]:
            if item["username"] == can["username"]:
                lon = float(item["location"].split("+")[0].encode('utf8'))
                lat = float(item["location"].split("+")[1].encode('utf8'))
                if abs(lon - client_ddic["lon"]) < 0.03 and abs(lat - client_ddic["lat"]) < 0.03:
                    if item["time"]>can["time"]:
                        ret_token[can["username"]] = {"driver_token":can["driver_token"],"drivers_lat":can["drivers_lat"],"drivers_lon":can["drivers_lon"], "driverd_lat": can["driverd_lat"], "driverd_lon":can["driverd_lon"]}
    print 'Finding driver matches your route:', ret_token
    if ret_token:
        # Send to client, a simple note
        for each_driver_username in ret_token: # For each driver, send notification
            this_driver_token = ret_token[each_driver_username]
            # A dictionary storing client and driver info; Send to driver
            
            # Put the driver's info into final_retdic, in addition to all client's info added previously; Send to driver
            final_retdic["drivers_lat"] = this_driver_token["drivers_lat"]
            final_retdic["drivers_lon"] = this_driver_token["drivers_lon"]
            final_retdic["driverd_lat"] = this_driver_token["driverd_lat"]
            final_retdic["driverd_lon"] = this_driver_token["driverd_lon"]
            apns_sns_send(this_driver_token["driver_token"], 'You have new match!', final_retdic)
            apns_sns_send(client_token, 'You have a new match!!!', {"matched": "1"})
            print 'DRIVER TOKEN MATCH************************', this_driver_token["driver_token"]
            break
    else:
        apns_sns_send(client_token, 'No match currently',{"matched": "0"})

    return 'Hello World'

def apns_sns_send(token, mes, dic_result):
    token_hex = token
    time.sleep(10)
    payload = Payload(alert=mes, custom = dic_result, sound="default", badge=1)
    apns.gateway_server.send_notification(token_hex, payload)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')