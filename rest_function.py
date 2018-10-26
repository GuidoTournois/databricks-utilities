# Databricks notebook source
import requests
import json

# COMMAND ----------

def decode(response):
  return json.loads(response.content.decode('utf-8'))

def get_request(url, **kwargs):
  auth = {'Authorization': 'Bearer ' + dbutils.secrets.get('api_key','token')}
  if len(kwargs):
    url += '?'
    for key, value in kwargs.items():
      url+=key+'='+str(value)+'&'
  return decode(requests.get(url, headers=auth))

def post_request(url, data):
  auth = {'Authorization': 'Bearer ' + dbutils.secrets.get('api_key','token')}
  return decode(requests.post(url, json = data, headers=auth))

# COMMAND ----------

class group_api:
  def create_group(groupname):
    content = {'group_name' : groupname}
    url = "https://westeurope.azuredatabricks.net/api/2.0/groups/create"
    response = post_request(url,content)
    try:
      message = response['message']
    except:
      message = "group {} successfully created".format(groupname)
    print(message)
  
  def delete_group(groupname):
    content = {'group_name' : groupname}
    url = "https://westeurope.azuredatabricks.net/api/2.0/groups/delete"
    response = post_request(url,content)
    try:  
      message = response['message']
    except:
      message = "group {} successfully deleted".format(groupname)
    print(message)
  
  def list_groups():
    url = "https://westeurope.azuredatabricks.net/api/2.0/groups/list"
    response = get_request(url)
    return response['group_names']

  def list_members_of_group(groupname):
    url = "https://westeurope.azuredatabricks.net/api/2.0/groups/list-members"
    return get_request(url, group_name=groupname)

  def add_member_to_group(member,groupname):
    content = {
      "user_name": member,
      "parent_name": groupname
    }
    url = "https://westeurope.azuredatabricks.net/api/2.0/groups/add-member"
    response = post_request(url,content)
    try:  
      message = response['message']
    except:
      message = "{} successfully added to {}".format(member,groupname)
    print(message)
    
  def add_member_to_group(member,groupname):
    content = {
      "user_name": member,
      "parent_name": groupname
    }
    url = "https://westeurope.azuredatabricks.net/api/2.0/groups/add-member"
    response = post_request(url, content)
    try:  
      message = response['message']
    except:
      message = "{} successfully added to {}".format(member,groupname)
    print(message)

# COMMAND ----------

class job_api:
  def get_clusters():
    url = "https://westeurope.azuredatabricks.net/api/2.0/clusters/list"
    return get_request(url)
  
  def create_job(name, notebook, cluster_id, time_out=3600):
    url = "https://westeurope.azuredatabricks.net/api/2.0/jobs/create"
    content = {
      "name": name,
      "existing_cluster_id": cluster_id,
      "timeout_seconds": time_out,
      'notebook_task': {
        'notebook_path' : notebook}
    }
    return post_request(url, content)
  
  def run_job(job_id):
    url = "https://westeurope.azuredatabricks.net/api/2.0/jobs/run-now"
    content = { "job_id": job_id}
    return post_request(url, content)
  
  def get_result_of_a_run(run_id):
    state = 'RUNNING'
    while state!='TERMINATED':
      url = "https://westeurope.azuredatabricks.net/api/2.0/jobs/runs/get"
      state = get_request(url, run_id=run_id)['state']['life_cycle_state']

    url = "https://westeurope.azuredatabricks.net/api/2.0/jobs/runs/get-output"
    return get_request(url, run_id=run_id)['notebook_output']['result']
  
  