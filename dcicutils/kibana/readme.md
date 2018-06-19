# Kibana related graphing and dashboard stuff

 - dashboard.json - complete set of saved searches, viz, and dashboards used in kibana.  Stored here for easy importing into other kibana environments.

 ## Import into kibana

 From Kibana dashboard ... *Management -> Saved Objects -> Import* select dashboards.json and import... if you get a bunch of errors about not finding stuff like duration, index_count etc... you need to have some filebeats data loaded (and the index refresed) before importing the dashboard.... make sure that is there first and then re-import.



# backup / restore of indices from AWS auto snapshots
https://docs.aws.amazon.com/elasticsearch-service/latest/developerguide/es-managedomains-snapshots.html

For Elasticsearch 5.x, Amazon Elasticsearch monitors and recreates .kibana index when it's deleted. 

Due to this behaviour, you may experience the following error when you are trying to restore indices. Please correct me if my understanding is not right.

$ curl -XPOST  https://<domain-end-point>/_snapshot/<repository-name>/<snapsnot-name>/_restore

{"error":{"root_cause":[{"type":"snapshot_restore_exception","reason":"[<repository-name>:<snapshot-name>/1A2B34aZQFWQpFOYYJfxmQ]
 cannot restore index [.kibana] because it's 
open"}],"type":"snapshot_restore_exception","reason":"[<repository-name>:<snapshot-name>/1A2B34aZQFWQpFOYYJfxmQ]
 cannot restore index [.kibana] because it's open"},"status":500}

To resolve this issue, you can rename .kibana index at restore with "rename_pattern" and "rename_replacement".

After you restore .kibana as another name, you can rename it to .kibana with reindex API.

# restore indices.   
$ curl -XPOST https://<domain-end-point>/_snapshot/<repository-name>/<snapshot-name>/_restore -d '
{                                             
  "indices": ".kibana",
  "ignore_unavailable": true,
  "include_global_state": true,
  "rename_pattern": ".kibana",
  "rename_replacement": "restored_.kibana"
}'

# reindex restored_.kibana to .kibana 
$ curl -XPOST https://<domain-end-point>/_reindex -d '
{
  "source": {
    "index": "restored_.kibana"
  },
  "dest": {
    "index": ".kibana"
  }
}'
