# Kibana related graphing and dashboard stuff

 - dashboard.json - complete set of saved searches, viz, and dashboards used in kibana.  Stored here for easy importing into other kibana environments.

 ## Import into kibana

 From Kibana dashboard ... *Management -> Saved Objects -> Import* select dashboards.json and import... if you get a bunch of errors about not finding stuff like duration, index_count etc... you need to have some filebeats data loaded (and the index refresed) before importing the dashboard.... make sure that is there first and then re-import.


