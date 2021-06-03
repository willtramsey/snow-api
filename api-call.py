from update_backlog_blob import *
import api_secrets

update_backlog_blob(api_secrets.euc_incident_url, 'euc_incidents')
update_backlog_blob(api_secrets.euc_task_url, 'euc_tasks')
update_backlog_blob(api_secrets.support_url, 'support_incidents')