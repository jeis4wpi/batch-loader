# GW ScholarSpace ingest configuration
ingest_path = "/opt/scholarspace/scholarspace-hyrax"
ingest_command = "rvmsudo RAILS_ENV=production rake gwss:ingest_etd"
ingest_depositor = "openaccess@gwu.edu"

debug_mode = False

# set to True to use HTTP authentication for downloading files
auth_enable = False
auth_user = "username"
auth_pass = "secret"