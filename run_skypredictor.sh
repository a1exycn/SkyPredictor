#!/bin/bash
# Configuration and launch-script for SkyPredictor

###################################################################
# configure and export variables; please only change this section #
###################################################################

# Cron schedule for fetching data
export BASELINE_FETCH_CRON="0 0 * * *"      

# OAP address for fetching data
export BASELINE_FETCH_SERVER_ENDPOINT="http://swnova.zlfang.com"    

# Downsampling interval for fetched data
export BASELINE_FETCH_SERVER_DOWN_SAMPLING="MINUTE"           

# Metrics to fetch from the server and predict for
export BASELINE_FETCH_SERVER_METRICS="service_cpm"             

# a VictoriaMetrics DB URL for writing predictions
export BASELINE_PREDICT_URL_WRITE="http://127.0.0.1:8428"           

# a VictoriaMetrics DB URL for reading predictions
export BASELINE_PREDICT_URL_READ="http://127.0.0.1:8428"      

# Frequency unit of predictions      
export BASELINE_PREDICT_FREQUENCY="min"         

# Period for which predictions are made              
export BASELINE_PREDICT_PERIOD="1440"    

# Offset for predictions      
export BASELINE_PREDICT_OFFSET="0"                   

# Set to true if pulling from OAP with the same format as the provided local test OAP, false otherwise               
export OAP_LOCAL="false"              

# Set to true to include historical data in the prediction results, false otherwise
export INCLUDE_HISTORICAL="false"          

###############
# section end #
###############

# Print all BASELINE_ variables
echo "Configuration:"
for var in $(compgen -v BASELINE_); do
    printf "%-35s = %s\n" "$var" "${!var}"
done

# launch the server - make sure you are in the right env
python -m server.server