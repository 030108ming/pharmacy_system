
# create enve
    python3 -m venv /Users/shaomingpan/UTA_project/my_envs/pharmacy_env

    source /Users/shaomingpan/UTA_project/my_envs/pharmacy_env/bin/activate

# then  pip install requirements
 pip install -r requirements.txt

# get grpc code

chmod +x generate_proto.sh && ./generate_proto.sh

# docker come up
    docker-compose up --build -d

# then  test
    sleep 15 && python client/test_client.py

#  evaluation
    cd evaluation && python benchmark.py
# plot results
    python plot_results.py

# check container status
    cd .. && docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"