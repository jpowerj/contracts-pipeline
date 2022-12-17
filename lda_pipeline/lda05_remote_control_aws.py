# Use Fabric to run construct_corpus on AWS remotely
import datetime
import os

import pipeline_util as plu
import aws_api
import fabric_api

def remote_control_aws(pl):
    dns_list = aws_api.get_all_dns()
    # Run the python file remotely on each instance
    commands = [
        "mkdir lda",
        "mv 02* 03* lda",
        "/home/ubuntu/anaconda3/bin/python canadian_aws_corpus_run.py $INST_NUM",
        "scp obranch_lda/05* jjacobs@textlab.econ.columbia.edu:~/ashmacnaidu/canadian_data/obranch_lda/"
    ]
    result_dict = fabric_api.run_commands(dns_list, commands)
    print("DNS info:")
    print([(dns_num, cur_dns) for dns_num, cur_dns in enumerate(dns_list)])
    timestamp = str(datetime.datetime.now()).replace(" ","_").split(".")[0]
    result_fpath = os.path.join(".","logs","aws_output_" + timestamp + ".pkl")
    print("Saving result_dict to " + result_fpath)
    plu.safe_to_pickle(result_dict, result_fpath)

def main():
    print("For testing only!")

if __name__ == "__main__":
    main()