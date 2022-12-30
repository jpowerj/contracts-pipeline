# Testing out boto3
import os
import re
import sys

import boto3
import paramiko

instance_reg = r'textlab-36cores-160ram-[0-9]+'


def copy_to_instance(filepath_list, dns):
    # Use paramiko to scp the files over to AWS
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(dns, username="ubuntu", key_filename="/home/jjacobs/aws/jeff.pem")
    sftp = client.open_sftp()
    for cur_filepath in filepath_list:
        print("Copying " + str(cur_filepath))
        remote_filename = os.path.basename(cur_filepath)
        sftp.put(cur_filepath, remote_filename)
    sftp.close()


def get_all_dns():
    instances = get_contract_instances()
    return [cur_inst["PublicDnsName"] for cur_inst in instances
            if cur_inst["PublicDnsName"] != ""]


def get_contract_instances():
    # Helper function, takes the full set of instances and throws away all but
    # the instances that run the contract code
    instance_list = []
    ec2 = boto3.client('ec2')
    response = ec2.describe_instances()
    reservations = response["Reservations"]
    for res_num, cur_reservation in enumerate(reservations):
        #print("Reservation #" + str(res_num))
        res_instances = cur_reservation["Instances"]
        for instance_num, cur_instance in enumerate(res_instances):
            #print("Instance #" + str(instance_num))
            if "Tags" in cur_instance:
                instance_name = cur_instance["Tags"][0]["Value"]
                if re.match(instance_reg, instance_name):
                    instance_list.append(cur_instance)
    # Now sort via the custom sort argument to .sort()
    instance_list.sort(key=lambda x: x["Tags"][0]["Value"])
    return instance_list


def get_dns(instance):
    return instance["PublicDnsName"]


def get_instance_ids(num_instances=4):
    instances = get_contract_instances()
    return [cur_inst["InstanceId"] for inst_num, cur_inst in enumerate(instances)
            if inst_num < num_instances]


def get_running_ips(pl):
    # Uses get_contract_instances() as a helper, then just extracts the ip from
    # each element it returns
    contract_instances = get_contract_instances(pl)
    print(contract_instances[0])
    quit()
    for instance_num, cur_instance in enumerate(res_instances):
        print("Instance #" + str(instance_num))
        if "Tags" in cur_instance and "PublicIpAddress" in cur_instance:
            print(cur_instance)
            print("***")
            instance_name = cur_instance["Tags"][0]["Value"]
            instance_num = int(instance_name.split("-")[-1])
            public_ip = cur_instance["PublicIpAddress"]
            public_dns = cur_instance["PublicDnsName"]
            instance_data = {"name":instance_name,"num":instance_num,
                             "ip":public_ip,"dns":public_dns}
            instance_list.append(instance_data)
    # Now sort via the custom sort argument to .sort()
    instance_list.sort(key=lambda x: x["num"])
    return instance_list


def get_name(instance_data):
    return instance_data["Tags"][0]["Value"]


def start_instances(num_instances=4):
    ec2 = boto3.client('ec2')
    # Get all the instance_ids
    inst_ids = get_instance_ids(num_instances)
    response = ec2.start_instances(InstanceIds=inst_ids)
    # Don't return until all are in state "running"
    num_running = 0
    for cur_inst_id in inst_ids:
        while True:
            status_response = ec2.describe_instance_status(InstanceIds=[cur_inst_id])
            #print(status_response)
            statuses = status_response["InstanceStatuses"]
            if len(statuses) > 0:
                status = statuses[0]["InstanceState"]["Name"]
            else:
                continue
            if status == "running":
                num_running = num_running + 1
                print(str(num_running) + " instances online")
                break


def stop_instances(num_instances=4):
    ec2 = boto3.client('ec2')
    inst_ids = get_instance_ids(num_instances)
    response = ec2.stop_instances(InstanceIds=inst_ids)
    print(response)


def main():
    # Get the command-line arg
    action = sys.argv[1]
    if action == "start":
        start_instances()
    elif action == "list":
        print(get_all_dns())
    elif action == "copy":
        copy_to_instance(None)
    #elif action == "retrieve":
    #    retrieve_from_instances(None)
    elif action == "stop":
        input("Make sure you're not running this by accident! Press Enter to "
              + "continue or Ctrl+C to exit...")
        stop_instances()
    else:
        print("Invalid argument!")


if __name__ == "__main__":
    main()