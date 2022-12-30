# Testing fabric as alternative to paramiko
import os

import fabric


def connect_to_dns(dns):
    return fabric.Connection(
        host=dns,
        user="ubuntu",
        connect_kwargs={
            "key_filename": "/home/jjacobs/aws/jeff.pem",
        }
    )


def copy_to_instance(filepath_list, dns):
    c = connect_to_dns(dns)
    for cur_filepath in filepath_list:
        cur_filename = os.path.basename(cur_filepath)
        print("Copying " + str(cur_filename))
        c.put(cur_filepath, remote="/home/ubuntu/" + cur_filename)


def run_commands(dns_list, command_list):
    # Set up the list of connections
    connection_list = []
    for dns_num, cur_dns in enumerate(dns_list):
        c = connect_to_dns(cur_dns)
        env_command = "export INST_NUM=" + str(dns_num)
        c.run(env_command)
        connection_list.append(c)
    tgroup = fabric.group.ThreadingGroup.from_connections(connection_list)

    # Now use this ThreadingGroup to run on all instances in parallel
    result_dict = {}
    for cur_command in command_list:
        print("Running " + cur_command)
        group_result = tgroup.run(cur_command, warn=True)
        # Print+save outputs from *all* instances
        result_buffer = ""
        for cnum, cur_connection in enumerate(connection_list):
            begin_str = "Connection #" + str(cnum) + " result:"
            print(begin_str)
            result_buffer = result_buffer + begin_str

            result_str = group_result[cur_connection].stdout
            print(result_str)
            result_buffer = result_buffer + result_str

            end_str = "*****"
            print("*****")
            result_buffer = result_buffer + end_str
        result_dict[cur_command] = result_buffer
    return result_dict
