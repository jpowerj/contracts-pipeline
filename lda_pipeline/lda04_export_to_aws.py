# Exports the necessary files to the various AWS instances, before
# remote_control_aws runs them
import datetime
import glob
import os

import aws_api
import fabric_api

dns_template = "DNS[{i}]=\"{the_dns}\""
env_var_template = "export DNS{i}=\"{dns}\""
ssh_template = "ssh -i /home/jjacobs/aws/jeff.pem -o StrictHostKeyChecking=no ubuntu@{dns} \"mkdir obranch_lda; mv 02* 03* obranch_lda; /home/ubuntu/anaconda3/bin/conda install -y -q fabric; /home/ubuntu/anaconda3/bin/python canadian_aws_corpus_run.py {inst_num};\""

def export_to_aws(pl, custom_glob=None, instances=None):
    # Set to True if you want to export the files via shell script rather than
    # within Python
    create_shell_script = False
    if custom_glob:
        pl.iprint("Using custom glob: \"" + custom_glob + "\"")
    # First we have to launch the aws instances
    inst_ids = aws_api.start_instances(pl.num_lda_subsets)
    # Get their DNS addresses
    dns_list = aws_api.get_all_dns()
    if create_shell_script:
        # Print the DNS array for the aws_corpus_export.sh file
        for dns_num, cur_dns in enumerate(dns_list):
            print(dns_template.format(i=dns_num, the_dns=cur_dns))
        # Now print the export commands that will store the DNS names
        for dns_num, cur_dns in enumerate(dns_list):
            print(env_var_template.format(i=dns_num, dns=cur_dns))
        # Print the ssh commands for the ssh_run_aws.sh file
        ssh_list = []
        for dns_num, cur_dns in enumerate(dns_list):
            ssh_list.append("( " + ssh_template.format(dns=cur_dns, inst_num=dns_num) + " ) & ")
        # Ugh
        ssh_str = "".join(ssh_list)
        print(ssh_str)
    else:
        # Do the exporting directly through python
        fpath_list = []
        # Standard set of files
        fpath_list.append(os.path.join("configs","canadian_dominik.conf"))
        lda_fpath_list = glob.glob(os.path.join("lda_pipeline","lda*"))
        fpath_list.extend(lda_fpath_list)
        fpath_list.append("pipeline.py")
        fpath_list.append("pipeline_util.py")
        fpath_list.append("canadian_aws_corpus_run.py")
        fpath_list.append("aws_api.py")
        fpath_list.append("fabric_api.py")
        #fpath_list.append(pl.get_lda_dict_fpath())
        # And the actual exporting
        for dns_num, cur_dns in enumerate(dns_list):
            if dns_num not in instances:
                continue
            print("Copying to instance #" + str(dns_num))
            # Need to make sure to copy the specific doclist file
            cur_fpath_list = fpath_list.copy()
            #cur_fpath_list.append(pl.get_lda_doclist_fpath(dns_num))
            fabric_api.copy_to_instance(cur_fpath_list, cur_dns)

# def export_to_aws_nofabric(pl, custom_glob=None):
#     run_remotely = False
#     if custom_glob:
#         pl.iprint("Using custom glob: \"" + custom_glob + "\"")
#     # First we have to launch the aws instances
#     inst_ids = aws_api.start_instances()
#     # Get their DNS addresses
#     dns_list = aws_api.get_all_dns()
    
#     ### STEP 1: Copy files over
#     # Build a file list
#     filepath_list = []
#     if not custom_glob:
#         # Standard set of files
#         filepath_list.append("configs/canadian_dominik.conf")
#         lda_filepath_list = glob.glob("lda_pipeline/lda*")
#         filepath_list.extend(lda_filepath_list)
#         filepath_list.append("pipeline.py")
#         filepath_list.append("pipeline_util.py")
#         filepath_list.append("canadian_aws_corpus_run.py")
#         filepath_list.append(pl.get_lda_dict_fpath())
#         filepath_list.append(pl.get_preprocessed_df_fpath())
#     else:
#         # Use the custom glob
#         custom_list = glob.glob(custom_glob)
#         filepath_list.extend(custom_list)
#     # Now copy the files over.
#     for cur_instance_num in range(pl.num_lda_subsets):
#         pl.iprint("Copying to instance #" + str(cur_instance_num))
#         cur_dns = dns_list[cur_instance_num]
#         cur_filepath_list = filepath_list.copy()
#         if not custom_glob:
#             # Also copy over the specific doclist for this instance
#             cur_filepath_list.append(pl.get_lda_doclist_fpath(doclist_num=cur_instance_num))
#         fabric_api.copy_to_instance(cur_filepath_list, cur_dns)
#         pl.iprint("Successfully copied files to instance #" + str(cur_instance_num))
