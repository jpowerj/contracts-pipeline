# Generates the authority graphs using matplotlib
import math
import os

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

import pipeline_util as plu

#from global_functions import (debugPrint, safePickle)
#from global_vars import (LDA_NUM_TOPICS, LDA_PATH, OUTPUT_PATH)

# v1
#auth_list = ["obligation","constraint","permission","entitlement"]
#topic_list = [1,2,3,6,7,8,9,10,12,13,14,15]
#topic_labels = {1:"Grievances",2:"Firing",3:"Vacation Leave",6:"Quitting",7:"Overtime",8:"Safety",9:"Insurance/Benefits",10:"Scheduling",12:"Holidays/Sick Leave",13:"Teacher Qualifications",14:"Seniority",15:"Salary"}

#v2. 4x5 grid
#auth_list = ["obligation","permission"]
#topic_list = [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19]
#topic_labels = {1:"Sick Leave",2:"Parental Leave",3:"Service Fee",4:"Deductions",5:"Barg. Unit",6:"Collective Bargaining",7:"Overtime",8:"Grievances",9:"Job Training",10:"Vacation",11:"Transfer",12:"Payment Schedule",13:"Overtime",14:"Safety",15:"Discipline",16:"Seniority",17:"Bereavement Leave",18:"Benefits",19:"Scheduling"}



def compute_sums(pl, subnorm):
    # Load the two weight files for the subject and compute weighted
    # permission, obliation, entitlement, and constraint
    print("Computing sums for subnorm " + subnorm)
    #weights_fpath = pl.get_weighted_auth_fpath()
    #weight_header = ["weight_topic_" + str(n) for n in range(num_topics)]
    #weight_df = pd.read_csv(weights_fpath)
    with open("test.txt", "w") as outfile:
        outfile.write("I AM HERE")
    weights_fpath = pl.get_weighted_auth_fpath(extension="pkl")
    weights_fpath = os.path.join(pl.get_lda_path(), "cropped_weights_df.pkl")
    weight_df = pd.read_pickle(weights_fpath)
    # Sum obligation + constraint to get obligation, sum permission+entitlement
    # to get permission
    # Existing vars
    obligation_var = subnorm + "_obligation"
    constraint_var = subnorm + "_constraint"
    permission_var = subnorm + "_permission"
    entitlement_var = subnorm + "_entitlement"
    # New vars
    combined_ob_var = subnorm + "_combined_ob"
    combined_perm_var = subnorm + "_combined_perm"
    vars_to_keep = [obligation_var, constraint_var, permission_var, entitlement_var, combined_ob_var, combined_perm_var]

    # permission_var = "worker_permission" 
    # combined_perm_var = worker + "_combined_perm"

    weight_df[combined_ob_var] = weight_df[obligation_var] + weight_df[constraint_var]
    weight_df[combined_perm_var] = weight_df[permission_var] + weight_df[entitlement_var]
    
    # This will map topics into sums of authority measures
    weight_dict = {}
    weight_dict["N"] = len(weight_df.index)
    for i in range(pl.num_topics):
        print ("compute_sums", i)
        weight_var = "topic_weight_" + str(i)
        # Now make a new dataframe containing just the weighted auth measures
        # See http://stackoverflow.com/questions/23195250/create-empty-dataframe-with-same-dimensions-as-another
        print ("        auth_df = pd.DataFrame().reindex_like(weight_df)".strip())
        auth_df = pd.DataFrame().reindex_like(weight_df)
        # Weighted obligation
        weighted_ob_var = combined_ob_var + "_" + str(i)
        print ("        weighted_ob_var = combined_ob_var + _ + str(i)".strip())
        auth_df[weighted_ob_var] = weight_df[combined_ob_var] * weight_df[weight_var]
        # Weighted permission
        print ("        auth_df[weighted_ob_var] = weight_df[combined_ob_var] * weight_df[weight_var]".strip())
        weighted_perm_var = combined_perm_var + "_" + str(i)
        print ("        weighted_perm_var = combined_perm_var + _ + str(i)")
        auth_df[weighted_perm_var] = weight_df[combined_perm_var] * weight_df[weight_var]
        
        # Now sum up the weighted vars for this topic
        num_rows = len(auth_df.index)
        auth_sum = auth_df.sum()
        # Now we don't need a whole dataframe, just a dict that contains the sums
        # as well as the number of rows N (since the final thing we'll graph is the
        # sum / N)
        cur_dict = {}
        cur_dict["obligation"] = auth_sum[weighted_ob_var]
        cur_dict["permission"] = auth_sum[weighted_perm_var]
        weight_dict[i] = cur_dict
    # Now pickle weight_dict
    # get_snauth_weights_fpath
    weight_dict_fpath = pl.get_snauth_weights_fpath(subnorm)
    #plu.safe_save_data(weight_dict, weight_dict_fpath)
    joblib.dump(weight_dict, weight_dict_fpath)
    return weight_dict

def draw_plot(pl, subject, sums_dict):
    # [Used to be global vars, changed 2019-02-19 -JJ]
    auth_list = ["obligation","permission"]
    topic_list = list(range(pl.num_topics))
    topic_labels = {t_num:"No Label" for t_num in topic_list}
    #topic_labels = ["schedulling", "indefinable", "food", "french sections", "insurance", "retirement", "maternity leave", "further education", "health insurance", "settlement", "legal issues", "strike", "safety conditions", "salary", "contract", "organizational", "sexual harrassment", "disciplinary actions", "reward", "disabilities"]
    topic_labels = ['scheduling', 'care', 'vacation', 'harrassment', 'complaint', 'activities', 'disabilities', 'membership', 'strike', 'pension', 'holidays', 'work safety', 'varia', 'parenthood', 'time', 'family', 'jurisdiction', 'scheduling', 'further education', 'rules']
    show_labels = False
    total_N = sums_dict["N"]

    # 4x5 grid
    plots_per_row = 5
    fig, ax_arr = plt.subplots(4,plots_per_row)
    #ax.ticklabel_format(useOffset=False)
    #if subject == "worker":
    #    plt.ylim(0, 0.1) 
    #elif subject == "manager":
    #    plt.ylim(0, 0.02)
    for topic_index in range(len(topic_list)):

        print (topic_index)
        cur_topic = topic_list[topic_index]
        topic_sums = sums_dict[cur_topic]
        topic_ob = topic_sums["obligation"]
        topic_perm = topic_sums["permission"]
        # Normalize
        topic_ob_normalized = topic_ob / total_N
        topic_perm_normalized = topic_perm / total_N
        #firm_1 = firm_sums[1]
        #firm_1_list = [(firm_1["obligation"]+firm_1["constraint"])/2,(firm_1["permission"]+firm_1["entitlement"])/2]
        ind = np.arange(1)
        width = 0.35
        grid_x = math.floor(topic_index / plots_per_row)
        grid_y = topic_index % plots_per_row
        ax_arr[grid_x,grid_y].ticklabel_format(useOffset=False, axis="y", style="plain")

        ob_rect = ax_arr[grid_x,grid_y].bar(1, topic_ob_normalized, width)
        perm_rect = ax_arr[grid_x,grid_y].bar(1 + width, topic_perm_normalized, width)
        #ax.legend((worker_rects[0], firm_rects[0]), ('Worker', 'Firm'))
        #title_text = "Topic " + str(cur_topic) 
        title_text = topic_labels[cur_topic]
        if show_labels:
            title_text = title_text + ": " + topic_labels[cur_topic]
        ax_arr[grid_x,grid_y].set_title(title_text,fontsize=7)
        ax_arr[grid_x,grid_y].set_xticks(ind + width / 2)
        ax_arr[grid_x,grid_y].set_xticklabels([])
        """
        if subject == "worker":
            ax_arr[grid_x,grid_y].set_ylim([0, 0.2])
        if subject == "manager":
            ax_arr[grid_x,grid_y].set_ylim([0, 0.02])
        """
        #ax.set_xticklabels(('Mean(Obligation,Constraint)', 'Mean(Permission,Entitlement)'),rotation=-45)
    # Uncomment to make the last (20th) slot blank
    #ax_arr[-1,-1].axis("off")
    fig.legend((ob_rect, perm_rect), ('Obligation', 'Entitlement'),
      loc="lower center", bbox_to_anchor=(0.5,0),fancybox=True,shadow=True,ncol=2)
    suptitle_text = "Subject: " + subject.capitalize() + " (N = " + str(total_N) + " statements)"
    plt.suptitle(suptitle_text)
    plt.tight_layout(w_pad=0.5)
    fig.subplots_adjust(top=0.9,bottom=0.1)
    #plt.show()
    #plt.savefig(os.path.join(pl.get_output_path(), subject + ".png"))
    plt.savefig(os.path.join(pl.get_output_path(), subject + "_correct_topics_normalized.png"))
    plt.clf()
    # And save it to a .png
    
def generate_auth_graphs(pl):
    #for cur_subject in ["employer","employee"]:
    for cur_subject in ["worker","manager"]:
        #topic_sums = compute_sums(pl, cur_subject)
        #topic_sums = joblib.load(os.path.join(pl.get_lda_path(), "13_canadian_" + cur_subject + "_weights.pkl"))
        topic_sums = joblib.load(os.path.join(pl.get_lda_path(), "final_summed_" + cur_subject + "_weights.pkl"))

        #print("worker_sums:")
        #print(worker_sums)
        #print("firm_sums:")
        #print(firm_sums)

        draw_plot(pl, cur_subject, topic_sums)
    
if __name__ == "__main__":
    #compute_sums("worker")
    #compute_sums("firm")
    #compute_sums("union")
    #compute_sums("manager")

    combine_subjects("worker","union","employee")
    combine_subjects("firm","manager","employer")
    generate_plot("firm")
    generate_plot("employee")
