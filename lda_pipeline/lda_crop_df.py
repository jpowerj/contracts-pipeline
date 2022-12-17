import pandas as pd
#weights_fpath = "/cluster/work/lawecon/Work/dominik/powerparser/output_all_data/obranch_lda_k20/11_canadian_weighted_auth.pkl"
#weights_fpath = "/cluster/work/lawecon/Work/dominik/powerparser/output_canadian_new/obranch_lda_k20/11_canadian_weighted_auth.pkl"
weights_fpath = "/cluster/work/lawecon/Work/dominik/powerparser/output_canadian_coref/obranch_lda_k20/11_canadian_weighted_auth.pkl"
weight_df = pd.read_pickle(weights_fpath)

variables = []
for subnorm in ["worker","manager"]:
	obligation_var = subnorm + "_obligation"
	constraint_var = subnorm + "_constraint"
	permission_var = subnorm + "_permission"
	entitlement_var = subnorm + "_entitlement"
	variables.append(obligation_var)
	variables.append(constraint_var)
	variables.append(permission_var)
	variables.append(entitlement_var)
	# New vars
	combined_ob_var = subnorm + "_combined_ob"
	combined_perm_var = subnorm + "_combined_perm"
for i in range(20):
	print ("compute_sums", i)
	weight_var = "topic_weight_" + str(i)
	# weight_df[weight_var]
	variables.append(weight_var)

small_df = weight_df[variables]
#small_df.to_pickle("/cluster/work/lawecon/Work/dominik/powerparser/output_all_data/obranch_lda_k20/cropped_weights_df.pkl")
#small_df.to_pickle("/cluster/work/lawecon/Work/dominik/powerparser/output_canadian_new/obranch_lda_k20/cropped_weights_df.pkl")
small_df.to_pickle("/cluster/work/lawecon/Work/dominik/powerparser/output_canadian_coref/obranch_lda_k20/cropped_weights_df.pkl")


# size before cropping: 33428879616
# size after cropping:   3530664921
