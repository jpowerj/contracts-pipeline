# by Dominik Stammbach

# LDA pipeline is buggy, thus here some last minute fixes before submission of the paper
import pandas as pd

# we need the corpus
corpus = pd.read_pickle("/cluster/work/lawecon/Work/dominik/powerparser/output_canadian_coref/obranch_lda_k20/03_canadian_ldadoclist_0.pkl")

# actually, we don't need the corpus, we just need contract_id and articlue_num
corpus = corpus[['contract_id', 'article_num']]
# and we need the summed authority_measures which we have computed before, so let's load them
auth_sums = pd.read_pickle("../05_canadian_authsums_sections.pkl")



# In [41]: result = pd.merge(left, right, on='key')
# we need the topic weights per contract_id and article_num
weights = pd.read_pickle("09_canadian_lda_weights.pkl")
# and then, we want to concatenate
concat = pd.concat([corpus, weights], axis=1)

# and merge the weights with the summed authority measures
weight_df = pd.merge(concat, auth_sums, on=['contract_id', 'article_num'])


# then, we can compute the weight_dict per topic
# what is done in lda13_sum_weighted_auths.py makes no sense, is computationally crazy (takes 1 hour per plot) and delivers the wrong results
def compute_sums(subnorm):
	num_topics = 20
	obligation_var = "obligation_" + subnorm
	constraint_var = "constraint_" + subnorm
	permission_var = "permission_" + subnorm
	entitlement_var = "entitlement_" + subnorm
	# New vars
	combined_ob_var = subnorm + "_combined_ob"
	combined_perm_var = subnorm + "_combined_perm"
	weight_df[combined_ob_var] = weight_df[obligation_var] + weight_df[constraint_var]
	weight_df[combined_perm_var] = weight_df[permission_var] + weight_df[entitlement_var]
	weight_dict = {}
	weight_dict["N"] = len(weight_df.index)
	for i in range(num_topics):
		print (i)
		weight_var = "topic_weight_" + str(i)
		weighted_ob_var = combined_ob_var + "_" + str(i)
		print ("        weighted_ob_var = combined_ob_var + _ + str(i)".strip())
		weight_df[weighted_ob_var] = weight_df[combined_ob_var] * weight_df[weight_var]
		# Weighted permission
		print ("        weight_df[weighted_ob_var] = weight_df[combined_ob_var] * weight_df[weight_var]".strip())
		weighted_perm_var = combined_perm_var + "_" + str(i)
		print ("        weighted_perm_var = combined_perm_var + _ + str(i)")
		weight_df[weighted_perm_var] = weight_df[combined_perm_var] * weight_df[weight_var]
		auth_sum_ob = weight_df[weighted_ob_var].sum() #/weight_df[weight_var].sum()
		auth_sum_perm = weight_df[weighted_perm_var].sum() #/ weight_df[weight_var].sum()
		cur_dict = {}
		cur_dict["obligation"] = auth_sum_ob
		cur_dict["permission"] = auth_sum_perm
		weight_dict[i] = cur_dict
	return weight_dict

# so, let's compute weights
sum_worker = compute_sums("worker")
sum_manager = compute_sums("manager")
import joblib
joblib.dump(sum_worker, "final_summed_" + "worker" + "_weights.pkl")
joblib.dump(sum_manager, "final_summed_" + "manager" + "_weights.pkl")

# we probably also want a map of the topics ranked by permission and obligation
def get_max(weight_dict):
	x = []
	for i,j in weight_dict.items():
		if i == "N":
			continue
		x.append((i,j["obligation"], j["permission"]))
	print (x)
	y = ['scheduling', 'care', 'vacation', 'harrassment', 'complaint', 'activities', 'disabilities', 'membership', 'strike', 'pension', 'holidays', 'work safety', 'varia', 'parenthood', 'time', 'family', 'jurisdiction', 'scheduling', 'further education', 'rules']
	# if obligation, lambda x: x[-2], if permission: lambda x:x[-1]
	print ([y[i[0]] for i in sorted(x, key=lambda x: x[-1])])

get_max(sum_worker)
get_max(sum_manager)

