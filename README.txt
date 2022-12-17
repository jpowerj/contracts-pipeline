# to run pipeline to compute authority measures

python pipeline_canadian.py 

# overwrite this line to run pipeline on customized data, and instantiate a new config file in configs with the same name, have a look at e.g., configs/canadian.config

currently, canadian.config output path is the following sample contracts

OUTPUT_PATH=/cluster/work/lawecon/Work/dominik/powerparser/output_rerun_pipeline

change this such that it points to the desired output

#CAUTION
the output path already must contain a folder 01_artsplit in which we have the split articles as json files!!!
I got these articles from jeff and wouldn't know how to compute them!

to run LDA

python pipeline_lda.py

this is mostly jeff's code, for a cleaner version of the code, and a README on how to install requirements etc., have a look at
https://github.com/dominiksinsaarland/labor-contracts
