import os
import glob
import pandas as pd

file = "/eresearch/lung/jjoh182/COPDgene/COPDGene_Phase1_SM_NS_25OCT21.txt"

subjects1 = "QtD_6015.csv"
pvv_subs = '/eresearch/copdgene/jjoh182/COPD_QtD_results/Output_csvs/vessel_like.csv'
# subjects2 = "/eresearch/copdgene/jjoh182/COPD_QtD_results/Output_csvs/allQtDs.csv"
# prism = "/home/jjoh182/PythonProjectMaster/PRISm_BPV/wholeLung_merged.csv"
qtd520 = "/home/jjoh182/PythonProjectMaster/QtD_COPD_outputs/QtD_520.csv"

sub_list1 = pd.read_csv(subjects1)
# sub_list2 = pd.read_csv(subjects2)

# sub_list = pd.concat([sub_list1,sub_list2],ignore_index=True)
# sub_list = sub_list.drop_duplicates()

sub_list = sub_list1
# sub_list = pd.read_csv(prism)
sub_list = pd.read_csv(qtd520)

data = pd.read_csv(file,sep="\t")

# sub_list = sub_list.rename(columns={'CID':'sid'})
sub_list = sub_list.rename(columns={'file':'sid'})
# retrieve_list = ['sid','cohort','gender','race','ethnic','smoking_status','age_visit','BMI','finalGold',
#                  'Height_CM','Weight_KG','HR','TLC_CT','FRC_CT','pctEmph_Thirona',
#                  'DLco_GLI_tr_pp','VA_pp','Kco_tr_pp','FEV1pp_post','FVCpp_post','TLC_pp_plethy',
#                  'FEV1_FVC_post','MeanAtten_Insp_total_Thirona','Insp_totalvolume_total_Thirona']

retrieve_list = ['sid','cohort','gender','race','ethnic','smoking_status','age_visit','BMI','finalGold',
                 'Height_CM','Weight_KG','HR','TLC_CT','FRC_CT','pctEmph_Thirona',
                 'DLco_GLI_tr_pp','VA_pp','Kco_tr_pp','FEV1pp_post','FVCpp_post','TLC_pp_plethy',
                 'FEV1_FVC_post','MeanAtten_Insp_total_Thirona','Insp_totalvolume_total_Thirona',
                 'MMRCDyspneaScor','SGRQ_scoreSymptom','SGRQ_scoreActive','SGRQ_scoreImpact','SGRQ_scoreTotal',
                 'SF36_PF_t_score','SF36_RP_t_score','SF36_RE_t_score','SF36_SF_t_score','SF36_BP_t_score',
                 'SF36_VT_t_score','SF36_MH_t_score','SF36_GH_t_score',#'SF36_PCS_t_score','SF36_MCS_t_score',
                 'BDR_pct_FEV1','BDR_pct_FVC','Exacerbation_Frequency','NewGOLD_SGRQ',#'AWT_Seg_Thirona','Walk_Total'
                 'O2_suppl_6MW','distwalked','Walk_Course']
#TO RETRIEVE LONGITUDINAL DATA
retrieve_list2 = ['sid',
                  'Change_P1_P2_Gold_class','Change_P1_P2_FEV1pp','Change_P1_P2_MMRC','Change_P1_P2_SGRQ_total',
                  'Change_P1_P2_distwalked','Change_P1_P2_O2','Change_TLC_Thirona','Change_pctEmph_Thirona',
                  'Change_FRC_Thirona','Change_pctGasTrap_Thirona','Change_Perc15_Insp_Thirona',
                  'Change_Perc15_Exp_Thirona','Change_MeanAtten_Insp_Thirona','Change_MeanAtten_Exp_Thirona',
                  'Change_Adj_Density_plethy','Change_Adj_Density_mesa','Change_pctEmph_LUL_Thirona',
                  'Change_pctEmph_LLL_Thirona','Change_pctEmph_RUL_Thirona','Change_pctEmph_RML_Thirona',
                  'Change_pctEmph_RLL_Thirona','Change_PRM_pct_emphysema_Thirona','Change_PRM_pct_airtrap_Thirona',
                  'Change_PRM_pct_normal_Thirona','Change_PRM_pct_other_Thirona']

# rows=list(sub_list['sid'])
# subset = data[data['sid'].isin(rows)]
# subset_select = subset[retrieve_list]
# subset_select.to_csv("/eresearch/copdgene/jjoh182/COPD_QtD_results/Output_csvs/Demographics_allQtDs2.csv",index=None)

merged_df = pd.merge(sub_list, data[retrieve_list], on='sid', how='left')
# merged_df.to_csv("/home/jjoh182/PythonProjectMaster/PRISm_BPV/Demographics_all.csv",index=False)
merged_df.to_csv("/home/jjoh182/PythonProjectMaster/QtD_COPD_outputs/Demo_outcomes_520.csv",index=False)

print(merged_df)

###########################
# to retrieve longitudinal
file2 = "/eresearch/lung/jjoh182/COPDgene/COPDGene_P1P2_SM_NS_25OCT21.txt"

data2 = pd.read_csv(file2,sep="\t",low_memory=False)

#TO RETRIEVE LONGITUDINAL DATA
retrieve_list2 = ['sid',
                  'Change_P1_P2_Gold_class','Change_P1_P2_FEV1pp','Change_P1_P2_MMRC','Change_P1_P2_SGRQ_total',
                  'Change_P1_P2_distwalked','Change_P1_P2_O2','Change_TLC_Thirona','Change_pctEmph_Thirona',
                  'Change_FRC_Thirona','Change_pctGasTrap_Thirona','Change_Perc15_Insp_Thirona',
                  'Change_Perc15_Exp_Thirona','Change_MeanAtten_Insp_Thirona','Change_MeanAtten_Exp_Thirona',
                  'Change_Adj_Density_plethy','Change_Adj_Density_mesa','Change_pctEmph_LUL_Thirona',
                  'Change_pctEmph_LLL_Thirona','Change_pctEmph_RUL_Thirona','Change_pctEmph_RML_Thirona',
                  'Change_pctEmph_RLL_Thirona','Change_PRM_pct_emphysema_Thirona','Change_PRM_pct_airtrap_Thirona',
                  'Change_PRM_pct_normal_Thirona','Change_PRM_pct_other_Thirona']

merged_df2 = pd.merge(sub_list, data2[retrieve_list2], on='sid', how='left')
merged_df2.to_csv("/home/jjoh182/PythonProjectMaster/QtD_COPD_outputs/Demo_outcomes_520_longitudinal.csv",index=False)
