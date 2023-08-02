import pytest
from pyspark.sql import SparkSession
from Exercice1 import *
from datetime import date 
from chispa import assert_df_equality

def test_create_dataset(spark_session):
    
    df_paths = [ 
                'C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Exercice 1/tables/maag_master_agrem',
                'C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Exercice 1/tables/reac_ref_act_type', 
                'C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Exercice 1/tables/maag_repa_rrol_linked', 
                'C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Exercice 1/tables/maag_raty_linked', 
                'C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Exercice 1/tables/rtpa_ref_third_party'
               ]
    
    #Partie 1
    
    maag_master_agremDF = DF(spark_session, df_paths[0], MAAG_MASTER_AGREEMENT_SCHEMA)
    reac_ref_act_typeDF = DF(spark_session, df_paths[1], REAC_REF_ACT_TYPE_SCHEMA)
    maag_repa_rrol_linkedDF = DF(spark_session, df_paths[2], MAAG_REPA_RROL_LINKED_SCHEMA)
    maag_raty_linkedDF = DF(spark_session, df_paths[3],  MAAG_RATY_LINKED_SCHEMA)
    rtpa_ref_third_partyDF = DF(spark_session, df_paths[4], RTPA_REF_THIRD_PARTY_SCHEMA)

    rtpa_ref_third_partyDF.column_rename('C_THIR_PART_REFER', 'C_PART_REFER')
    
    dataset = DataSet(
                        [maag_master_agremDF, reac_ref_act_typeDF, maag_repa_rrol_linkedDF, maag_raty_linkedDF, rtpa_ref_third_partyDF], 
                        [
                            ['C_ACT_TYPE', 'N_APPLIC_INFQ'], 
                            ['N_APPLIC_INFQ', 'C_MAST_AGREM_REFER'], 
                            ['C_MAST_AGREM_REFER'], 
                            ['N_APPLIC_INFQ', 'C_PART_REFER']
                        ]
                     )
    


    df_test = spark_session.createDataFrame(dataset.take(2), DATASET_SCHEMA)
    expected_result = spark_session.createDataFrame \
    (
        data = [
                    (
                        38,
                        "26701",
                        "AUT12027900",
                        "PRET",
                        date(2013,5,3),
                        15,
                        "BONGARZONE TP",
                        "TER",
                        6,
                        "Prêts",
                        "DLAFFEC",
                        date(1970,1,1),
                        None,
                        date(2023,2,10),
                        "CAPIMPAYKSP",
                        None,
                        None,
                        None,
                    ),
                    (
                        38,
                        "A0014605",
                        "AUT12027900",
                        "PRET",
                        date(2013,5,3),
                        15,
                        "BONGARZONE TP",
                        "TER",
                        6,
                        "Prêts",
                        "CLIENT",
                        date(1980,1,1),
                        None,
                        date(2023,2,10),
                        "CAPIMPAYKSP",
                        None,
                        None,
                        None,
                    )                    
                
               ],
        schema = DATASET_SCHEMA
    )
    
    assert_df_equality(expected_result, df_test, ignore_row_order=True, ignore_schema=True)
