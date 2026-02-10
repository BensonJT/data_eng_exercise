from sqlalchemy import Column, Integer, String, Float, Date, MetaData
from sqlalchemy.orm import declarative_base

metadata = MetaData(schema="data")
Base = declarative_base(metadata=metadata)

class BeneficiarySummaryMixin:
    DESYNPUF_ID = Column(String, primary_key=True)
    YEAR = Column(Integer, primary_key=True)  # Injected during ingestion
    BENE_BIRTH_DT = Column(String) # formatted as YYYYMMDD
    BENE_DEATH_DT = Column(String)
    BENE_SEX_IDENT_CD = Column(String)
    BENE_RACE_CD = Column(String)
    BENE_ESRD_IND = Column(String)
    SP_STATE_CODE = Column(String)
    BENE_COUNTY_CD = Column(String)
    BENE_HI_CVRAGE_TOT_MONS = Column(Integer)
    BENE_SMI_CVRAGE_TOT_MONS = Column(Integer)
    BENE_HMO_CVRAGE_TOT_MONS = Column(Integer)
    PLAN_CVRG_MOS_NUM = Column(Integer)
    SP_ALZHDMTA = Column(String)
    SP_CHF = Column(String)
    SP_CHRNKIDN = Column(String)
    SP_CNCR = Column(String)
    SP_COPD = Column(String)
    SP_DEPRESSN = Column(String)
    SP_DIABETES = Column(String)
    SP_ISCHMCHT = Column(String)
    SP_OSTEOPRS = Column(String)
    SP_RA_OA = Column(String)
    SP_STRKETIA = Column(String)
    MEDREIMB_IP = Column(Float)
    BENRES_IP = Column(Float)
    PPPYMT_IP = Column(Float)
    MEDREIMB_OP = Column(Float)
    BENRES_OP = Column(Float)
    PPPYMT_OP = Column(Float)
    MEDREIMB_CAR = Column(Float)
    BENRES_CAR = Column(Float)
    PPPYMT_CAR = Column(Float)

class SrcBeneficiarySummary(Base, BeneficiarySummaryMixin):
    __tablename__ = 'src_beneficiary_summary'

class NewBeneficiarySummary(Base, BeneficiarySummaryMixin):
    __tablename__ = 'new_beneficiary_summary'

class CarrierClaimsMixin:
    DESYNPUF_ID = Column(String, index=True)
    CLM_ID = Column(String, primary_key=True)
    CLM_FROM_DT = Column(String)
    CLM_THRU_DT = Column(String)
    
    # Diagnosis Codes
    ICD9_DGNS_CD_1 = Column(String)
    ICD9_DGNS_CD_2 = Column(String)
    ICD9_DGNS_CD_3 = Column(String)
    ICD9_DGNS_CD_4 = Column(String)
    ICD9_DGNS_CD_5 = Column(String)
    ICD9_DGNS_CD_6 = Column(String)
    ICD9_DGNS_CD_7 = Column(String)
    ICD9_DGNS_CD_8 = Column(String)
    
    # Provider NPIs 1-13
    PRF_PHYSN_NPI_1 = Column(String)
    PRF_PHYSN_NPI_2 = Column(String)
    PRF_PHYSN_NPI_3 = Column(String)
    PRF_PHYSN_NPI_4 = Column(String)
    PRF_PHYSN_NPI_5 = Column(String)
    PRF_PHYSN_NPI_6 = Column(String)
    PRF_PHYSN_NPI_7 = Column(String)
    PRF_PHYSN_NPI_8 = Column(String)
    PRF_PHYSN_NPI_9 = Column(String)
    PRF_PHYSN_NPI_10 = Column(String)
    PRF_PHYSN_NPI_11 = Column(String)
    PRF_PHYSN_NPI_12 = Column(String)
    PRF_PHYSN_NPI_13 = Column(String)
    
    # Tax Numbers 1-13
    TAX_NUM_1 = Column(String)
    TAX_NUM_2 = Column(String)
    TAX_NUM_3 = Column(String)
    TAX_NUM_4 = Column(String)
    TAX_NUM_5 = Column(String)
    TAX_NUM_6 = Column(String)
    TAX_NUM_7 = Column(String)
    TAX_NUM_8 = Column(String)
    TAX_NUM_9 = Column(String)
    TAX_NUM_10 = Column(String)
    TAX_NUM_11 = Column(String)
    TAX_NUM_12 = Column(String)
    TAX_NUM_13 = Column(String)
    
    # HCPCS Codes 1-13
    HCPCS_CD_1 = Column(String)
    HCPCS_CD_2 = Column(String)
    HCPCS_CD_3 = Column(String)
    HCPCS_CD_4 = Column(String)
    HCPCS_CD_5 = Column(String)
    HCPCS_CD_6 = Column(String)
    HCPCS_CD_7 = Column(String)
    HCPCS_CD_8 = Column(String)
    HCPCS_CD_9 = Column(String)
    HCPCS_CD_10 = Column(String)
    HCPCS_CD_11 = Column(String)
    HCPCS_CD_12 = Column(String)
    HCPCS_CD_13 = Column(String)
    
    # Line Payment Amounts 1-13
    LINE_NCH_PMT_AMT_1 = Column(Float)
    LINE_NCH_PMT_AMT_2 = Column(Float)
    LINE_NCH_PMT_AMT_3 = Column(Float)
    LINE_NCH_PMT_AMT_4 = Column(Float)
    LINE_NCH_PMT_AMT_5 = Column(Float)
    LINE_NCH_PMT_AMT_6 = Column(Float)
    LINE_NCH_PMT_AMT_7 = Column(Float)
    LINE_NCH_PMT_AMT_8 = Column(Float)
    LINE_NCH_PMT_AMT_9 = Column(Float)
    LINE_NCH_PMT_AMT_10 = Column(Float)
    LINE_NCH_PMT_AMT_11 = Column(Float)
    LINE_NCH_PMT_AMT_12 = Column(Float)
    LINE_NCH_PMT_AMT_13 = Column(Float)
    
    # Deductible Amounts 1-13
    LINE_BENE_PTB_DDCTBL_AMT_1 = Column(Float)
    LINE_BENE_PTB_DDCTBL_AMT_2 = Column(Float)
    LINE_BENE_PTB_DDCTBL_AMT_3 = Column(Float)
    LINE_BENE_PTB_DDCTBL_AMT_4 = Column(Float)
    LINE_BENE_PTB_DDCTBL_AMT_5 = Column(Float)
    LINE_BENE_PTB_DDCTBL_AMT_6 = Column(Float)
    LINE_BENE_PTB_DDCTBL_AMT_7 = Column(Float)
    LINE_BENE_PTB_DDCTBL_AMT_8 = Column(Float)
    LINE_BENE_PTB_DDCTBL_AMT_9 = Column(Float)
    LINE_BENE_PTB_DDCTBL_AMT_10 = Column(Float)
    LINE_BENE_PTB_DDCTBL_AMT_11 = Column(Float)
    LINE_BENE_PTB_DDCTBL_AMT_12 = Column(Float)
    LINE_BENE_PTB_DDCTBL_AMT_13 = Column(Float)
    
     # Primary Payer Paid Amounts 1-13
    LINE_BENE_PRMRY_PYR_PD_AMT_1 = Column(Float)
    LINE_BENE_PRMRY_PYR_PD_AMT_2 = Column(Float)
    LINE_BENE_PRMRY_PYR_PD_AMT_3 = Column(Float)
    LINE_BENE_PRMRY_PYR_PD_AMT_4 = Column(Float)
    LINE_BENE_PRMRY_PYR_PD_AMT_5 = Column(Float)
    LINE_BENE_PRMRY_PYR_PD_AMT_6 = Column(Float)
    LINE_BENE_PRMRY_PYR_PD_AMT_7 = Column(Float)
    LINE_BENE_PRMRY_PYR_PD_AMT_8 = Column(Float)
    LINE_BENE_PRMRY_PYR_PD_AMT_9 = Column(Float)
    LINE_BENE_PRMRY_PYR_PD_AMT_10 = Column(Float)
    LINE_BENE_PRMRY_PYR_PD_AMT_11 = Column(Float)
    LINE_BENE_PRMRY_PYR_PD_AMT_12 = Column(Float)
    LINE_BENE_PRMRY_PYR_PD_AMT_13 = Column(Float)
    
    # Coinsurance Amounts 1-13
    LINE_COINSRNC_AMT_1 = Column(Float)
    LINE_COINSRNC_AMT_2 = Column(Float)
    LINE_COINSRNC_AMT_3 = Column(Float)
    LINE_COINSRNC_AMT_4 = Column(Float)
    LINE_COINSRNC_AMT_5 = Column(Float)
    LINE_COINSRNC_AMT_6 = Column(Float)
    LINE_COINSRNC_AMT_7 = Column(Float)
    LINE_COINSRNC_AMT_8 = Column(Float)
    LINE_COINSRNC_AMT_9 = Column(Float)
    LINE_COINSRNC_AMT_10 = Column(Float)
    LINE_COINSRNC_AMT_11 = Column(Float)
    LINE_COINSRNC_AMT_12 = Column(Float)
    LINE_COINSRNC_AMT_13 = Column(Float)
    
    # Allowed Charge Amounts 1-13
    LINE_ALOWD_CHRG_AMT_1 = Column(Float)
    LINE_ALOWD_CHRG_AMT_2 = Column(Float)
    LINE_ALOWD_CHRG_AMT_3 = Column(Float)
    LINE_ALOWD_CHRG_AMT_4 = Column(Float)
    LINE_ALOWD_CHRG_AMT_5 = Column(Float)
    LINE_ALOWD_CHRG_AMT_6 = Column(Float)
    LINE_ALOWD_CHRG_AMT_7 = Column(Float)
    LINE_ALOWD_CHRG_AMT_8 = Column(Float)
    LINE_ALOWD_CHRG_AMT_9 = Column(Float)
    LINE_ALOWD_CHRG_AMT_10 = Column(Float)
    LINE_ALOWD_CHRG_AMT_11 = Column(Float)
    LINE_ALOWD_CHRG_AMT_12 = Column(Float)
    LINE_ALOWD_CHRG_AMT_13 = Column(Float)
    
    # Processing Indicator Codes 1-13
    LINE_PRCSG_IND_CD_1 = Column(String)
    LINE_PRCSG_IND_CD_2 = Column(String)
    LINE_PRCSG_IND_CD_3 = Column(String)
    LINE_PRCSG_IND_CD_4 = Column(String)
    LINE_PRCSG_IND_CD_5 = Column(String)
    LINE_PRCSG_IND_CD_6 = Column(String)
    LINE_PRCSG_IND_CD_7 = Column(String)
    LINE_PRCSG_IND_CD_8 = Column(String)
    LINE_PRCSG_IND_CD_9 = Column(String)
    LINE_PRCSG_IND_CD_10 = Column(String)
    LINE_PRCSG_IND_CD_11 = Column(String)
    LINE_PRCSG_IND_CD_12 = Column(String)
    LINE_PRCSG_IND_CD_13 = Column(String)
    
    # Line Diagnosis Codes 1-13
    LINE_ICD9_DGNS_CD_1 = Column(String)
    LINE_ICD9_DGNS_CD_2 = Column(String)
    LINE_ICD9_DGNS_CD_3 = Column(String)
    LINE_ICD9_DGNS_CD_4 = Column(String)
    LINE_ICD9_DGNS_CD_5 = Column(String)
    LINE_ICD9_DGNS_CD_6 = Column(String)
    LINE_ICD9_DGNS_CD_7 = Column(String)
    LINE_ICD9_DGNS_CD_8 = Column(String)
    LINE_ICD9_DGNS_CD_9 = Column(String)
    LINE_ICD9_DGNS_CD_10 = Column(String)
    LINE_ICD9_DGNS_CD_11 = Column(String)
    LINE_ICD9_DGNS_CD_12 = Column(String)
    LINE_ICD9_DGNS_CD_13 = Column(String)


class SrcCarrierClaims(Base, CarrierClaimsMixin):
    __tablename__ = 'src_carrier_claims'

class NewCarrierClaims(Base, CarrierClaimsMixin):
    __tablename__ = 'new_carrier_claims'
