import os, logging, pyodbc, csv
from utility import Utility
from datetime import datetime

class RECOVER:

    def __init__(self):  
        self._util = Utility()
        self._conn = pyodbc.connect(self._util._constr)
        self._limitRows = ""     # top 100 for testing, otherwise empty string
        self._fileDate = "{:%d%m%Y}".format(datetime.now())
        self._files = []
        
    def Process(self):
        self.CreateCohort()
        self.ExportCONDITION()
        self.ExportDEATH()
        self.ExportDEMOGRAPHIC()
        self.ExportDIAGNOSIS()
        self.ExportDISPENSING()
        self.ExportENCOUNTER()
        self.ExportIMMUNIZATION()
        self.ExportLAB_RESULT_CM()
        self.ExportMED_ADMIN()
        self.ExportOBS_CLIN()
        self.ExportPRESCRIBING()
        self.ExportPROCEDURES()
        self.ExportVITAL()
        self.CreateTARFile()
        self.ExportGEOCODE_INPUT()  #example - create input file for geocoding with degauss using lds_address_history; comment out or modify for you site

    def db_export(self, tableName, sql):
        logging.info(f'Exporting: {tableName}')
        self._files.append(f"{self._util._site}_{self._util._dateModel}_{tableName}_{self._fileDate}.dsv")
        outf = open(f"{self._util._output_dir}\{self._util._site}_{self._util._dateModel}_{tableName}_{self._fileDate}.dsv", 'w', newline='')
        csvwriter = csv.writer(outf, delimiter='|', quotechar='"', quoting=csv.QUOTE_NONE)

        cursor=self._conn.cursor()
        cursor.execute(sql)
        header = [column[0] for column in cursor.description]
        csvwriter.writerow(header)

        while True:
            rows = cursor.fetchmany(1000)
            if not rows:
                break
            for row in rows:
                i = 0
                while i < len(cursor.description):
                    if isinstance(row[i], str):
                        row[i] = row[i].strip().replace("\"","").replace("|","")
                    i += 1
                csvwriter.writerow(row)
        cursor.close()

    def CreateTARFile(self):
        import tarfile
        os.chdir(self._util._output_dir)
        tar = tarfile.open(f"{self._util._site}_{self._util._dateModel}_CDM_{self._fileDate}.tar.gz", "w:gz")
        for name in self._files:
            tar.add(name)
        tar.close()

    # sql based on https://nyc-cdrn.atlassian.net/wiki/spaces/REC/pages/2996011048/March+2022+-+First+Data+Submissions
    def CreateCohort(self):
        logging.info("Creating Cohort")
        cursor=self._conn.cursor()
        sql = """CREATE TABLE #RECOVER_COHORT (
                patid varchar(18) PRIMARY KEY
            );"""
        cursor.execute(sql)
        sql = f"""with tmp (patid)
            as (
            -- COVID lab tests 
            select distinct patid from {self._util._schema}.lab_result_cm where lab_loinc in ('95942-9','97099-6','95941-1','95380-2','95423-0','95422-2','98733-9','99771-8','98846-9','98847-7','99774-2','99773-4','95209-3','94763-0','94661-6','95825-6','98069-8','94762-2','95542-7','94769-7','96118-5','94504-8','94503-0','94558-4','96119-3','97097-0','96094-8','98080-5','96896-6','96764-6','96763-8','94562-6','94768-9','95427-1','94720-0','95125-1','96742-2','94761-4','94563-4','94507-1','95429-7','94505-5','94547-7','95416-4','94564-2','94508-9','95428-9','94506-3','96895-8','100157-7','96957-6','95521-1','96898-2','94510-5','94311-8','94312-6','95522-9','94760-6','96986-5','95409-9','94533-7','94756-4','94757-2','95425-5','96448-6','96958-4','94766-3','94316-7','94307-6','94308-4','99596-9','95411-5','95410-7','97098-8','98132-4','98494-8','96899-0','94644-2','94511-3','94559-2','95824-9','94639-2','97104-4','98131-6','98493-0','94646-7','94645-9','96120-1','94534-5','96091-4','94314-2','96123-5','99314-7','94745-7','94746-5','94819-0','94565-9','94759-8','95406-5','96797-6','95608-6','94500-6','95424-8','94845-5','94822-4','94660-8','94309-2','96829-7','96897-4','94531-1','95826-4','94306-8','96900-6','94642-6','94643-4','94640-0','95609-4','96765-3','94767-1','94641-8','96752-1','96751-3','99597-7','96603-6','98732-1','98734-7','96894-1','95970-0','100156-9','96741-4','96755-4','94764-8','99772-6','95971-8','95974-2','95972-6','95973-4','94509-7','96121-9','94758-0','95823-1','94765-5','94315-9','96122-7','94313-4','94310-0','94502-2','94647-5','94532-9')
            union
            -- COVID diagnoses including MIS-C and PASC, myocarditis and Kawasaki's disease*/
            select distinct patid from {self._util._schema}.diagnosis where dx in ('B97.29','U07.1','B34.2','B97.2','B97.21','J12.81','J12.82','U04','U04.9','U07.2','B97.29','M35.81','U09.9', 'M30.3', 'B33.22', 'I40', 'I40.0', 'I40.1', 'I40.8', 'I40.9', 'I51.4')
            union
            -- respiratory diagnoses after 1/1/2019 
            select distinct patid from {self._util._schema}.diagnosis where admit_date >= '01-JAN-2019' and dx in('480.0','480.1','480.8','480.9','486','487.0','480.0','J12.0','J12.1','J12.89','J12.9','J12','J12.8','J18.8','P23.0','A01.03','A02.22','A37.01','A37.11','A37.81','A37.91','A54.84','B01.2','B05.2','B06.81','B20.6','B77.81','J12','J12.2','J12.3','J12.8','J12.81','J13','J14','J14','J15','J15.0','J15.1','J15.2','J15.20','J15.21','J15.211','J15.212','J15.29','J15.3','J15.4','J15.5','J15.6','J15.7','J15.8','J15.8','J15.9','J15.9','J16','J16.0','J16.8','J17','J17.0','J17.1','J17.2','J17.3','J17.8','J18','J18','J18.0','J18.0','J18.1','J18.1','J18.2','J18.2','J18.9','J18.9','J84.11','J84.111','J84.116','J84.117','J84.2','J85.1','J95.851','J41','J40','J41.0','J41.1','J41.8','J42','J68.0','491','491.1','491.8','491.9','506','490','J98.8','J84.115','J22','J44.0','J47.0','J20.2','J20.1','J20.0','J20.7','J20.3','J20.6','J20.5','J20.4','J20.8','J20.9','J21.0','J21.1','J21.8','J21.9','J20','J21','041','041.5','041.81','079.1','079.2','079.3','079.6','079.89','466','466.11','466.19','519.8','J09','J09','J09.01','J09.010','J09.018','J09.019','J09.02','J09.03','J09.090','J09.091','J09.092','J09.098','J09.11','J09.110','J09.118','J09.119','J09.12','J09.13','J09.190','J09.191','J09.192','J09.198','J09.X','J09.X1','J09.X2','J09.X3','J09.X9','J10','J10','J10.0','J10.0','J10.00','J10.01','J10.08','J10.1','J10.1','J10.2','J10.8','J10.8','J10.81','J10.82','J10.83','J10.89','J11','J11','J11.0','J11.0','J11.00','J11.08','J11.1','J11.1','J11.2','J11.8','J11.8','J11.81','J11.82','J11.83','J11.89','518.82','J80','R06.03','P22','P22.0','P22.8','P22.9','P28.11','R06.03','J95.82','J95.821','J95.822','J96','J96.0','J96.00','J96.01','J96.02','J96.1','J96.10','J96.11','J96.12','J96.2','J96.20','J96.21','J96.22','J96.9','J96.90','J96.91','J96.92','P28.5','P28.81','R09.2','786.2','R05','B33.8','B34.8','B97.89','J00','J02.9','J04.0','J04.1','J04.10','J04.11','J04.2','J04.3','J04.30','J04.31','J05.0','J05.0','J05.1','J05.10','J05.11','J06.0','J06.9','J39.8','J39.9','R05','J01.0','J01.00','J01.01','J01.1','J01.10','J01.11','J01.2','J01.20','J02.21','J01.3','J01.30','J01.31','J01.4','J01.40','J01.41','J01.8','J01.80','J01.81','J01.9','J01.90','J01.91','J02','J02.0','J02.8','J03','J03.0','J03.00','J03.01','J03.8','J03.80','J03.81','J03.9','J03.90','J03.91','786.05','R06.02','780.60','780.61','R50.9','R50.81','R50','R50.8','R50.2','R50.84','R50.9','R56.00')
            union
            -- procedure codes for COVID vaccination 
            select distinct patid from {self._util._schema}.procedures where px in ('91300','0001A','0002A','91301','0011A','0012A','91303','0031A')
            union         
            -- CVX codes for vaccination  
            select distinct patid from {self._util._schema}.immunization where vx_code in ('207','208','210','211','212','217','218','219','221')
            union
            -- RxNorm codes for vaccination  
            select distinct patid from {self._util._schema}.prescribing where rxnorm_cui in ('2468230', '2468231', '2468232', '2468233', '2468234', '2468235', '2470232', '2470233', '2470234', '2479830', '2479831', '2479832', '2479833', '2479834', '2479835', '2583742', '2583743')
            )
            insert into #RECOVER_COHORT (patid)
            SELECT patid from tmp;"""
        sql = "insert into #RECOVER_COHORT (patid) select patid from recover.dbo.recover_cohort"
        cursor.execute(sql)
        sql = "select count(*) from #RECOVER_COHORT"
        cursor.execute(sql)
        qty=cursor.fetchone()
        logging.info(f"Patients in Cohort: {qty[0]}")
        cursor.close()

    def ExportCONDITION(self):
        sql = f"""select {self._limitRows} [CONDITIONID]
            ,[PATID]
            ,[ENCOUNTERID]
            ,[REPORT_DATE]
            ,[RESOLVE_DATE]
            ,[ONSET_DATE]
            ,[CONDITION_STATUS]
            ,[CONDITION]
            ,[CONDITION_TYPE]
            ,[CONDITION_SOURCE]
            ,[RAW_CONDITION_STATUS]
            ,[RAW_CONDITION]
            ,[RAW_CONDITION_TYPE]
            ,[RAW_CONDITION_SOURCE] from {self._util._schema}.CONDITION
            where patid in (select patid from #RECOVER_COHORT)"""
        self.db_export("CONDITION", sql)
    
    def ExportDEATH(self):
        sql = f"""select {self._limitRows} [PATID]
            ,[DEATH_DATE]
            ,[DEATH_DATE_IMPUTE]
            ,[DEATH_SOURCE]
            ,[DEATH_MATCH_CONFIDENCE] from {self._util._schema}.DEATH
            where patid in (select patid from #RECOVER_COHORT)"""
        self.db_export("DEATH", sql)

    def ExportDEMOGRAPHIC(self):
        sql = f"""select {self._limitRows} [PATID]
            ,[BIRTH_DATE]
            ,[BIRTH_TIME]
            ,[SEX]
            ,[SEXUAL_ORIENTATION]
            ,[GENDER_IDENTITY]
            ,[HISPANIC]
            ,[RACE]
            ,[BIOBANK_FLAG]
            ,[PAT_PREF_LANGUAGE_SPOKEN]
            ,[RAW_SEX]
            ,[RAW_HISPANIC]
            ,[RAW_RACE]
            ,[RAW_PAT_PREF_LANGUAGE_SPOKEN]
            ,[RAW_SEXUAL_ORIENTATION]
            ,[RAW_GENDER_IDENTITY] from {self._util._schema}.DEMOGRAPHIC
            where patid in (select patid from #RECOVER_COHORT)"""
        self.db_export("DEMOGRAPHIC", sql)
    
    def ExportDIAGNOSIS(self):
        sql = f"""select {self._limitRows} [DIAGNOSISID]
            ,[PATID]
            ,[ENCOUNTERID]
            ,[ENC_TYPE]
            ,[ADMIT_DATE]
            ,[PROVIDERID]
            ,[DX]
            ,[DX_POA]
            ,[DX_TYPE]
            ,[DX_DATE]
            ,[DX_SOURCE]
            ,[DX_ORIGIN]
            ,[PDX]
            ,[RAW_DX]
            ,[RAW_DX_TYPE]
            ,[RAW_DX_SOURCE]
            ,[RAW_PDX]
            ,[RAW_DX_POA] from {self._util._schema}.DIAGNOSIS
            where patid in (select patid from #RECOVER_COHORT)"""
        self.db_export("DIAGNOSIS", sql)

    def ExportDISPENSING(self):
        sql = f"""select {self._limitRows} [DISPENSINGID]
            ,[PATID]
            ,[PRESCRIBINGID]
            ,[DISPENSE_DATE]
            ,[NDC]
            ,[DISPENSE_SOURCE]
            ,[DISPENSE_SUP]
            ,[DISPENSE_AMT]
            ,[DISPENSE_DOSE_DISP]
            ,[DISPENSE_DOSE_DISP_UNIT]
            ,[DISPENSE_ROUTE]
            ,[RAW_NDC]
            ,[RAW_DISPENSE_DOSE_DISP]
            ,[RAW_DISPENSE_DOSE_DISP_UNIT]
            ,[RAW_DISPENSE_ROUTE] from {self._util._schema}.DISPENSING
            where patid in (select patid from #RECOVER_COHORT)"""
        self.db_export("DISPENSING", sql)

    def ExportENCOUNTER(self):
        sql = f"""select {self._limitRows} [ENCOUNTERID]
            ,[PATID]
            ,[ADMIT_DATE]
            ,[ADMIT_TIME]
            ,[DISCHARGE_DATE]
            ,[DISCHARGE_TIME]
            ,[PROVIDERID]
            ,[FACILITY_LOCATION]
            ,[ENC_TYPE]
            ,[FACILITYID]
            ,[DISCHARGE_DISPOSITION]
            ,[DISCHARGE_STATUS]
            ,[DRG]
            ,[DRG_TYPE]
            ,[ADMITTING_SOURCE]
            ,[PAYER_TYPE_PRIMARY]
            ,[PAYER_TYPE_SECONDARY]
            ,[FACILITY_TYPE]
            ,[RAW_SITEID]
            ,[RAW_ENC_TYPE]
            ,[RAW_DISCHARGE_DISPOSITION]
            ,[RAW_DISCHARGE_STATUS]
            ,[RAW_DRG_TYPE]
            ,[RAW_ADMITTING_SOURCE]
            ,[RAW_FACILITY_TYPE]
            ,[RAW_PAYER_TYPE_PRIMARY]
            ,[RAW_PAYER_NAME_PRIMARY]
            ,[RAW_PAYER_ID_PRIMARY]
            ,[RAW_PAYER_TYPE_SECONDARY]
            ,[RAW_PAYER_NAME_SECONDARY]
            ,[RAW_PAYER_ID_SECONDARY] from {self._util._schema}.ENCOUNTER
            where patid in (select patid from #RECOVER_COHORT)"""
        self.db_export("ENCOUNTER", sql)

    def ExportIMMUNIZATION(self):
        sql = f"""select {self._limitRows} [IMMUNIZATIONID]
            ,[PATID]
            ,[ENCOUNTERID]
            ,[PROCEDURESID]
            ,[VX_PROVIDERID]
            ,[VX_RECORD_DATE]
            ,[VX_ADMIN_DATE]
            ,[VX_CODE_TYPE]
            ,[VX_CODE]
            ,[VX_STATUS]
            ,[VX_STATUS_REASON]
            ,[VX_SOURCE]
            ,[VX_DOSE]
            ,[VX_DOSE_UNIT]
            ,[VX_ROUTE]
            ,[VX_BODY_SITE]
            ,[VX_MANUFACTURER]
            ,[VX_LOT_NUM]
            ,[VX_EXP_DATE]
            ,[RAW_VX_NAME]
            ,[RAW_VX_CODE]
            ,[RAW_VX_CODE_TYPE]
            ,[RAW_VX_DOSE]
            ,[RAW_VX_DOSE_UNIT]
            ,[RAW_VX_ROUTE]
            ,[RAW_VX_BODY_SITE]
            ,[RAW_VX_STATUS]
            ,[RAW_VX_STATUS_REASON]
            ,[RAW_VX_MANUFACTURER] from {self._util._schema}.IMMUNIZATION
            where patid in (select patid from #RECOVER_COHORT)"""
        self.db_export("IMMUNIZATION", sql)

    
    def ExportLAB_RESULT_CM(self):
        sql = f"""select {self._limitRows} [LAB_RESULT_CM_ID]
            ,[PATID]
            ,[ENCOUNTERID]
            ,[SPECIMEN_SOURCE]
            ,[LAB_LOINC]
            ,[LAB_RESULT_SOURCE]
            ,[LAB_LOINC_SOURCE]
            ,[PRIORITY]
            ,[RESULT_LOC]
            ,[LAB_PX]
            ,[LAB_PX_TYPE]
            ,[LAB_ORDER_DATE]
            ,[SPECIMEN_DATE]
            ,[SPECIMEN_TIME]
            ,[RESULT_DATE]
            ,[RESULT_TIME]
            ,[RESULT_QUAL]
            ,[RESULT_SNOMED]
            ,[RESULT_NUM]
            ,[RESULT_MODIFIER]
            ,[RESULT_UNIT]
            ,[NORM_RANGE_LOW]
            ,[NORM_MODIFIER_LOW]
            ,[NORM_RANGE_HIGH]
            ,[NORM_MODIFIER_HIGH]
            ,[ABN_IND]
            ,[RAW_LAB_NAME]
            ,[RAW_LAB_CODE]
            ,[RAW_PANEL]
            ,[RAW_RESULT]
            ,[RAW_UNIT]
            ,[RAW_ORDER_DEPT]
            ,[RAW_FACILITY_CODE] from {self._util._schema}.LAB_RESULT_CM
            where patid in (select patid from #RECOVER_COHORT) and RESULT_DATE >= '1/1/2017'"""
        self.db_export("LAB_RESULT_CM", sql)

    def ExportMED_ADMIN(self):
        sql = f"""select {self._limitRows} [MEDADMINID]
            ,[PATID]
            ,[ENCOUNTERID]
            ,[PRESCRIBINGID]
            ,[MEDADMIN_PROVIDERID]
            ,[MEDADMIN_START_DATE]
            ,[MEDADMIN_START_TIME]
            ,[MEDADMIN_STOP_DATE]
            ,[MEDADMIN_STOP_TIME]
            ,[MEDADMIN_TYPE]
            ,[MEDADMIN_CODE]
            ,[MEDADMIN_DOSE_ADMIN]
            ,[MEDADMIN_DOSE_ADMIN_UNIT]
            ,[MEDADMIN_ROUTE]
            ,[MEDADMIN_SOURCE]
            ,[RAW_MEDADMIN_MED_NAME]
            ,[RAW_MEDADMIN_CODE]
            ,[RAW_MEDADMIN_DOSE_ADMIN]
            ,[RAW_MEDADMIN_DOSE_ADMIN_UNIT]
            ,[RAW_MEDADMIN_ROUTE] from {self._util._schema}.MED_ADMIN
            where patid in (select patid from #RECOVER_COHORT) and MEDADMIN_START_DATE >= '1/1/2017'"""
        self.db_export("MED_ADMIN", sql)

    def ExportOBS_CLIN(self):
        sql = f"""select {self._limitRows} [OBSCLINID]
            ,[PATID]
            ,[ENCOUNTERID]
            ,[OBSCLIN_PROVIDERID]
            ,[OBSCLIN_START_DATE]
            ,[OBSCLIN_START_TIME]
            ,[OBSCLIN_STOP_DATE]
            ,[OBSCLIN_STOP_TIME]
            ,[OBSCLIN_TYPE]
            ,[OBSCLIN_CODE]
            ,[OBSCLIN_RESULT_QUAL]
            ,[OBSCLIN_RESULT_TEXT]
            ,[OBSCLIN_RESULT_SNOMED]
            ,[OBSCLIN_RESULT_NUM]
            ,[OBSCLIN_RESULT_MODIFIER]
            ,[OBSCLIN_RESULT_UNIT]
            ,[OBSCLIN_SOURCE]
            ,[OBSCLIN_ABN_IND]
            ,[RAW_OBSCLIN_NAME]
            ,[RAW_OBSCLIN_CODE]
            ,[RAW_OBSCLIN_TYPE]
            ,[RAW_OBSCLIN_RESULT]
            ,[RAW_OBSCLIN_MODIFIER]
            ,[RAW_OBSCLIN_UNIT] from {self._util._schema}.OBS_CLIN
            where patid in (select patid from #RECOVER_COHORT) and OBSCLIN_START_DATE >= '1/1/2017'"""
        self.db_export("OBS_CLIN", sql)

    def ExportPRESCRIBING(self):
        sql = f"""select {self._limitRows} [PRESCRIBINGID]
            ,[PATID]
            ,[ENCOUNTERID]
            ,[RX_PROVIDERID]
            ,[RX_ORDER_DATE]
            ,[RX_ORDER_TIME]
            ,[RX_START_DATE]
            ,[RX_END_DATE]
            ,[RX_DOSE_ORDERED]
            ,[RX_DOSE_ORDERED_UNIT]
            ,[RX_QUANTITY]
            ,[RX_DOSE_FORM]
            ,[RX_REFILLS]
            ,[RX_DAYS_SUPPLY]
            ,[RX_FREQUENCY]
            ,[RX_PRN_FLAG]
            ,[RX_ROUTE]
            ,[RX_BASIS]
            ,[RXNORM_CUI]
            ,[RX_SOURCE]
            ,[RX_DISPENSE_AS_WRITTEN]
            ,[RAW_RX_MED_NAME]
            ,[RAW_RX_FREQUENCY]
            ,[RAW_RXNORM_CUI]
            ,[RAW_RX_QUANTITY]
            ,[RAW_RX_NDC]
            ,[RAW_RX_DOSE_ORDERED]
            ,[RAW_RX_DOSE_ORDERED_UNIT]
            ,[RAW_RX_ROUTE]
            ,[RAW_RX_REFILLS] from {self._util._schema}.PRESCRIBING
            where patid in (select patid from #RECOVER_COHORT) and RX_ORDER_DATE >= '1/1/2017'"""
        self.db_export("PRESCRIBING", sql)

    def ExportPROCEDURES(self):
        sql = f"""select {self._limitRows} [PROCEDURESID]
            ,[PATID]
            ,[ENCOUNTERID]
            ,[ENC_TYPE]
            ,[ADMIT_DATE]
            ,[PROVIDERID]
            ,[PX_DATE]
            ,[PX]
            ,[PX_TYPE]
            ,[PX_SOURCE]
            ,[PPX]
            ,[RAW_PX]
            ,[RAW_PX_TYPE]
            ,[RAW_PPX] from {self._util._schema}.PROCEDURES
            where patid in (select patid from #RECOVER_COHORT)"""
        self.db_export("PROCEDURES", sql)

    def ExportVITAL(self):
        sql = f"""select {self._limitRows} [VITALID]
            ,[PATID]
            ,[ENCOUNTERID]
            ,[MEASURE_DATE]
            ,[MEASURE_TIME]
            ,[VITAL_SOURCE]
            ,[HT]
            ,[WT]
            ,[DIASTOLIC]
            ,[SYSTOLIC]
            ,[ORIGINAL_BMI]
            ,[BP_POSITION]
            ,[SMOKING]
            ,[TOBACCO]
            ,[TOBACCO_TYPE]
            ,[RAW_DIASTOLIC]
            ,[RAW_SYSTOLIC]
            ,[RAW_BP_POSITION]
            ,[RAW_SMOKING]
            ,[RAW_TOBACCO]
            ,[RAW_TOBACCO_TYPE] from {self._util._schema}.VITAL
            where patid in (select patid from #RECOVER_COHORT) and MEASURE_DATE >= '1/1/2017'"""
        self.db_export("VITAL", sql)

    def ExportGEOCODE_INPUT(self):
        sql = f"""select {self._limitRows} [PATID]
            ,([CRANE_ADDRESS] + ' ' + [ADDRESS_CITY] + ' ' + [ADDRESS_STATE] + ' ' + [ADDRESS_ZIP5]) AS ADDRESS
            from {self._util._schema}.LDS_ADDRESS_HISTORY
            where patid in (select patid from #RECOVER_COHORT) and ADDRESS_PERIOD_END IS NULL and CRANE_ADDRESS IS NOT NULL"""
        self.db_export("GEOCODE_INPUT", sql)

