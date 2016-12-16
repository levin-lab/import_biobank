#!/usr/bin/env python
# $Id: import_biobank.py 5221 2016-12-14 21:18:52Z levinm08 $
"""Find matching anesthetic records for biobank participants.

    It takes as input a list of biobank MRNs and some other identifiers
    from the biobank.biobank_import table, which is populated by a job
    that runs bi-weekly on the biobank Oracle db.

    It then looks in case_summary for cases and updates biobank.biobank_cases.
    An audit trail of new/old/disappeared mrns and cases is kept.

    Run from a chron job
"""
import sys
import time
import logging
from logging.handlers import RotatingFileHandler
import MySQLdb as mysql
import settings_biobank     # db connect string and other site-specific settings

def main():
    formatter = logging.Formatter(fmt='%(asctime)s: %(levelname)s: %(message)s')
    # TODO: create log file if it doesn't exist
    logfile_handler = RotatingFileHandler(settings_biobank.LOG,
                                          maxBytes=(1048576 * 100), backupCount=100)
    logfile_handler.setLevel(logging.DEBUG)
    logfile_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)

    log = logging.getLogger('biobank_import')
    log.setLevel(logging.INFO)
    log.addHandler(logfile_handler)
    log.addHandler(console_handler)

    log.info('Using Python %s', sys.version)
    log.info('Using server {host}'.format(**settings_biobank.DB_CONNECT_STRING)
             + ' database {BIOBANK_DB}'.format(**settings_biobank.DB_CONFIG))
    db = mysql.connect(**settings_biobank.DB_CONNECT_STRING)
    cursor = db.cursor()


    # Use today as the date for new matches.
    now = time.strftime('%Y-%m-%d %H:%M:%S')

    # count of patients who have opted-out since last import
    cursor.execute("""
    SELECT MAX(import_date)
    FROM {BIOBANK_DB}.biobank_import
    """.format(**settings_biobank.DB_CONFIG))
    last_import_date = cursor.fetchone()[0]
    settings_biobank.DB_CONFIG['LAST_IMPORT_DATE'] = last_import_date    #for convenience
    log.info('last import date %s', format(last_import_date))

    cursor.execute("""
    SELECT COUNT(DISTINCT(masked_mrn))
    FROM {BIOBANK_DB}.biobank_import b
    WHERE b.bb_optout = 'TRUE' AND optout_date >= '{LAST_IMPORT_DATE}'
    """.format(**settings_biobank.DB_CONFIG))
    opt_out_cnt = cursor.fetchone()[0]
    log.info('%s patient(s) have opted out since last import', format(opt_out_cnt))

    # NOTE: we're not checking opt-out date, so this may miss the rare person
    #       who opted back in after opting out
    log.info("Gathering up list of all opted-out patients")
    cursor.execute("""
    CREATE TEMPORARY TABLE {BIOBANK_DB}.tmp_bb_optout ENGINE=MEMORY
    SELECT distinct(masked_mrn) FROM {BIOBANK_DB}.biobank_import 
    WHERE bb_optout = 'TRUE' 
    """.format(**settings_biobank.DB_CONFIG))
    db.commit()

    # total count of all patient who have ever opted out
    cursor.execute("""
    SELECT COUNT(*)
    FROM {BIOBANK_DB}.tmp_bb_optout
    """.format(**settings_biobank.DB_CONFIG))
    all_opt_out_cnt = cursor.fetchone()[0]
    log.info('%s patient(s) have opted out at any time', format(all_opt_out_cnt))

    # Match on MRN to produce the broadest possible list of matches
    # also use the site identifier to limit query to MSH cases
    # Honor the opt-out flag here by excluding patients in the tmp_bb_outout list
    log.info("Matching cases")
    cursor.execute("""
    CREATE TEMPORARY TABLE {BIOBANK_DB}.tmp_bb_all_cases ENGINE=MEMORY
    SELECT DISTINCT s.case_name, b.masked_mrn, s.visit_id, b.enroll_date,b.bb_optout
    FROM {REPORT_DB}.case_summary s
    JOIN {BIOBANK_DB}.biobank_import b on b.mrn = s.mrn
    WHERE s.site = {COMPURECORD_SITEID_MSH} 
    AND b.masked_mrn NOT IN (SELECT masked_mrn FROM {BIOBANK_DB}.tmp_bb_optout)
    ORDER BY s.case_timestamp ASC""".format(**settings_biobank.DB_CONFIG))
    db.commit()
    cursor.execute("""
    SELECT case_name, masked_mrn, visit_id 
    FROM {BIOBANK_DB}.tmp_bb_all_cases""".format(**settings_biobank.DB_CONFIG))
    cases_now_details = cursor.fetchall()
    cases_now = set([row[0] for row in cases_now_details])
    match_date_by_case_name, masked_mrn_by_case_name, visit_id_by_case_name = {}, {}, {}
    for (case, masked_mrn, visit_id) in cases_now_details:
        match_date_by_case_name[case] = now
        masked_mrn_by_case_name[case] = masked_mrn
        visit_id_by_case_name[case] = visit_id

        # TODO: Match also on gender, and see if the numbers are different?
        # Which they could be in the case of gender reassignment surgeries etc

    log.info("Determining which cases are new, old, and disappeared (opted-out).")

    # Set of cases as it was before
    cursor.execute("""
    SELECT DISTINCT b.case_name, b.match_date, b.masked_mrn, b.visit_id 
    FROM {BIOBANK_DB}.biobank_cases b""".format(**settings_biobank.DB_CONFIG))
    cases_then_details = cursor.fetchall()
    log.debug(cursor._executed)
    cases_then = set([row[0] for row in cases_then_details])
    # Save the match date for each existing case
    for (case, match_date, masked_mrn, visit_id) in cases_then_details:
        match_date_by_case_name[case] = match_date
        masked_mrn_by_case_name[case] = masked_mrn
        visit_id_by_case_name[case] = visit_id

    # Do some set arithmetic to decide which cases are new
    new_cases = cases_now - cases_then
    old_cases = cases_now & cases_then
    disappeared_cases = cases_then - cases_now

    # Report on each case
    # assert((len(new_cases) + len(old_cases) - len(disappeared_cases)) == len(cases_now))
    def print_case_line(case, descriptor):
        """Convenience function to print each case line."""
        log.info("%s:%s:%s:%s:%s", descriptor, match_date_by_case_name[case], case,
                 masked_mrn_by_case_name[case], visit_id_by_case_name[case])

    for case in disappeared_cases:
        print_case_line(case, "DISAPPEARED")
    for case in old_cases:
        print_case_line(case, "OLD")
    for case in new_cases:
        print_case_line(case, "NEW")

    # Create temporary table of the new cases,join into case_summary, and update ordw.biobank_cases
    cursor.execute("""
    CREATE TEMPORARY TABLE {BIOBANK_DB}.tmp_bb_new_cases (case_name CHAR(12)) ENGINE=MEMORY
    """.format(**settings_biobank.DB_CONFIG))
    insert_new = """
    INSERT INTO {BIOBANK_DB}.tmp_bb_new_cases(case_name) VALUES(%s)
    """.format(**settings_biobank.DB_CONFIG)
    cursor.executemany(insert_new, [(case,) for case in new_cases])
    db.commit()

    log.info("Adding new cases to biobank_cases...")
    insert_new = """
    INSERT INTO {BIOBANK_DB}.biobank_cases
    (masked_mrn, case_name, service_date, enroll_date, match_date)
    SELECT
        a.masked_mrn,
        s.case_name,
        s.service_date,
        a.enroll_date,
        %s
    FROM {REPORT_DB}.case_summary s
    JOIN {BIOBANK_DB}.tmp_bb_new_cases n on s.case_name = n.case_name
    JOIN {BIOBANK_DB}.tmp_bb_all_cases a on n.case_name = a.case_name
    """.format(**settings_biobank.DB_CONFIG)
    cursor.execute(insert_new, (now,))
    log.debug(cursor._executed)

    db.commit()

    # Now delete cases of patients that have opted out
    log.info("Deleting disappeared cases from biobank_cases...")
    delete_disappeared = """
    DELETE FROM {BIOBANK_DB}.biobank_cases 
    WHERE case_name = %s""".format(**settings_biobank.DB_CONFIG)
    cursor.executemany(delete_disappeared, [(case,) for case in disappeared_cases])
    log.debug(cursor._executed)
    db.commit()

    # Report some statistics
    log.info("Patients that opted out since last import %d", opt_out_cnt)
    log.info("Patients that opted out at any time: %s", format(all_opt_out_cnt))
    log.info("Starting count of cases: %d", len(cases_then))
    log.info("Cases we knew about already: %d", len(old_cases))
    log.info("Cases newly found: %d", len(new_cases))
    log.info("Cases that disappeared: %d", len(disappeared_cases))
    log.info("Cases now: %d", len(cases_now))
    log.info("Biobank import complete.")

if __name__ == '__main__':
    main()
