CREATE SEQUENCE reminder_hist_seq
    START WITH 1000
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;


CREATE TABLE reminder_hist (
    id INT DEFAULT reminder_hist_seq.NEXTVAL PRIMARY KEY,
    name VARCHAR2(40),
    surname VARCHAR2(40),
    account_id INT,
    balance_amount NUMBER,
    reminder_balance_date DATE,
    update_date date
);

/*

With customer_id 

CREATE OR REPLACE PROCEDURE GET_CUSTOMER_BALANCE is

BEGIN


    MERGE INTO reminder_hist hist
    USING
        (SELECT ct.ADI, ct.SOYADI, q.HESAB_ID, q.BALANS_MEBLEG, q.BALANS_TARIX
         FROM ACC_HESAB_QALIQ q
                  INNER JOIN ACC_HESAB h ON q.HESAB_ID = h.HESAB_ID
                  INNER JOIN CT_MUSHTERI_FIZIKI_SHEXS ct ON ct.MUSHTERI_FIZIKI_SHEXS_ID = h.MUSHTERI_ID) reminder_tab
    ON (hist.account_id = reminder_tab.HESAB_ID)
    WHEN MATCHED THEN
        UPDATE
        SET hist.balance_amount        = reminder_tab.BALANS_MEBLEG,
            hist.reminder_balance_date = reminder_tab.BALANS_TARIX,
            hist.update_date           = sysdate
    WHEN NOT MATCHED THEN
        INSERT (name, surname, account_id, balance_amount, reminder_balance_date, update_date)
        VALUES (reminder_tab.ADI, reminder_tab.SOYADI, reminder_tab.HESAB_ID, reminder_tab.BALANS_MEBLEG,
                reminder_tab.BALANS_TARIX, sysdate);


END GET_CUSTOMER_BALANCE;

*/






CREATE OR REPLACE PROCEDURE GET_CUSTOMER_BALANCE is

BEGIN


    MERGE INTO reminder_hist hist
    USING
        (SELECT ct.ADI, ct.SOYADI, q.HESAB_ID, q.BALANS_MEBLEG, q.BALANS_TARIX
         FROM ACC_HESAB_QALIQ q
                  INNER JOIN ACC_HESAB h ON q.HESAB_ID = h.HESAB_ID
                  INNER JOIN CT_MUSHTERI_FIZIKI_SHEXS ct ON ct.MUSHTERI_FIZIKI_SHEXS_ID = h.MUSHTERI_ID) reminder_tab
    ON (hist.account_id = reminder_tab.HESAB_ID)
    WHEN MATCHED THEN
        UPDATE
        SET hist.balance_amount        = reminder_tab.BALANS_MEBLEG,
            hist.reminder_balance_date = reminder_tab.BALANS_TARIX,
            hist.update_date           = sysdate
    WHEN NOT MATCHED THEN
        INSERT (name, surname, account_id, balance_amount, reminder_balance_date, update_date)
        VALUES (reminder_tab.ADI, reminder_tab.SOYADI, reminder_tab.HESAB_ID, reminder_tab.BALANS_MEBLEG,
                reminder_tab.BALANS_TARIX, sysdate);


END GET_CUSTOMER_BALANCE;
/


BEGIN
  DBMS_SCHEDULER.create_job (
    job_name        => 'GET_CUSTOMER_BALANCE_JOB',
    job_type        => 'PLSQL_BLOCK',
    job_action      => 'BEGIN GET_CUSTOMER_BALANCE; END;',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=MONTHLY; BYMONTHDAY=1; BYHOUR=0; BYMINUTE=0; BYSECOND=0',
    enabled         => TRUE
  );
END;
/






