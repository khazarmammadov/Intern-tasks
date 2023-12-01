
CREATE SEQUENCE credit_information_hist
    START WITH 1000
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;


create table credit_information_hist (
    id int default credit_information_hist.nextval primary key,
    name varchar2(40),
    surname varchar2(40),
    credit_amount number,
    payment_date date,
    amount number,
    first_payment_date date,
    first_payment number,
    delay_percentage number,
    payment_amount number,
    vendor varchar2(40)
);

create or replace procedure credit_information is

begin

    merge into credit_information_hist hist
    using (select distinct ct.ADI,
                           ct.SOYADI,
                           dc.SENED_NOMRESI,
                           dc.NISYE_MEBLEG,
                           ctp.PAYMENT_DT,
                           ctp.AMOUNT,
                           dc.SENED_TARIXI as first_payment_date,
                           dc.ILKIN_ODENISH,
                           dc.CERIME_FAIZI,
                           dc.MEBLEG,
                           ctp.VENDOR_ID
           from DOC_ASM dc
                    inner join CT_TRM_PAYMENTS ctp on ctp.DOC_ITEM_NUMBER = dc.SENED_NOMRESI
                    inner join CT_MUSHTERI_FIZIKI_SHEXS ct on ct.MUSHTERI_FIZIKI_SHEXS_ID = dc.MUSHTERI_ID) info
    on (hist.doc_id = info.SENED_NOMRESI)
    when matched then
        update
        set hist.payment_date     = info.PAYMENT_DT,
            hist.amount           = info.AMOUNT,
            hist.delay_percentage = info.CERIME_FAIZI,
            hist.payment_amount   = info.MEBLEG,
            hist.vendor           = info.VENDOR_ID
    when not matched then
        insert (hist.name, hist.surname, hist.doc_id, hist.credit_amount, hist.payment_amount, hist.amount,
                hist.first_payment_date, hist.first_payment, hist.delay_percentage, hist.payment_amount, hist.vendor)
        values (info.ADI, info.SOYADI, info.SENED_NOMRESI, info.NISYE_MEBLEG, info.PAYMENT_DT, info.AMOUNT,
                info.first_payment_date, info.ILKIN_ODENISH, info.CERIME_FAIZI, info.MEBLEG,
                info.VENDOR_ID);
end;


BEGIN
  DBMS_SCHEDULER.create_job (
    job_name        => 'CREDIT_INFORMATION_JOB',
    job_type        => 'PLSQL_BLOCK',
    job_action      => 'BEGIN credit_information; END;',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=MONTHLY; BYMONTHDAY=1; BYHOUR=0; BYMINUTE=0; BYSECOND=0',
    enabled         => TRUE
  );
END;
/




