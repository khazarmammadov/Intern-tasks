CREATE SEQUENCE cus_acc
    START WITH 1000
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;


create table customer_account_hist
(
    id          int default cus_acc.nextval,
    customer_id number,
    account_id  number
);

create or replace procedure customer_account is

begin

    merge into customer_account_hist chist
    using (select cf.MUSHTERI_FIZIKI_SHEXS_ID, ac.HESAB_ID
           from ACC_HESAB ac
                    inner join CT_MUSHTERI_FIZIKI_SHEXS cf on ac.MUSHTERI_ID = cf.MUSHTERI_FIZIKI_SHEXS_ID) info
    on
        (chist.customer_id = info.MUSHTERI_FIZIKI_SHEXS_ID)
    when matched then
        update
        set chist.customer_id = info.MUSHTERI_FIZIKI_SHEXS_ID,
            chist.account_id = info.HESAB_ID
    when
        not
        matched then
        insert (customer_id, account_id)
        values (info.MUSHTERI_FIZIKI_SHEXS_ID, info.HESAB_ID);
end customer_account;

BEGIN
  DBMS_SCHEDULER.create_job (
    job_name        => 'customer_account',
    job_type        => 'PLSQL_BLOCK',
    job_action      => 'BEGIN customer_account; END;',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=MONTHLY; BYMONTHDAY=1; BYHOUR=0; BYMINUTE=0; BYSECOND=0',
    enabled         => TRUE
  );
END;
/