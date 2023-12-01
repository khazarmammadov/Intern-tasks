CREATE SEQUENCE document_counter
    START WITH 1000
    INCREMENT BY 1
    NOCACHE
    NOCYCLE;


create table customer_documentID
(
    id          int default document_counter.nextval,
    customer_id number,
    document_id number
);

create or replace procedure customer_document is

begin

    merge into customer_documentID cust
    using (select MUSHTERI_FIZIKI_SHEXS_ID, dc.SENED_ID
           from CT_MUSHTERI_FIZIKI_SHEXS ct
                    inner join DOC_ASM dc on ct.MUSHTERI_FIZIKI_SHEXS_ID = dc.MUSHTERI_ID) info
    on (cust.customer_id = info.MUSHTERI_FIZIKI_SHEXS_ID)
    when matched then
        update
        set cust.customer_id = info.MUSHTERI_FIZIKI_SHEXS_ID,
            cust.document_id = info.SENED_ID
    when not matched then
        insert (customer_id, document_id)
        values (info.MUSHTERI_FIZIKI_SHEXS_ID, info.SENED_ID);
end customer_document;


BEGIN
  DBMS_SCHEDULER.create_job (
    job_name        => 'customer_document',
    job_type        => 'PLSQL_BLOCK',
    job_action      => 'BEGIN customer_document; END;',
    start_date      => SYSTIMESTAMP,
    repeat_interval => 'FREQ=MONTHLY; BYMONTHDAY=1; BYHOUR=0; BYMINUTE=0; BYSECOND=0',
    enabled         => TRUE
  );
END;
/