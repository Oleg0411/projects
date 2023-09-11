-- OPRIVALOVTHEMAYCOM__DWH.global_metrics definition

CREATE TABLE OPRIVALOVTHEMAYCOM__DWH.global_metrics
(
    date_update timestamp(0),
    currency_from int,
    amount_total int,
    cnt_transactions int,
    avg_transactions_per_account int,
    cnt_accounts_make_transactions int
);


CREATE PROJECTION OPRIVALOVTHEMAYCOM__DWH.global_metrics
(
 date_update,
 currency_from,
 amount_total,
 cnt_transactions,
 avg_transactions_per_account,
 cnt_accounts_make_transactions
)
AS
 SELECT global_metrics.date_update,
        global_metrics.currency_from,
        global_metrics.amount_total,
        global_metrics.cnt_transactions,
        global_metrics.avg_transactions_per_account,
        global_metrics.cnt_accounts_make_transactions
 FROM OPRIVALOVTHEMAYCOM__DWH.global_metrics
 ORDER BY global_metrics.date_update
SEGMENTED BY hash(global_metrics.date_update, global_metrics.currency_from) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);