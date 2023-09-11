INSERT INTO OPRIVALOVTHEMAYCOM__DWH.global_metrics
(date_update,
currency_from,
amount_total,
cnt_transactions,
avg_transactions_per_account,
cnt_accounts_make_transactions)
WITH amount_tot_dollar AS (
SELECT t.transaction_dt::Date AS transaction_dt,
t.currency_code AS currency_code,
SUM(amount) AS amount
FROM OPRIVALOVTHEMAYCOM__STAGING.transactions t
LEFT JOIN OPRIVALOVTHEMAYCOM__STAGING.currencies c
ON t.currency_code = c.currency_code
WHERE c.currency_code  = 420
GROUP BY t.transaction_dt::Date, t.currency_code
),
 amount_tot AS (
SELECT t.transaction_dt::Date AS transaction_dt,
t.currency_code AS currency_code,
SUM(amount*c.currency_with_div) AS amount
FROM OPRIVALOVTHEMAYCOM__STAGING.transactions t
LEFT JOIN OPRIVALOVTHEMAYCOM__STAGING.currencies c
ON t.currency_code = c.currency_code
WHERE c.currency_code_with = 420
GROUP BY t.transaction_dt::Date, t.currency_code
),
cnt_tr AS (
SELECT t.transaction_dt::Date AS transaction_dt,
t.currency_code AS currency_code,
COUNT(DISTINCT t.operation_id) AS cnt_transactions
FROM OPRIVALOVTHEMAYCOM__STAGING.transactions t
LEFT JOIN OPRIVALOVTHEMAYCOM__STAGING.currencies c
ON t.currency_code = c.currency_code
GROUP BY t.transaction_dt::Date, t.currency_code
),
avg_tr_per_account AS (
SELECT t.transaction_dt::Date AS transaction_dt,
t.currency_code AS currency_code,
COUNT(DISTINCT t.operation_id)/COUNT(DISTINCT t.account_number_from) AS avg_transactions_per_account
FROM OPRIVALOVTHEMAYCOM__STAGING.transactions t
LEFT JOIN OPRIVALOVTHEMAYCOM__STAGING.currencies c
ON t.currency_code = c.currency_code
GROUP BY t.transaction_dt::Date, t.currency_code
),
cnt_accounts AS (
SELECT t.transaction_dt::Date AS transaction_dt,
t.currency_code AS currency_code,
COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions
FROM OPRIVALOVTHEMAYCOM__STAGING.transactions t
LEFT JOIN OPRIVALOVTHEMAYCOM__STAGING.currencies c
ON t.currency_code = c.currency_code
GROUP BY t.transaction_dt::Date, t.currency_code
)
SELECT distinct t.transaction_dt::Date AS date_update,
t.currency_code AS currency_from,
ISNULL(am.amount, 0) + ISNULL(atd.amount, 0) AS amount_total,
ct.cnt_transactions AS cnt_transactions,
atpa.avg_transactions_per_account AS avg_transactions_per_account,
ca.cnt_accounts_make_transactions AS cnt_accounts_make_transactions
FROM OPRIVALOVTHEMAYCOM__STAGING.transactions t
LEFT JOIN OPRIVALOVTHEMAYCOM__STAGING.currencies c
ON t.currency_code = c.currency_code
LEFT JOIN amount_tot am
ON t.currency_code = am.currency_code AND t.transaction_dt::Date = am.transaction_dt
LEFT JOIN cnt_tr ct
ON t.currency_code = ct.currency_code AND t.transaction_dt::Date = ct.transaction_dt
LEFT JOIN avg_tr_per_account atpa
ON t.currency_code = atpa.currency_code AND t.transaction_dt::Date = atpa.transaction_dt
LEFT JOIN cnt_accounts ca
ON t.currency_code = ca.currency_code AND t.transaction_dt::Date = ca.transaction_dt
LEFT JOIN amount_tot_dollar atd
ON t.currency_code = atd.currency_code AND t.transaction_dt::Date = atd.transaction_dt
WHERE t.transaction_dt::Date = '{execution_date}'