-- OPRIVALOVTHEMAYCOM__STAGING.transactions definition

CREATE TABLE OPRIVALOVTHEMAYCOM__STAGING.transactions
(
    operation_id varchar(60),
    account_number_from int,
    account_number_to int,
    currency_code int,
    country varchar(30),
    status varchar(30),
    transaction_type varchar(30),
    amount int,
    transaction_dt timestamp(0)
);


CREATE PROJECTION OPRIVALOVTHEMAYCOM__STAGING.transactions
(
 operation_id,
 account_number_from,
 account_number_to,
 currency_code,
 country,
 status,
 transaction_type,
 amount,
 transaction_dt
)
AS
 SELECT transactions.operation_id,
        transactions.account_number_from,
        transactions.account_number_to,
        transactions.currency_code,
        transactions.country,
        transactions.status,
        transactions.transaction_type,
        transactions.amount,
        transactions.transaction_dt
 FROM OPRIVALOVTHEMAYCOM__STAGING.transactions
 ORDER BY transactions.transaction_dt
SEGMENTED BY hash(transactions.transaction_dt, transactions.operation_id) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);