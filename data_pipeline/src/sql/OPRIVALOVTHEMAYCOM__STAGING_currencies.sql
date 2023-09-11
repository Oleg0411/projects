-- OPRIVALOVTHEMAYCOM__STAGING.currencies definition

CREATE TABLE OPRIVALOVTHEMAYCOM__STAGING.currencies
(
    date_update timestamp(0),
    currency_code int,
    currency_code_with int,
    currency_with_div numeric(5,3)
);


CREATE PROJECTION OPRIVALOVTHEMAYCOM__STAGING.currencies
(
 date_update,
 currency_code,
 currency_code_with,
 currency_with_div
)
AS
 SELECT currencies.date_update,
        currencies.currency_code,
        currencies.currency_code_with,
        currencies.currency_with_div
 FROM OPRIVALOVTHEMAYCOM__STAGING.currencies
 ORDER BY currencies.date_update
SEGMENTED BY hash(currencies.date_update, currencies.currency_code) ALL NODES KSAFE 1;


SELECT MARK_DESIGN_KSAFE(1);