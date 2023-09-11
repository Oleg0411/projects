SELECT date_update, currency_code, currency_code_with, currency_with_div
FROM public.currencies
where date_update::Date = '{execution_date}'